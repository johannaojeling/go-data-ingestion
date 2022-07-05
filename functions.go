package functions

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"

	"github.com/johannaojeling/go-data-ingestion/pkg/models"
	"github.com/johannaojeling/go-data-ingestion/pkg/utils"
)

const (
	dateLayout = "2006-01-02"
	configPath = "resources/config/batch-ingestion.yaml"
)

var (
	//go:embed resources
	resources embed.FS

	projectId = os.Getenv("PROJECT")
	bucket    = os.Getenv("BUCKET")

	newBigQueryClientFunc = utils.NewBigQueryClient
)

func init() {
	functions.HTTP("IngestBatch", IngestBatch)
}

func IngestBatch(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprint(writer, "supports only POST method")
		return
	}

	var data struct {
		Date string
	}
	err := json.NewDecoder(request.Body).Decode(&data)
	if err != nil {
		log.Printf("error unmarshaling request requestBody: %v\n", err)
		writer.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(writer, "invalid request body")
		return
	}

	date, err := time.Parse(dateLayout, data.Date)
	if err != nil {
		log.Printf("error parsing date string to time: %v\n", err)
		writer.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(writer, "invalid date")
		return
	}

	templateFields := struct {
		Bucket string
		Date   time.Time
	}{
		Bucket: bucket,
		Date:   date,
	}

	config, err := resources.ReadFile(configPath)
	if err != nil {
		log.Fatalf("failed to read config file")
	}

	var configMap map[string]models.DataSource
	err = utils.ParseConfig(string(config), templateFields, &configMap)
	if err != nil {
		log.Printf("error parsing config to map of data sources: %v\n", err)
		writer.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(writer, "ingestion failed, check logs for more details")
		return
	}

	bigQueryClient := newBigQueryClientFunc()
	ctx := request.Context()
	err = bigQueryClient.Init(ctx, projectId)
	if err != nil {
		log.Printf("error creating BigQuery bigQueryClient: %v\n", err)
		writer.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(writer, "ingestion failed, check logs for more details")
		return
	}
	defer bigQueryClient.Close()

	err = ingestAll(ctx, bigQueryClient, configMap)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(writer, "ingestion failed, check logs for more details")
		return
	}

	writer.WriteHeader(http.StatusNoContent)
}

func ingestAll(
	ctx context.Context,
	bigQueryClient utils.BigQueryClient,
	configMap map[string]models.DataSource,
) error {
	totalCount := len(configMap)
	log.Printf("starting ingestion of %d data source(s)\n", totalCount)

	startTime := time.Now()
	channel := make(chan error)

	for name, dataSource := range configMap {
		name := name
		dataSource := dataSource

		go func() {
			channel <- bigQueryClient.LoadToBigQuery(ctx, name, dataSource)
		}()
	}

	errorCount := 0
	for range configMap {
		err := <-channel
		if err != nil {
			errorCount++
			log.Println(err.Error())
		}
	}

	elapsed := time.Since(startTime).Seconds()
	log.Printf(
		"finished ingestion of %d data source(s) after %f seconds. Succeeded: %d. Failed: %d\n",
		totalCount,
		elapsed,
		totalCount-errorCount,
		errorCount,
	)

	if errorCount != 0 {
		return fmt.Errorf(
			"failed ingestion of %d data sources",
			errorCount,
		)
	}
	return nil
}
