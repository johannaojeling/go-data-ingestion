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

	"cloud.google.com/go/bigquery"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/johannaojeling/go-data-ingestion/pkg"
)

const (
	dateLayout = "2006-01-02"
	configPath = "resources/config/batch-ingestion.yaml"
)

var (
	//go:embed resources
	resources embed.FS

	config []byte
	client *bigquery.Client

	projectID = os.Getenv("PROJECT")
	bucket    = os.Getenv("BUCKET")
)

func init() {
	var err error

	config, err = resources.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	client, err = bigquery.NewClient(context.Background(), projectID)
	if err != nil {
		log.Fatalf("Error initializing BigQuery client: %v", err)
	}

	functions.HTTP("IngestBatch", IngestBatch)
}

type batchRequest struct {
	Date string
}

type templateFields struct {
	Bucket string
	Date   time.Time
}

func IngestBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		log.Printf("Invalid HTTP method: %v\n", r.Method)
		http.Error(w, "Only POST method is supported", http.StatusMethodNotAllowed)
		return
	}

	var batchReq batchRequest
	if err := json.NewDecoder(r.Body).Decode(&batchReq); err != nil {
		log.Printf("Error unmarshaling request body: %v\n", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	date, err := time.Parse(dateLayout, batchReq.Date)
	if err != nil {
		log.Printf("Error parsing date string to time: %v\n", err)
		http.Error(w, "Invalid date format", http.StatusBadRequest)
		return
	}

	fields := templateFields{
		Bucket: bucket,
		Date:   date,
	}

	var configMap map[string]pkg.DataSource
	if err := pkg.ParseConfig(string(config), fields, &configMap); err != nil {
		log.Printf("Error parsing config to map of data sources: %v\n", err)
		http.Error(w, "Unable to parse configuration", http.StatusInternalServerError)
		return
	}

	if err := ingestAll(r.Context(), client, configMap); err != nil {
		log.Printf("Error ingesting data sources: %v\n", err)
		http.Error(w, "Unable to ingest data sources", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func ingestAll(
	ctx context.Context,
	client *bigquery.Client,
	configMap map[string]pkg.DataSource,
) error {
	count := len(configMap)
	log.Printf("Starting ingestion of %d data source(s)\n", count)

	start := time.Now()
	errChan := make(chan error)

	for name, source := range configMap {
		go func(name string, source pkg.DataSource) {
			errChan <- pkg.LoadToBigQuery(ctx, client, name, source)
		}(name, source)
	}

	errCount := 0

	for range configMap {
		if err := <-errChan; err != nil {
			log.Println(err.Error())
			errCount++
		}
	}

	elapsed := time.Since(start).Seconds()
	log.Printf(
		"Finished ingestion of %d data source(s) after %f seconds. Succeeded: %d. Failed: %d\n",
		count,
		elapsed,
		count-errCount,
		errCount,
	)

	if errCount != 0 {
		return fmt.Errorf("failed ingestion of %d data sources", errCount)
	}

	return nil
}
