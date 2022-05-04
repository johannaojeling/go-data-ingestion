package utils

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/johannaojeling/go-data-ingestion/pkg/models"
)

type BigQueryClient interface {
	Init(ctx context.Context, projectId string) error
	LoadToBigQuery(ctx context.Context, name string, dataSource models.DataSource) error
	Close() error
}

func NewBigQueryClient() BigQueryClient {
	return new(bigQueryClientImpl)
}

type bigQueryClientImpl struct {
	client *bigquery.Client
}

func (bigQueryClient *bigQueryClientImpl) Init(ctx context.Context, projectId string) error {
	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		return err
	}
	bigQueryClient.client = client
	return nil
}

func (bigQueryClient *bigQueryClientImpl) LoadToBigQuery(
	ctx context.Context,
	name string,
	dataSource models.DataSource,
) error {
	log.Printf("starting load job for %s\n", name)
	startTime := time.Now()

	job, err := bigQueryClient.createLoadJob(ctx, dataSource)
	if err != nil {
		return fmt.Errorf("failed creating load job for %s: %v", name, err)
	}

	if err = waitUntilComplete(ctx, job); err != nil {
		return fmt.Errorf("failed running load job for %s: %v", name, err)
	}

	elapsed := time.Since(startTime).Seconds()
	log.Printf("completed load job for %s after %f seconds\n", name, elapsed)
	return nil
}

func (bigQueryClient *bigQueryClientImpl) Close() error {
	return bigQueryClient.client.Close()
}

func (bigQueryClient *bigQueryClientImpl) createLoadJob(
	ctx context.Context,
	dataSource models.DataSource,
) (*bigquery.Job, error) {
	gcsRef := generateGCSReference(dataSource)

	loader := bigQueryClient.client.Dataset(dataSource.DatasetId).
		Table(dataSource.TableId).
		LoaderFrom(gcsRef)
	loader.WriteDisposition = dataSource.WriteDisposition
	loader.CreateDisposition = dataSource.CreateDisposition

	return loader.Run(ctx)
}

func generateGCSReference(dataSource models.DataSource) *bigquery.GCSReference {
	gcsRef := bigquery.NewGCSReference(dataSource.SourceUri)
	gcsRef.SourceFormat = dataSource.SourceFormat
	gcsRef.AutoDetect = dataSource.AutoDetect

	switch dataSource.SourceFormat {
	case bigquery.Avro:
		avroOptions := dataSource.AvroOptions
		gcsRef.AvroOptions = &bigquery.AvroOptions{
			UseAvroLogicalTypes: avroOptions.UseAvroLogicalTypes,
		}
	case bigquery.CSV:
		csvOptions := dataSource.CSVOptions
		gcsRef.CSVOptions = bigquery.CSVOptions{
			AllowJaggedRows:     csvOptions.AllowJaggedRows,
			AllowQuotedNewlines: csvOptions.AllowQuotedNewlines,
			Encoding:            csvOptions.Encoding,
			FieldDelimiter:      csvOptions.FieldDelimiter,
			Quote:               csvOptions.Quote,
			ForceZeroQuote:      csvOptions.ForceZeroQuote,
			SkipLeadingRows:     csvOptions.SkipLeadingRows,
			NullMarker:          csvOptions.NullMarker,
		}
	case bigquery.Parquet:
		parquetOptions := dataSource.ParquetOptions
		gcsRef.ParquetOptions = &bigquery.ParquetOptions{
			EnumAsString:        parquetOptions.EnumAsString,
			EnableListInference: parquetOptions.EnableListInference,
		}
	}
	return gcsRef
}

func waitUntilComplete(ctx context.Context, job *bigquery.Job) error {
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	return status.Err()
}
