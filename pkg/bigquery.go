package pkg

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
)

func LoadToBigQuery(
	ctx context.Context,
	client *bigquery.Client,
	name string,
	source DataSource,
) error {
	log.Printf("starting load job for %s\n", name)
	start := time.Now()

	job, err := createLoadJob(ctx, client, source)
	if err != nil {
		return fmt.Errorf("error creating load job for %s: %v", name, err)
	}

	if err := waitUntilComplete(ctx, job); err != nil {
		return fmt.Errorf("error running load job for %s: %v", name, err)
	}

	elapsed := time.Since(start).Seconds()
	log.Printf("completed load job for %s after %f seconds\n", name, elapsed)

	return nil
}

func createLoadJob(
	ctx context.Context,
	client *bigquery.Client,
	source DataSource,
) (*bigquery.Job, error) {
	gcsRef := generateGCSReference(source)

	loader := client.Dataset(source.DatasetId).
		Table(source.TableId).
		LoaderFrom(gcsRef)

	loader.WriteDisposition = source.WriteDisposition
	loader.CreateDisposition = source.CreateDisposition

	return loader.Run(ctx)
}

func generateGCSReference(source DataSource) *bigquery.GCSReference {
	gcsRef := bigquery.NewGCSReference(source.SourceUri)
	gcsRef.SourceFormat = source.SourceFormat
	gcsRef.AutoDetect = source.AutoDetect

	switch source.SourceFormat {
	case bigquery.Avro:
		avroOptions := source.AvroOptions
		gcsRef.AvroOptions = &bigquery.AvroOptions{
			UseAvroLogicalTypes: avroOptions.UseAvroLogicalTypes,
		}

	case bigquery.CSV:
		csvOptions := source.CSVOptions
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
		parquetOptions := source.ParquetOptions
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
