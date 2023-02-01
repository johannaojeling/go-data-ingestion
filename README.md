# Go Data Ingestion

## Introduction

This project contains a function for ingesting data from Google Cloud Storage to BigQuery
using [batch load](https://cloud.google.com/bigquery/docs/batch-loading-data). The function can be deployed to Cloud
Functions and triggered with an HTTP POST request.

The data sources are configured in the
file [resources/config/batch-ingestion.yaml](resources/config/batch-ingestion.yaml). There are four examples, each for a
different source format. The source objects are ingested to time partitioned BigQuery tables. The configuration
file is templated with the fields `.Bucket` and `.Date` representing the bucket containing data and the date of
ingestion.

Below are the options that can be configured in the yaml configuration file.
See [pkg/data_source.go](pkg/data_source.go) for details about specific file type options.

| Key                | Description                                                                 |
|--------------------|-----------------------------------------------------------------------------|
| source_uri         | GCS source URI                                                              |
| source_format      | Source format: `AVRO`, `CSV`, `NEWLINE_DELIMITED_JSON`, `PARQUET`           |
| avro_options       | Options for avro files                                                      |
| csv_options        | Options for csv files                                                       |
| parquet_options    | Options for parquet files                                                   |
| auto_detect        | Whether schema should be auto-detected from file                            |
| dataset_id         | BigQuery dataset id                                                         |
| table_id           | BigQuery table id                                                           |
| create_disposition | BigQuery create disposition: `CREATE_IF_NEEDED`, `CREATE_NEVER`             |
| write_disposition  | BigQuery write disposition: `WRITE_APPEND`, `WRITE_EMPTY`, `WRITE_TRUNCATE` |

## Pre-requisites

- Go version 1.19
- Gcloud SDK

## Development

### Setup

Install dependencies

```bash
go mod download
```

### Testing

Run unit tests

```bash
go test ./...
```

### Running the application

Set environment variables

| Variable        | Description                                            |
|-----------------|--------------------------------------------------------|
| PROJECT         | GCP project                                            |
| BUCKET          | Bucket with source data                                |
| FUNCTION_TARGET | Function entrypoint. Should be set to `IngestBatch`    |
| PORT            | Port for web server. If not set, will listen on `8080` |

Run application

```bash
go run cmd/main.go
```

Set URL

```bash
URL="localhost:${PORT}"
```

Invoke the function by making an HTTP POST request with a body containing a date in the format `%Y-%m-%d`

```bash
curl -i -X POST ${URL} \
-H "Content-Type: application/json" \
-d '{"date":"2022-04-30"}'
```

## Deployment

### Deploying to Cloud Functions

Set environment variables

| Variable          | Description                                                                                                                                                                                |
|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| PROJECT           | GCP project                                                                                                                                                                                |
| REGION            | Region                                                                                                                                                                                     |
| CLOUD_FUNCTION_SA | Email of service account used for Cloud Function. Needs the roles:<br/><ul><li>`roles/bigquery.dataEditor`</li><li>`roles/bigquery.jobUser`</li><li>`roles/storage.objectViewer`</li></ul> |
| FUNCTION_NAME     | Name of function                                                                                                                                                                           |
| BUCKET            | Bucket with source data                                                                                                                                                                    |

Deploy to Cloud Functions

```bash
gcloud functions deploy ${FUNCTION_NAME} \
--gen2 \
--project=${PROJECT} \
--region=${REGION} \
--trigger-http \
--runtime=go119 \
--entry-point=IngestBatch \
--service-account=${CLOUD_FUNCTION_SA} \
--set-env-vars=PROJECT=${PROJECT},BUCKET=${BUCKET} \
--no-allow-unauthenticated
```

### Invoking Cloud Function

Set URL and token

```bash
URL=$(gcloud functions describe ${FUNCTION_NAME} --gen2 --region=${REGION} --format="value(serviceConfig.uri)")

TOKEN=$(gcloud auth print-identity-token)
```

Invoke the function by making an HTTP POST request with a body containing a date in the format `%Y-%m-%d`

```bash
curl -i -X POST ${URL} \
-H "Authorization: Bearer ${TOKEN}" \
-H "Content-Type: application/json" \
-d '{"date":"2022-04-30"}'
```
