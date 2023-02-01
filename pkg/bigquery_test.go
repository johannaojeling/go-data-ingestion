package pkg

import (
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
)

func TestGenerateGCSReference(t *testing.T) {
	testCases := []struct {
		dataSource DataSource
		expected   *bigquery.GCSReference
		reason     string
	}{
		{
			dataSource: DataSource{
				SourceUri:    "gs://test-bucket",
				SourceFormat: "NEWLINE_DELIMITED_JSON",
				AutoDetect:   true,
			},
			expected: &bigquery.GCSReference{
				URIs: []string{"gs://test-bucket"},
				FileConfig: bigquery.FileConfig{
					SourceFormat: "NEWLINE_DELIMITED_JSON",
					AutoDetect:   true,
				},
			},
			reason: "Should generate GCSReference struct from DataSource",
		},
		{
			dataSource: DataSource{
				SourceUri:    "gs://test-bucket",
				SourceFormat: "AVRO",
				AvroOptions: &AvroOptions{
					UseAvroLogicalTypes: true,
				},
			},
			expected: &bigquery.GCSReference{
				URIs: []string{"gs://test-bucket"},
				FileConfig: bigquery.FileConfig{
					SourceFormat: "AVRO",
					AvroOptions: &bigquery.AvroOptions{
						UseAvroLogicalTypes: true,
					},
				},
			},
			reason: "Should generate GCSReference struct from DataSource" +
				"including AvroOptions when source format is AVRO",
		},
		{
			dataSource: DataSource{
				SourceUri:    "gs://test-bucket",
				SourceFormat: "CSV",
				CSVOptions: &CSVOptions{
					FieldDelimiter:  "|",
					SkipLeadingRows: 1,
				},
			},
			expected: &bigquery.GCSReference{
				URIs: []string{"gs://test-bucket"},
				FileConfig: bigquery.FileConfig{
					SourceFormat: "CSV",
					CSVOptions: bigquery.CSVOptions{
						FieldDelimiter:  "|",
						SkipLeadingRows: 1,
					},
				},
			},
			reason: "Should generate GCSReference struct from DataSource" +
				"including CSVOptions when source format is CSV",
		},
		{
			dataSource: DataSource{
				SourceUri:    "gs://test-bucket",
				SourceFormat: "PARQUET",
				ParquetOptions: &ParquetOptions{
					EnumAsString:        true,
					EnableListInference: false,
				},
			},
			expected: &bigquery.GCSReference{
				URIs: []string{"gs://test-bucket"},
				FileConfig: bigquery.FileConfig{
					SourceFormat: "PARQUET",
					ParquetOptions: &bigquery.ParquetOptions{
						EnumAsString:        true,
						EnableListInference: false,
					},
				},
			},
			reason: "Should generate GCSReference struct from DataSource" +
				"including ParquetOptions when source format is PARQUET",
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			actual := generateGCSReference(tc.dataSource)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
