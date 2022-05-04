package utils

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/johannaojeling/go-data-ingestion/pkg/models"
)

func TestParseConfig(t *testing.T) {
	testCases := []struct {
		configFile string
		fields     interface{}
		expected   map[string]models.DataSource
		expectsErr bool
		reason     string
	}{
		{
			configFile: "config-valid.yaml",
			fields: struct {
				Bucket string
				Date   time.Time
			}{
				Bucket: "test-bucket",
				Date:   time.Date(2022, 01, 31, 0, 0, 0, 0, time.UTC),
			},
			expected: map[string]models.DataSource{
				"test01": {
					SourceUri:         "gs://test-bucket/input/2022/01/31/test.json",
					SourceFormat:      "NEWLINE_DELIMITED_JSON",
					AutoDetect:        true,
					DatasetId:         "raw",
					TableId:           "test01$20220131",
					CreateDisposition: "CREATE_NEVER",
					WriteDisposition:  "WRITE_TRUNCATE",
				},
				"test02": {
					SourceUri:    "gs://test-bucket/input/2022/01/31/test.csv",
					SourceFormat: "CSV",
					AutoDetect:   true,
					CSVOptions: &models.CSVOptions{
						SkipLeadingRows: 1,
					},
					DatasetId:         "raw",
					TableId:           "test02$20220131",
					CreateDisposition: "CREATE_NEVER",
					WriteDisposition:  "WRITE_TRUNCATE",
				},
			},
			expectsErr: false,
			reason:     "Should parse config to map with data sources",
		},
		{
			configFile: "config-invalid.yaml",
			fields: struct {
				Bucket string
				Date   time.Time
			}{
				Bucket: "test-bucket",
				Date:   time.Date(2022, 01, 31, 0, 0, 0, 0, time.UTC),
			},
			expected:   nil,
			expectsErr: true,
			reason:     "Should return error due to invalid input",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			content, err := ioutil.ReadFile("./testdata/" + tc.configFile)
			if err != nil {
				t.Fatalf("failed reading testdata: %v", err)
			}

			var actual map[string]models.DataSource
			actualErr := ParseConfig(string(content), tc.fields, &actual)

			assert.Equal(t, tc.expectsErr, actualErr != nil, "Error should match")
			assert.Equal(t, tc.expected, actual, "DataSource should match")
		})
	}
}

func TestParseTemplate(t *testing.T) {
	testCases := []struct {
		content    string
		data       interface{}
		expected   []byte
		expectsErr bool
		reason     string
	}{
		{
			content: `string: {{ .Field1 }}, time: {{ .Field2.Format "2006-01-02" }}`,
			data: struct {
				Field1 string
				Field2 time.Time
			}{
				Field1: "test",
				Field2: time.Date(2022, 01, 31, 0, 0, 0, 0, time.UTC),
			},
			expected:   []byte("string: test, time: 2022-01-31"),
			expectsErr: false,
			reason:     "Should apply template to templated content",
		},
		{
			content: `string: {{ .Field1 }}, time: {{ .Field2 Format "2006-01-02" }}`,
			data: struct {
				Field1 string
				Field2 time.Time
			}{
				Field1: "test",
				Field2: time.Date(2022, 01, 31, 0, 0, 0, 0, time.UTC),
			},
			expected:   []byte(""),
			expectsErr: true,
			reason:     "Should return error due to incorrectly formatted templated content",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			actual, actualErr := parseTemplate(tc.content, tc.data)
			assert.Equal(t, tc.expectsErr, actualErr != nil, "Error should match")
			assert.Equal(t, tc.expected, actual, "Bytes should match")
		})
	}
}
