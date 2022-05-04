package functions

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/johannaojeling/go-data-ingestion/pkg/models"
	"github.com/johannaojeling/go-data-ingestion/pkg/utils"
)

type bigQueryClientMock struct {
	mock.Mock
}

func (mock *bigQueryClientMock) Init(ctx context.Context, projectId string) error {
	args := mock.Called(ctx, projectId)
	return args.Error(0)
}

func (mock *bigQueryClientMock) Close() error {
	args := mock.Called()
	return args.Error(0)
}

func (mock *bigQueryClientMock) LoadToBigQuery(
	ctx context.Context,
	name string,
	dataSource models.DataSource,
) error {
	args := mock.Called(ctx, name, dataSource)
	return args.Error(0)
}

func TestIngestBatch(t *testing.T) {
	testCases := []struct {
		requestMethod        string
		requestBody          string
		returnInit           error
		returnLoadToBigQuery error
		expectedCode         int
		expectedContent      string
		reason               string
	}{
		{
			requestMethod:   "GET",
			requestBody:     "",
			expectedCode:    405,
			expectedContent: "supports only POST method",
			reason:          "Should return status 405 due to not allowed method",
		},
		{
			requestMethod:   "POST",
			requestBody:     `{"date": 2022-05-02}`,
			expectedCode:    400,
			expectedContent: "invalid request body",
			reason:          "Should return status 400 due to invalid request body",
		},
		{
			requestMethod:   "POST",
			requestBody:     `{"date": "2022-05-123"}`,
			expectedCode:    400,
			expectedContent: "invalid date",
			reason:          "Should return status 400 due to invalid date format",
		},
		{
			requestMethod:   "POST",
			requestBody:     `{"date": "2022-05-02"}`,
			returnInit:      errors.New("error in Init"),
			expectedCode:    500,
			expectedContent: "ingestion failed, check logs for more details",
			reason:          "Should return status 500 due to failing to initialize BigQuery client",
		},
		{
			requestMethod:        "POST",
			requestBody:          `{"date": "2022-05-02"}`,
			returnInit:           nil,
			returnLoadToBigQuery: errors.New("error in LoadToBigQuery"),
			expectedCode:         500,
			expectedContent:      "ingestion failed, check logs for more details",
			reason:               "Should return status 500 due to failing to run LoadToBigQuery",
		},
		{
			requestMethod:        "POST",
			requestBody:          `{"date": "2022-05-02"}`,
			returnInit:           nil,
			returnLoadToBigQuery: nil,
			expectedCode:         204,
			expectedContent:      "",
			reason:               "Should return status 204 when ingestion is successful",
		},
	}

	originalProjectId := projectId
	defer func() { projectId = originalProjectId }()
	projectId = "project"

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d: %s", i, tc.reason), func(t *testing.T) {
			request := httptest.NewRequest(tc.requestMethod, "/", strings.NewReader(tc.requestBody))
			request.Header.Add("Content-Type", "application/json")

			ctx := request.Context()
			recorder := httptest.NewRecorder()

			clientMock := new(bigQueryClientMock)
			clientMock.On("Init", ctx, projectId).Return(tc.returnInit)
			clientMock.On("Close").Return(nil)
			clientMock.On(
				"LoadToBigQuery",
				ctx,
				mock.AnythingOfType("string"),
				mock.AnythingOfType("DataSource"),
			).Return(tc.returnLoadToBigQuery)

			originalNewBigQueryClientFunc := newBigQueryClientFunc
			defer func() { newBigQueryClientFunc = originalNewBigQueryClientFunc }()
			newBigQueryClientFunc = func() utils.BigQueryClient { return clientMock }

			IngestBatch(recorder, request)

			actualCode := recorder.Code
			actualContent := recorder.Body.String()

			assert.Equal(t, tc.expectedCode, actualCode, "Response status code should match")
			assert.Equal(t, tc.expectedContent, actualContent, "Response body should match")
		})
	}
}
