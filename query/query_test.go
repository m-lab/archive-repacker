package query

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/option"

	"github.com/m-lab/go/testingx"
)

func TestRun(t *testing.T) {
	type Row struct {
		Date string
	}
	tests := []struct {
		name    string
		date    string
		query   string
		want    []Row
		wantErr bool
	}{
		{
			name:  "success-template-variable",
			date:  "2023-01-01",
			query: `SELECT CAST(date AS STRING) as Date FROM (SELECT DATE("2023-01-01") as date) WHERE date = @date`,
			want: []Row{
				{Date: "2023-01-01"},
			},
		},
		{
			name:    "error-canceled-context",
			wantErr: true,
		},
	}

	bqServer, err := server.New(server.TempStorage)
	testingx.Must(t, err, "failed to start fake bq server")
	defer bqServer.Close()
	bqServer.SetProject("test-project")
	testServer := bqServer.TestServer()
	defer testServer.Close()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := bigquery.NewClient(context.Background(), "test-project", option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
			testingx.Must(t, err, "failed to create bq client")
			defer client.Close()

			ctx, cancel := context.WithCancel(context.Background())
			if tt.wantErr {
				cancel()
			}
			defer cancel()

			p := map[string]interface{}{
				"date": tt.date,
			}
			result, err := Run[Row](ctx, client, tt.query, p)
			if (err != nil) != tt.wantErr {
				t.Errorf("Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(result, tt.want) {
				t.Errorf("Run() = %v, want %v", result, tt.want)
			}
		})
	}
}
