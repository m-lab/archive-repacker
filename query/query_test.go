package query

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/m-lab/go/cloud/bqfake"
)

func TestRun(t *testing.T) {
	type Row struct {
		Date string
	}
	tests := []struct {
		name    string
		date    string
		query   string
		config  bqfake.QueryConfig[Row]
		want    []Row
		wantErr bool
	}{
		{
			name:  "success-template-variable",
			date:  "2023-01-01",
			query: `SELECT CAST(date AS STRING) as Date FROM (SELECT DATE("2023-01-01") as date) WHERE date = @date`,
			config: bqfake.QueryConfig[Row]{
				RowIteratorConfig: bqfake.RowIteratorConfig[Row]{
					Rows: []Row{{Date: "2023-01-01"}},
				},
			},
			want: []Row{
				{Date: "2023-01-01"},
			},
		},
		{
			name: "error-row-iterator",
			config: bqfake.QueryConfig[Row]{
				RowIteratorConfig: bqfake.RowIteratorConfig[Row]{
					IterErr: errors.New("fake iterator error"),
				},
			},
			wantErr: true,
		},
		{
			name: "error-read",
			config: bqfake.QueryConfig[Row]{
				ReadErr: errors.New("read error"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := bqfake.NewQueryReadClient[Row](tt.config)
			ctx := context.Background()
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
