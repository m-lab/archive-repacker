package process_test

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/m-lab/archive-repacker/archive"
	"github.com/m-lab/archive-repacker/internal/jobs"
	"github.com/m-lab/archive-repacker/internal/process"
	"github.com/m-lab/go/cloud/bqfake"
)

type fakeRow struct {
	File string
}

type fakeProcessor struct {
	badCount int
}

func (f *fakeProcessor) Init(ctx context.Context, date string) {}
func (f *fakeProcessor) Source(ctx context.Context, row fakeRow) *archive.Reader {
	s, err := archive.NewFileReader(row.File)
	if err != nil {
		panic(err)
	}
	s.Count += f.badCount
	return s
}
func (f *fakeProcessor) File(h *tar.Header, b []byte) ([]byte, error) {
	if h.Name == "corrupt" {
		return nil, process.ErrCorrupt
	}
	return nil, nil
}
func (f *fakeProcessor) Finish(ctx context.Context, out *archive.Writer) error { return nil }

func TestManager_ProcessDate(t *testing.T) {
	// Hide logs during tests.
	log.SetOutput(io.Discard)
	tests := []struct {
		name     string
		date     string
		query    string
		config   bqfake.QueryConfig[fakeRow]
		badCount int
		wantErr  bool
	}{
		{
			name:  "success",
			date:  "2023-01-01",
			query: "SELECT 'file://./testdata/input.tgz' AS File",
			config: bqfake.QueryConfig[fakeRow]{
				RowIteratorConfig: bqfake.RowIteratorConfig[fakeRow]{
					Rows: []fakeRow{{File: "file://./testdata/input.tgz"}},
				},
			},
		},
		{
			name:  "success-tarfile-is-corrupt",
			date:  "2023-01-01",
			query: "SELECT 'file://./testdata/corrupt-tarfile.tgz' AS File",
			config: bqfake.QueryConfig[fakeRow]{
				RowIteratorConfig: bqfake.RowIteratorConfig[fakeRow]{
					Rows: []fakeRow{{File: "file://./testdata/corrupt-tarfile.tgz"}},
				},
			},
		},
		{
			name:  "success-corrupt-file-within-tarfile",
			date:  "2023-01-01",
			query: "SELECT 'file://./testdata/corrupt.tgz' AS File",
			config: bqfake.QueryConfig[fakeRow]{
				RowIteratorConfig: bqfake.RowIteratorConfig[fakeRow]{
					Rows: []fakeRow{{File: "file://./testdata/corrupt.tgz"}},
				},
			},
		},
		{
			name:  "bad-query",
			date:  "2023-01-01",
			query: "CORRUPT QUERY INVALID",
			config: bqfake.QueryConfig[fakeRow]{
				ReadErr: errors.New("read error"),
			},
			wantErr: true,
		},
		{
			name:  "bad-source-file-count",
			date:  "2023-01-01",
			query: "SELECT 'file://./testdata/input.tgz' AS File",
			config: bqfake.QueryConfig[fakeRow]{
				RowIteratorConfig: bqfake.RowIteratorConfig[fakeRow]{
					Rows: []fakeRow{{File: "file://./testdata/input.tgz"}},
				},
			},
			badCount: 1,
			wantErr:  true,
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/update", http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
	}))
	s := httptest.NewServer(mux)
	u, _ := url.Parse(s.URL)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := bqfake.NewQueryReadClient[fakeRow](tt.config)
			process.MaxDelaySeconds = 1

			p := &fakeProcessor{badCount: tt.badCount}
			r := process.Manager[fakeRow]{
				Process: p,
				Client:  client,
				Query:   tt.query,
				Jobs: jobs.Client{
					Server: u,
					Client: http.DefaultClient,
				},
			}
			ctx := context.Background()
			err := r.ProcessDate(ctx, tt.date)
			if (err != nil) != tt.wantErr {
				t.Errorf("ProcessDate() error; got %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}
