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
	forceBadCount int
	outCount      int
	fileCalls     int
}

func (f *fakeProcessor) Init(ctx context.Context, date string) {}
func (f *fakeProcessor) Source(ctx context.Context, row fakeRow) *archive.Source {
	s, err := archive.NewFileSource(row.File)
	if err != nil {
		panic(err)
	}
	s.Count += f.forceBadCount
	return s
}
func (f *fakeProcessor) File(h *tar.Header, b []byte) ([]byte, error) {
	f.fileCalls++
	if h.Name == "corrupt" {
		return nil, process.ErrCorrupt
	}
	return nil, nil
}
func (f *fakeProcessor) Finish(ctx context.Context, out *archive.Target) error {
	f.outCount = out.Count
	return nil
}

func TestManager_ProcessDate(t *testing.T) {
	// Hide logs during tests.
	log.SetOutput(io.Discard)
	tests := []struct {
		name          string
		date          string
		query         string
		config        bqfake.QueryConfig[fakeRow]
		wantCount     int
		wantFileCalls int
		forceBadCount int
		wantErr       bool
	}{
		{
			name:  "success-two-files",
			date:  "2023-01-01",
			query: "SELECT 'file://./testdata/input.tgz' AS File",
			config: bqfake.QueryConfig[fakeRow]{
				RowIteratorConfig: bqfake.RowIteratorConfig[fakeRow]{
					Rows: []fakeRow{{File: "file://./testdata/input.tgz"}},
				},
			},
			wantFileCalls: 2,
			wantCount:     2,
		},
		{
			name:  "success-tarfile-is-corrupt-with-one-good-file",
			date:  "2023-01-01",
			query: "SELECT 'file://./testdata/corrupt-tarfile.tgz' AS File",
			config: bqfake.QueryConfig[fakeRow]{
				RowIteratorConfig: bqfake.RowIteratorConfig[fakeRow]{
					Rows: []fakeRow{{File: "file://./testdata/corrupt-tarfile.tgz"}},
				},
			},
			wantFileCalls: 1,
			wantCount:     1,
		},
		{
			name:  "success-tarfile-contains-one-corrupt-file",
			date:  "2023-01-01",
			query: "SELECT 'file://./testdata/corrupt.tgz' AS File",
			config: bqfake.QueryConfig[fakeRow]{
				RowIteratorConfig: bqfake.RowIteratorConfig[fakeRow]{
					Rows: []fakeRow{{File: "file://./testdata/corrupt.tgz"}},
				},
			},
			wantFileCalls: 1,
			wantCount:     0, // no good files remain.
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
			forceBadCount: 1,
			wantErr:       true,
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

			p := &fakeProcessor{forceBadCount: tt.forceBadCount}
			r := process.Manager[fakeRow]{
				Process:     p,
				QueryClient: client,
				Query:       tt.query,
				Jobs:        jobs.NewClient(u, http.DefaultClient),
			}
			ctx := context.Background()
			err := r.ProcessDate(ctx, tt.date)
			if (err != nil) != tt.wantErr {
				t.Errorf("ProcessDate() error; got %v, wantErr %t", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if tt.wantCount != p.outCount {
				t.Errorf("Process.Finish() output count = %d, want %d", p.outCount, tt.wantCount)
			}
			if tt.wantFileCalls != p.fileCalls {
				t.Errorf("Process.File() calls = %d, want %d", p.fileCalls, tt.wantFileCalls)
			}
		})
	}
}
