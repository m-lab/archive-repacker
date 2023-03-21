package archive_test

import (
	"archive/tar"
	"context"
	"log"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/goccy/bigquery-emulator/server"
	"github.com/m-lab/archive-repacker/archive"
	"github.com/m-lab/go/testingx"
	"google.golang.org/api/option"
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
		return nil, archive.ErrCorrupt
	}
	return nil, nil
}
func (f *fakeProcessor) Finish(ctx context.Context, out *archive.Writer) error { return nil }

func TestReprocessor_ProcessDate(t *testing.T) {
	bqServer, err := server.New(server.TempStorage)
	testingx.Must(t, err, "failed to start fake bq server")
	defer bqServer.Close()
	bqServer.SetProject("test-project")
	testServer := bqServer.TestServer()
	defer testServer.Close()

	ctx := context.Background()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	tests := []struct {
		name     string
		date     string
		query    string
		badCount int
		wantErr  bool
	}{
		{
			name:  "success",
			date:  "2023-01-01",
			query: "SELECT 'file://./testdata/input.tgz' AS File",
		},
		{
			name:  "success-corrupt-tarfile",
			date:  "2023-01-01",
			query: "SELECT 'file://./testdata/corrupt-tarfile.tgz' AS File",
		},
		{
			name:  "success-corrupt-file",
			date:  "2023-01-01",
			query: "SELECT 'file://./testdata/corrupt.tgz' AS File",
		},
		{
			name:    "bad-query",
			date:    "2023-01-01",
			query:   "CORRUPT QUERY INVALID",
			wantErr: true,
		},
		{
			name:     "bad-source-file-count",
			date:     "2023-01-01",
			query:    "SELECT 'file://./testdata/input.tgz' AS File",
			badCount: 1,
			wantErr:  true,
		},
	}
	//u, _ := url.Parse("http://localhost:12345")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := bigquery.NewClient(ctx, "test-project", option.WithEndpoint(testServer.URL), option.WithoutAuthentication())
			testingx.Must(t, err, "failed to create bq client")
			defer client.Close()
			archive.MaxDelaySeconds = 1
			archive.ProcessRetries = 0

			p := &fakeProcessor{badCount: tt.badCount}
			r := archive.Reprocessor[fakeRow]{
				Process: p,
				Client:  client,
				Query:   tt.query,
				/*Jobs: jobs.Client{
					Server: u,
					Client: http.DefaultClient,
				},*/
			}
			err = r.ProcessDate(context.TODO(), tt.date)
			if (err != nil) != tt.wantErr {
				t.Errorf("ProcessDate() error; got %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}
