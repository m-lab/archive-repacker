package archive

import (
	"context"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/go/testingx"
)

func TestNewTarget(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		wantErr bool
	}{
		{
			name: "success",
			file: "file://./testdata/input.tgz",
		},
	}
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: []fakestorage.Object{
			{
				ObjectAttrs: fakestorage.ObjectAttrs{
					BucketName: "fake-output-bucket",
				},
			},
		},
		BucketsLocation: "US",
	})
	testingx.Must(t, err, "error initializing GCS server")
	defer server.Stop()
	client := server.Client()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, err := NewFileSource(tt.file)
			testingx.Must(t, err, "failed to open file: %s", tt.file)
			out := NewTarget()

			// Copy input to output
			for {
				h, b, err := src.NextFile()
				if err != nil {
					break
				}
				out.AddFile(CopyHeader(h), b)
			}

			src.Close()
			out.Close()
			if out.Count != src.Count {
				t.Errorf("Target.Count = %d, want %d", out.Count, src.Count)
			}

			p, err := ParseArchiveURL("gs://fake-output-bucket/fake/path/input.tgz")
			testingx.Must(t, err, "failed to parse output gcs path")

			err = out.Upload(context.TODO(), stiface.AdaptClient(client), p)
			testingx.Must(t, err, "failed to upload file")
		})
	}
}
