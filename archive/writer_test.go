package archive

import (
	"context"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/m-lab/go/testingx"
)

func TestNewTarget(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		output  string
		wantErr bool
	}{
		{
			name:   "success",
			file:   "file://./testdata/input.tgz",
			output: "gs://fake-output-bucket/fake/path/input.tgz",
		},
		{
			name:    "error-writing-to-bucket",
			file:    "file://./testdata/input.tgz",
			output:  "gs://error-bucket/fake/path/input.tgz",
			wantErr: true,
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
			p, err := ParseArchiveURL(tt.output)
			testingx.Must(t, err, "failed to parse output gcs path")

			// Verify & Upload
			if out.Count != src.Count {
				t.Errorf("Target.Count = %d, want %d", out.Count, src.Count)
			}
			err = out.Upload(context.Background(), client, p)
			if (err != nil) != tt.wantErr {
				t.Errorf("Target.Upload() = %v, want nil", err)
			}
		})
	}
}
