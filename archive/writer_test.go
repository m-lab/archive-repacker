package archive

import (
	"archive/tar"
	"context"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/m-lab/go/testingx"
)

func TestNewWriter(t *testing.T) {
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
			src, err := NewFileReader(tt.file)
			testingx.Must(t, err, "failed to open file: %s", tt.file)
			out := NewWriter()

			// Copy input to output
			for {
				h, b, err := src.NextFile()
				if err != nil {
					break
				}
				testingx.Must(t, out.AddFile(CopyHeader(h), b), "failed to add file to output")
			}
			src.Close()
			out.Close()
			p, err := ParseArchiveURL(tt.output)
			testingx.Must(t, err, "failed to parse output gcs path")

			// Verify & Upload
			if out.Count != src.Count {
				t.Errorf("Writer.Count = %d, want %d", out.Count, src.Count)
			}
			err = out.Upload(context.Background(), client, p)
			if (err != nil) != tt.wantErr {
				t.Errorf("Writer.Upload() = %v, want nil", err)
			}
			obj := client.Bucket(p.Bucket()).Object(p.Object())
			attr, err := obj.Attrs(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("Object.Attrs() = %v, wantErr %t", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			// Is the uploaded object the same size as the original one?
			if attr.Size != int64(src.Size) {
				t.Errorf("Object.Attr.Size = %d, want %d", attr.Size, src.Size)
			}
		})
	}
}

func TestWriter_AddFile(t *testing.T) {
	tests := []struct {
		name     string
		h        *tar.Header
		contents []byte
		wantErr  bool
	}{
		{
			name: "success-nil-header",
		},
		{
			name: "error-corrupt-header",
			h: &tar.Header{
				Format: ^tar.FormatGNU, // invalid format
				Size:   -1,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := NewWriter()
			if err := out.AddFile(tt.h, tt.contents); (err != nil) != tt.wantErr {
				t.Errorf("Writer.AddFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
