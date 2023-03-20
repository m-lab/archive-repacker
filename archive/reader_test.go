package archive

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/google/go-cmp/cmp"
	"github.com/m-lab/go/testingx"
)

func TestNewFileReader(t *testing.T) {
	tests := []struct {
		name      string
		file      string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "success",
			file:      "file://./testdata/input.tgz",
			wantCount: 1,
		},
		{
			name:    "error-file-path",
			file:    "file://./testdata/input.wrong-extension",
			wantErr: true,
		},
		{
			name:    "error-file-does-not-exist",
			file:    "file://./testdata/notfound.tgz",
			wantErr: true,
		},
		{
			name:    "error-file-not-really-compressed",
			file:    "file://./testdata/notcompressed.tgz",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewFileReader(tt.file)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFileReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			for {
				_, _, err := got.NextFile()
				if err == io.EOF {
					break
				}
			}
			if got.Count != tt.wantCount {
				t.Errorf("Reader.Count = %v, want %v", got.Count, tt.wantCount)
			}
			p, err := ParseArchiveURL(tt.file)
			testingx.Must(t, err, "failed to parse input filename")
			s, err := os.Stat(p.Filename())
			testingx.Must(t, err, "failed to find input filename")
			if got.Size != int(s.Size()) {
				t.Errorf("Reader.Size = %v, want %v", got.Size, s.Size())
			}
		})
	}
}

func TestNewGCSReader(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "success",
			url:       "gs://fake-test-bucket/fake/path/input.tgz",
			wantCount: 1,
		},
		{
			name:    "error-bad-url",
			url:     "gs://fake-test-bucket/fake/path/input.wrong-extension",
			wantErr: true,
		},
		{
			name:    "error-file-not-really-compressed",
			url:     "gs://fake-test-bucket/fake/path/notcompressed.tgz",
			wantErr: true,
		},
		{
			name:    "error-does-not-exist",
			url:     "gs://fake-test-bucket/fake/path/does-not-exist.tgz",
			wantErr: true,
		},
	}
	objs := []fakestorage.Object{
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-test-bucket",
				Name:       "fake/path/input.tgz",
				Updated:    time.Now(),
			},
			Content: testingx.MustReadFile(t, "testdata/input.tgz"),
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-test-bucket",
				Name:       "fake/path/notcompressed.tgz",
				Updated:    time.Now(),
			},
			Content: testingx.MustReadFile(t, "testdata/notcompressed.tgz"),
		},
	}
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects:  objs,
		BucketsLocation: "US",
	})
	testingx.Must(t, err, "error initializing GCS server")
	defer server.Stop()
	client := server.Client()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, err := NewGCSReader(context.Background(), client, tt.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewGCSReader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			for {
				h, _, err := src.NextFile()
				if err != nil {
					break
				}
				h2 := CopyHeader(h)
				if !cmp.Equal(h, h2) {
					t.Errorf("CopyHeader badcopy; got %#v, want %#v", h2, h)
				}
			}
			src.Close()
			if src.Count != tt.wantCount {
				t.Errorf("output file counts do not match: %d %d", src.Count, tt.wantCount)
			}
			if tt.wantErr {
				return
			}
			// Read object attr size.
			p, err := ParseArchiveURL(tt.url)
			testingx.Must(t, err, "failed to parse output gcs path")
			obj := client.Bucket(p.Bucket()).Object(p.Object())
			attr, err := obj.Attrs(context.Background())
			testingx.Must(t, err, "failed to read object attrs")
			if src.Size != int(attr.Size) {
				t.Errorf("Reader.Size = %d, want %d", src.Size, attr.Size)
			}
		})
	}
}
