package archive

import (
	"strings"
	"testing"

	"github.com/m-lab/go/testingx"
)

func TestParseURL(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		file    string
		wantErr bool
	}{
		{
			name: "success-gs",
			path: "gs://fake-bucket/fake/path/anyfile.foo",
		},
		{
			name: "success-relative-file",
			path: "file://fake/path/anyfile.foo",
			file: "fake/path/anyfile.foo",
		},
		{
			name: "success-absolute-file",
			path: "file:///fake/path/anyfile.foo",
			file: "/fake/path/anyfile.foo",
		},
		{
			name:    "error-parsing-url",
			path:    ":- this-is-not-a-url",
			wantErr: true,
		},
		{
			name:    "error-unsupported-scheme-url",
			path:    "http://localhost:1234",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseURL(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if tt.path != got.String() {
				t.Errorf("Path.String() = %v, want %v", got.String(), tt.path)
			}
			if strings.HasPrefix(tt.path, "file:") && tt.file != got.Filename() {
				t.Errorf("Path.Filename() = %v, want %v", got.Filename(), tt.path)
			}
		})
	}
}

func TestParseArchiveURL(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		bucket  string
		object  string
		wantErr bool
	}{
		{
			name:   "success",
			path:   "gs://fake-bucket/fake/path/anyfile.tgz",
			bucket: "fake-bucket",
			object: "fake/path/anyfile.tgz",
		},
		{
			name:    "error-parsing-url",
			path:    ":- this-is-not-a-url",
			wantErr: true,
		},
		{
			name:    "error-wrong-suffix",
			path:    "gs://fake-bucket/fake/path/anyfile.foo",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseArchiveURL(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseArchiveURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if tt.path != got.String() {
				t.Errorf("Path.String() = %v, want %v", got.String(), tt.path)
			}
			if tt.bucket != got.Bucket() {
				t.Errorf("Path.Bucket() = %v, want %v", got.Bucket(), tt.bucket)
			}
			if tt.object != got.Object() {
				t.Errorf("Path.Object() = %v, want %v", got.Object(), tt.object)
			}
		})
	}
}

func TestPath_Dup(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		bucket string
		want   string
	}{
		{
			name:   "success",
			input:  "gs://fake-input-bucket/fake/path/foo.tgz",
			bucket: "alternate-output-bucket",
			want:   "gs://alternate-output-bucket/fake/path/foo.tgz",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := ParseArchiveURL(tt.input)
			testingx.Must(t, err, "failed to parse archive url")
			out := p.Dup(tt.bucket)
			if out.String() != tt.want {
				t.Errorf("Path.Dup() = %v, want %v", out.String(), tt.want)
			}
		})
	}
}
