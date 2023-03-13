package archive

import (
	"testing"

	"github.com/m-lab/go/testingx"
)

func TestParseURL(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{
			name: "success",
			path: "gs://fake-bucket/fake/path/anyfile.foo",
		},
		{
			name:    "error-parsing-url",
			path:    ":- this-is-not-a-url",
			wantErr: true,
		},
		{
			name:    "error-non-gcs-url",
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
				t.Errorf("ParseURL() = %v, want %v", got.String(), tt.path)
			}
		})
	}
}

func TestParseArchiveURL(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{
			name: "success",
			path: "gs://fake-bucket/fake/path/anyfile.tgz",
		},
		{
			name:    "error-parsing-url",
			path:    ":- this-is-not-a-url",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseArchiveURL(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if tt.path != got.String() {
				t.Errorf("ParseURL() = %v, want %v", got.String(), tt.path)
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
