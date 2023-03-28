package annotation

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"

	"github.com/m-lab/archive-repacker/archive"
	"github.com/m-lab/go/testingx"
)

func TestRenamer_List(t *testing.T) {
	tests := []struct {
		name         string
		date         string
		bucket       string
		fromDatatype string
		newDatatype  string
		want         []string
		wantErr      bool
	}{
		{
			name:         "success",
			date:         "2023-03-01",
			bucket:       "fake-bucket",
			fromDatatype: "annotation",
			newDatatype:  "annotation2",
			want: []string{
				"gs://fake-bucket/ndt/annotation/2023/03/01/20230302T031500.576788Z-annotation-mlab1-chs0t-ndt.tgz",
				"gs://fake-bucket/ndt/annotation/2023/03/01/20230302T031500.123450Z-annotation-mlab2-chs0t-ndt.tgz",
			},
		},
		{
			name:    "error-bad-date",
			date:    "-this-is-not-a-date-",
			wantErr: true,
		},
		{
			name:    "error-cannot-walk-bad-bucket",
			date:    "2023-03-01",
			bucket:  "this-bucket-does-not-exist",
			wantErr: true,
		},
	}
	objs := []fakestorage.Object{
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-bucket",
				Name:       "ndt/annotation/2023/03/01/20230302T031500.576788Z-annotation-mlab1-chs0t-ndt.tgz",
				Updated:    time.Now(),
			},
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-bucket",
				Name:       "ndt/annotation/2023/03/01/20230302T031500.123450Z-annotation-mlab2-chs0t-ndt.tgz",
				Updated:    time.Now(),
			},
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
			r := NewRenamer(client, tt.bucket, tt.fromDatatype, tt.newDatatype)
			got, err := r.List(context.Background(), tt.date)
			if (err != nil) != tt.wantErr {
				t.Errorf("Renamer.List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			// Return order is not guaranteed; sort for comparison.
			sort.Strings(got)
			sort.Strings(tt.want)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Renamer.List() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRenamer_Rename(t *testing.T) {
	tests := []struct {
		name         string
		bucket       string
		fromDatatype string
		newDatatype  string
		url          string
		want         string
		wantErr      bool
	}{
		{
			name:         "success-rename-annotation2-archive",
			bucket:       "fake-bucket",
			fromDatatype: "annotation",
			newDatatype:  "annotation2",
			url:          "gs://fake-bucket/ndt/annotation/2023/03/01/20230302T031500.576788Z-annotation-mlab1-chs0t-ndt.tgz",
			want:         "gs://fake-bucket/ndt/annotation2/2023/03/01/20230302T031500.576788Z-annotation2-mlab1-chs0t-ndt.tgz",
		},
		{
			name:         "success-rename-hopannotation2-archive",
			bucket:       "fake-bucket",
			fromDatatype: "hopannotation1",
			newDatatype:  "hopannotation2",
			url:          "gs://fake-bucket/ndt/hopannotation1/2023/03/01/20230302T031500.123450Z-hopannotation1-mlab2-chs0t-ndt.tgz",
			want:         "gs://fake-bucket/ndt/hopannotation2/2023/03/01/20230302T031500.123450Z-hopannotation2-mlab2-chs0t-ndt.tgz",
		},
		{
			name:    "error-bad-url",
			url:     "-this-is:-invalid-url",
			wantErr: true,
		},
	}
	objs := []fakestorage.Object{
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-bucket",
				Name:       "ndt/annotation/2023/03/01/20230302T031500.576788Z-annotation-mlab1-chs0t-ndt.tgz",
				Updated:    time.Now(),
			},
			Content: []byte{0, 1, 2, 3, 4},
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-bucket",
				Name:       "ndt/hopannotation1/2023/03/01/20230302T031500.123450Z-hopannotation1-mlab2-chs0t-ndt.tgz",
				Updated:    time.Now(),
			},
			Content: []byte{0, 1, 2, 3, 4},
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
			r := NewRenamer(client, tt.bucket, tt.fromDatatype, tt.newDatatype)
			got, err := r.Rename(context.Background(), tt.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("Renamer.Rename() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Renamer.Rename() = \n%v, want \n%v", got, tt.want)
			}
			if tt.wantErr {
				return
			}

			// Verify output objects are in GCS.
			s, err := archive.ParseArchiveURL(tt.url)
			testingx.Must(t, err, "failed to parse archive url: %s", tt.url)
			d, err := archive.ParseArchiveURL(got)
			testingx.Must(t, err, "failed to parse output url: %s", got)
			// Src attrs.
			src := client.Bucket(s.Bucket()).Object(s.Object())
			srcattr, err := src.Attrs(context.Background())
			testingx.Must(t, err, "failed to read attrs")
			// Dst attrs.
			dst := client.Bucket(d.Bucket()).Object(d.Object())
			dstattr, err := dst.Attrs(context.Background())
			testingx.Must(t, err, "failed to read attrs")
			if srcattr.Size != dstattr.Size {
				t.Errorf("Renamer.Rename() wrong object size; got %d, want %d", dstattr.Size, srcattr.Size)
			}
		})
	}
}
