package annotation

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/m-lab/traceroute-caller/hopannotation"
	"github.com/m-lab/uuid-annotator/annotator"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/m-lab/archive-repacker/archive"
	"github.com/m-lab/go/testingx"
)

func TestHopProcessor_Init(t *testing.T) {
	date := "2023-02-05"
	objs := []fakestorage.Object{
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-routeview-bucket",
				Name:       "RouteViewIPv4/2023/02/routeviews-rv2-20230205-2200.pfx2as.gz",
				Updated:    time.Now(),
			},
			Content: testingx.MustReadFile(t, "testdata/routeviews-rv2-20230205-2200.pfx2as.gz"),
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-routeview-bucket",
				Name:       "RouteViewIPv6/2023/02/routeviews-rv6-20230205-0600.pfx2as.gz",
				Updated:    time.Now(),
			},
			Content: testingx.MustReadFile(t, "testdata/routeviews-rv6-20230205-0600.pfx2as.gz"),
		},
	}
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects:  objs,
		BucketsLocation: "US",
	})
	testingx.Must(t, err, "error initializing GCS server")
	defer server.Stop()
	client := server.Client()

	t.Run("success", func(t *testing.T) {

		rv4, err := archive.ParseURL("gs://fake-routeview-bucket/RouteViewIPv4/")
		testingx.Must(t, err, "failed to parse v4 url prefix")
		rv6, err := archive.ParseURL("gs://fake-routeview-bucket/RouteViewIPv6/")
		testingx.Must(t, err, "failed to parse v6 url prefix")
		asnames, err := archive.ParseURL("file:./testdata/asnames.ipinfo.csv")
		testingx.Must(t, err, "failed to parse asname url")

		p := NewHopProcessor(client, "", rv4.URL, rv6.URL, asnames.URL)

		// Not exiting is success.
		ctx := context.Background()
		p.Init(ctx, date)
	})
}

func TestHopProcessor_Source(t *testing.T) {
	log.SetOutput(io.Discard)
	tests := []struct {
		name string
		row  Result
		want *archive.Path
	}{
		{
			name: "success",
			row: Result{
				ArchiveURL: "gs://fake-source-bucket/fake/path/input.tgz",
				Files: []struct {
					DstIP    string
					Filename string
				}{
					{DstIP: "127.0.0.1", Filename: "foo"},
				},
			},
			want: mustParse("gs://fake-source-bucket/fake/path/input.tgz"),
		},
	}
	objs := []fakestorage.Object{
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-source-bucket",
				Name:       "fake/path/input.tgz",
				Updated:    time.Now(),
			},
			Content: testingx.MustReadFile(t, "testdata/input.tgz"),
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
			p := &HopProcessor{
				client: client,
			}
			ctx := context.Background()
			src := p.Source(ctx, tt.row)
			if !reflect.DeepEqual(src.Path, tt.want) {
				t.Errorf("HopProcessor.Source() = %v, want %v", src.Path, tt.want)
			}
		})
	}
}

func TestHopProcessor_File(t *testing.T) {
	tests := []struct {
		name    string
		fromURL string
		netAnn  *annotator.Network
		wantErr bool
	}{
		{
			name: "success",
			netAnn: &annotator.Network{
				// Replace original client network annotation.
				Missing: true,
			},
			fromURL: "file://./testdata/hopannotations1.tgz",
		},
		{
			name: "bad-file-returns-original-content",
			netAnn: &annotator.Network{
				CIDR:     "62.115.0.0/16",
				ASNumber: 1299,
				ASName:   "Telia Company AB",
				Systems: []annotator.System{
					{ASNs: []uint32{1299}},
				},
			},
			fromURL: "file://./testdata/hopannotations1-badformat.tgz",
		},
		{
			name:    "error-parsing",
			fromURL: "file://./testdata/hopannotations1-unparsable.tgz",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, err := archive.NewFileSource(tt.fromURL)
			testingx.Must(t, err, "failed to read source file: %s", tt.fromURL)
			p := &HopProcessor{
				asn: &fakeAnno{netAnn: tt.netAnn},
				src: src,
			}
			h, b, err := src.NextFile()
			testingx.Must(t, err, "failed to get next file from source")

			bnew, err := p.File(h, b)
			if (err != nil) != tt.wantErr {
				t.Errorf("HopProcessor.File() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			an := hopannotation.HopAnnotation1{}
			err = json.Unmarshal(bnew, &an)
			testingx.Must(t, err, "failed to unmarshal processor result")
			// Verify that output is well formatted.
			if !reflect.DeepEqual(an.Annotations.Network, tt.netAnn) {
				t.Errorf("HopProcessor.File() = %v, want %v", an.Annotations.Network, tt.netAnn)
			}
		})
	}
}

func TestHopProcessor_Finish(t *testing.T) {
	tests := []struct {
		name      string
		outBucket string
		fromURL   string
		wantURL   string
	}{
		{
			name:      "success",
			outBucket: "fake-target-bucket",
			fromURL:   "gs://fake-source-bucket/ndt/annotation/2023/03/01/20230302T031500.576788Z-annotation-mlab1-chs0t-ndt.tgz",
			wantURL:   "gs://fake-target-bucket/ndt/annotation/2023/03/01/20230302T031500.576788Z-annotation-mlab1-chs0t-ndt.tgz",
		},
	}

	objs := []fakestorage.Object{
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-source-bucket",
				Name:       "ndt/annotation/2023/03/01/20230302T031500.576788Z-annotation-mlab1-chs0t-ndt.tgz",
				Updated:    time.Now(),
			},
			Content: testingx.MustReadFile(t, "testdata/input.tgz"),
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-target-bucket",
				Name:       "stub-to-create-bucket",
				Updated:    time.Now(),
			},
			Content: []byte{},
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
			ctx := context.Background()
			src, err := archive.NewGCSSource(ctx, client, tt.fromURL)
			testingx.Must(t, err, "failed to load gcs source")
			p := &HopProcessor{
				src:       src,
				outBucket: tt.outBucket,
				client:    client,
			}
			out := createTargetFromSource(src)
			err = p.Finish(ctx, out)
			if err != nil {
				t.Errorf("HopProcessor.Finish() error = %v, want nil", err)
				return
			}
			// Verify new file is in the fake-target-bucket.
			src2, err := archive.NewGCSSource(ctx, client, tt.wantURL)
			testingx.Must(t, err, "failed to load gcs source: %s", tt.wantURL)
			if src2.Size != src.Size {
				t.Errorf("HopProcessor.Finish() size = %d, want %d", src2.Size, src.Size)
			}
		})
	}
}
