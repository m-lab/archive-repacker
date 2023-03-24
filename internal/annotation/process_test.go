package annotation

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/m-lab/uuid-annotator/annotator"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/m-lab/archive-repacker/archive"
	"github.com/m-lab/go/testingx"
)

func TestProcessor_Init(t *testing.T) {
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

		p := NewProcessor(client, "", rv4.URL, rv6.URL, asnames.URL)

		// Not exiting is success.
		ctx := context.Background()
		p.Init(ctx, date)
	})
}

func mustParse(u string) *archive.Path {
	p, err := archive.ParseURL(u)
	if err != nil {
		panic(err)
	}
	return p
}

func TestProcessor_Source(t *testing.T) {
	log.SetOutput(io.Discard)
	tests := []struct {
		name  string
		row   Result
		files map[string]string
		want  *archive.Path
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
			files: map[string]string{
				"foo": "127.0.0.1",
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
			p := &Processor{
				client: client,
			}
			ctx := context.Background()
			src := p.Source(ctx, tt.row)
			if !reflect.DeepEqual(src.Path, tt.want) {
				t.Errorf("Processor.Source() = %v, want %v", src.Path, tt.want)
			}
			if !reflect.DeepEqual(p.files, tt.files) {
				t.Errorf("Processor.Source() = %v, want %v", p.files, tt.files)
			}
		})
	}
}

type fakeAnno struct {
	annotator.Annotator // Satisfies asnannotator.ASNAnnotator interface, but not used.
	netAnn              *annotator.Network
}

func (f *fakeAnno) Reload(context.Context) {}
func (f *fakeAnno) AnnotateIP(src string) *annotator.Network {
	return f.netAnn
}

func TestProcessor_File(t *testing.T) {
	tests := []struct {
		name    string
		fromURL string
		files   map[string]string
		src     *archive.Source
		netAnn  *annotator.Network
		want    []byte
		wantErr bool
	}{
		{
			name: "success",
			files: map[string]string{
				"ndt-fxmwp_1678992660_000000000005D8AF.json": "127.0.0.1",
			},
			netAnn: &annotator.Network{
				// Replace original client network annotation.
				Missing: true,
			},
			fromURL: "file://./testdata/annotations.tgz",
		},
		{
			name:  "success-file-missing-from-processor-files",
			files: map[string]string{},
			// Expect the same client network annotation.
			netAnn: &annotator.Network{
				CIDR:     "2600:3c02::/32",
				ASNumber: 63949,
				ASName:   "Linode, LLC",
				Systems: []annotator.System{
					{ASNs: []uint32{63949}},
				},
			},
			fromURL: "file://./testdata/annotations.tgz",
		},
		{
			name: "error-parsing",
			files: map[string]string{
				"ndt-fxmwp_1678992660_000000000005D8AF.json": "127.0.0.1",
			},
			fromURL: "file://./testdata/annotations-unparsable.tgz",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, err := archive.NewFileSource(tt.fromURL)
			testingx.Must(t, err, "failed to read source file: %s", tt.fromURL)
			p := &Processor{
				asn:   &fakeAnno{netAnn: tt.netAnn},
				files: tt.files,
				src:   src,
			}
			h, b, err := src.NextFile()
			testingx.Must(t, err, "failed to get next file from source")

			bnew, err := p.File(h, b)
			if (err != nil) != tt.wantErr {
				t.Errorf("Processor.File() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			an := annotator.Annotations{}
			err = json.Unmarshal(bnew, &an)
			testingx.Must(t, err, "failed to unmarshal processor result")
			// Verify that output is well formatted.
			if !reflect.DeepEqual(an.Client.Network, tt.netAnn) {
				t.Errorf("Processor.File() = %v, want %v", an, tt.want)
			}
		})
	}
}

func createTargetFromSource(src *archive.Source) *archive.Target {
	out := archive.NewTarget()
	for {
		h, b, err := src.NextFile()
		if err != nil {
			break
		}
		out.AddFile(h, b)
	}
	return out
}

func TestProcessor_Finish(t *testing.T) {
	tests := []struct {
		name      string
		files     map[string]string
		src       *archive.Source
		outBucket string
		fromURL   string
		wantURL   string
		wantErr   bool
	}{
		{
			name: "success",
			files: map[string]string{
				"foo": "bar",
			},
			outBucket: "fake-target-bucket",
			fromURL:   "gs://fake-source-bucket/ndt/annotation/2023/03/01/20230302T031500.576788Z-annotation-mlab1-chs0t-ndt.tgz",
			wantURL:   "gs://fake-target-bucket/ndt/annotation2/2023/03/01/20230302T031500.576788Z-annotation2-mlab1-chs0t-ndt.tgz",
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
			p := &Processor{
				files:     tt.files,
				src:       src,
				outBucket: tt.outBucket,
				client:    client,
			}
			out := createTargetFromSource(src)
			err = p.Finish(ctx, out)
			if (err != nil) != tt.wantErr {
				t.Errorf("Processor.Finish() error = %v, wantErr %v", err, tt.wantErr)
			}
			// Verify new file is in the fake-target-bucket.
			src2, err := archive.NewGCSSource(ctx, client, tt.wantURL)
			testingx.Must(t, err, "failed to load gcs source: %s", tt.wantURL)
			if src2.Size != src.Size {
				t.Errorf("Finish() size = %d, want %d", src2.Size, src.Size)
			}
		})
	}
}
