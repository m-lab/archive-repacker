package routeview

import (
	"net/url"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/m-lab/go/testingx"

	"github.com/m-lab/go/rtx"
)

// gs://fake-test-bucket/RouteViewIPv4/2022/12/routeviews-rv2-20221202-1200.pfx2as.gz

func TestURL_Next(t *testing.T) {

	tests := []struct {
		name string
		url  string
		date string
		want string
	}{
		{
			name: "success-ipv4",
			url:  "gs://fake-test-bucket/RouteViewIPv4",
			date: "2022-12-02",
			want: "gs://fake-test-bucket/RouteViewIPv4/2022/12/routeviews-rv2-20221202-1200.pfx2as.gz",
		},
		{
			name: "success-ipv6",
			url:  "gs://fake-test-bucket/RouteViewIPv6",
			date: "2022-12-02",
			want: "gs://fake-test-bucket/RouteViewIPv6/2022/12/routeviews-rv6-20221202-1200.pfx2as.gz",
		},
	}
	objs := []fakestorage.Object{
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-test-bucket",
				Name:       "RouteViewIPv4/2022/12/routeviews-rv2-20221201-1200.pfx2as.gz",
				Updated:    time.Now(),
			},
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-test-bucket",
				Name:       "RouteViewIPv4/2022/12/routeviews-rv2-20221202-1200.pfx2as.gz",
				Updated:    time.Now(),
			},
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "fake-test-bucket",
				Name:       "RouteViewIPv6/2022/12/routeviews-rv6-20221202-1200.pfx2as.gz",
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
			p := NewURL(client, tt.url)
			u, err := url.Parse(tt.want)
			rtx.Must(err, "failed to parse url:", tt.want)
			if got := p.Next(tt.date); *got != *u {
				t.Errorf("URL.Next() = %v, want %v", got, tt.want)
			}
		})
	}
}
