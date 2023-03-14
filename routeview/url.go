package routeview

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"

	"github.com/m-lab/archive-repacker/archive"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/go/storagex"
)

type URL struct {
	client  *storage.Client
	version string
	path    *archive.Path
}

func NewURL(client *storage.Client, prefix string) *URL {
	p, err := archive.ParseURL(prefix)
	rtx.Must(err, "failed to parse prefix")
	version := "rv2"
	if strings.Contains(p.Path, "IPv6") {
		version = "rv6"
	}
	return &URL{
		client:  client,
		version: version,
		path:    p,
	}
}

func (u *URL) Next(date string) *url.URL {
	//ctx := context.Background()
	//client, err := gcs.NewClient(ctx)
	//rtx.Must(err, "Failed to allocate storage.Client")

	// https://storage.cloud.google.com/downloader-mlab-sandbox/RouteViewIPv4/2023/02/routeviews-rv2-20230205-2200.pfx2as.gz
	d, err := civil.ParseDate(date)
	rtx.Must(err, "failed to parse given date")
	ym := fmt.Sprintf("%04d/%02d", d.Year, d.Month)
	file := fmt.Sprintf("routeviews-%s-%04d%02d%02d", u.version, d.Year, d.Month, d.Day)
	datePrefix := path.Join(u.path.Object(), ym)

	result := ""
	bucket := storagex.NewBucket(u.client.Bucket(u.path.Bucket()))
	bucket.Walk(context.TODO(), datePrefix+"/", func(o *storagex.Object) error {
		if strings.HasPrefix(o.LocalName(), file) {
			result = "gs://" + u.path.Bucket() + "/" + o.ObjectName()
			return io.EOF
		}
		return nil
	})

	ur, err := url.Parse(result)
	rtx.Must(err, "failed to parse url:", result)
	return ur
}
