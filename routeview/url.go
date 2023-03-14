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

// URLGenerator uses a GCS archive to identify per-date routeview prefix2as
// archive URLs.
type URLGenerator struct {
	client  *storage.Client
	version string
	path    *archive.Path
}

// NewURLGenerator creates a new URLGenerator for files under the given GCS prefix URL.
func NewURLGenerator(client *storage.Client, prefix string) *URLGenerator {
	p, err := archive.ParseURL(prefix)
	rtx.Must(err, "failed to parse prefix")
	version := "rv2"
	if strings.Contains(p.Path, "IPv6") {
		version = "rv6"
	}
	return &URLGenerator{
		client:  client,
		version: version,
		path:    p,
	}
}

// Next returns a routeview prefix2as URL for the named date. If no file is found, the process exits.
func (u *URLGenerator) Next(ctx context.Context, date string) (*url.URL, error) {
	// An example of GCS URLs we are searching:
	// * gs://downloader-mlab-sandbox/RouteViewIPv4/2023/02/routeviews-rv2-20230205-2200.pfx2as.gz
	d, err := civil.ParseDate(date)
	if err != nil {
		return nil, err
	}
	ym := fmt.Sprintf("%04d/%02d", d.Year, d.Month)
	file := fmt.Sprintf("routeviews-%s-%04d%02d%02d", u.version, d.Year, d.Month, d.Day)
	datePrefix := path.Join(u.path.Object(), ym)

	result := ""
	bucket := storagex.NewBucket(u.client.Bucket(u.path.Bucket()))
	bucket.Walk(ctx, datePrefix+"/", func(o *storagex.Object) error {
		if strings.HasPrefix(o.LocalName(), file) {
			result = "gs://" + u.path.Bucket() + "/" + o.ObjectName()
			return io.EOF
		}
		return nil
	})
	return url.Parse(result)
}
