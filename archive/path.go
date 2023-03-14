package archive

import (
	"context"
	"errors"
	"net/url"
	"path"
	"strings"

	"cloud.google.com/go/storage"
)

// Path represents a parsed URL to an object in GCS or local file.
type Path struct {
	*url.URL
}

// ParseURL parses a GCS path of any object type.
func ParseURL(path string) (*Path, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "gs" && u.Scheme != "file" {
		return nil, errors.New("unsupported scheme:" + u.Scheme)
	}
	return &Path{URL: u}, nil
}

// ParseArchiveURL parses a gs:// or file:// path of a .tgz archive.
func ParseArchiveURL(path string) (*Path, error) {
	p, err := ParseURL(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(path, ".tgz") {
		return nil, errors.New("unsupported file extension:" + path)
	}
	return p, nil
}

// Dup creates a new Path with an alternate named bucket.
func (p *Path) Dup(bucket string) *Path {
	u := *p.URL
	u.Host = bucket
	return &Path{&u}
}

// Bucket returns the path bucket name.
func (p *Path) Bucket() string {
	return p.Hostname()
}

// Object returns the Object name.
func (p *Path) Object() string {
	return strings.TrimPrefix(p.Path, "/")
}

// Filename returns a filename based on the Path.
func (p *Path) Filename() string {
	return path.Join(p.Host, p.Path)
}

// Reader creates a GCS reader from this Path object. Caller is responsible for calling Close on readers.
func (p *Path) Reader(ctx context.Context, client *storage.Client) (*storage.Reader, error) {
	obj := client.Bucket(p.Bucket()).Object(p.Object())
	return obj.NewReader(ctx)
}

// Reader creates a GCS writer to this Path object. Caller is responsible for calling Close on writers.
func (p *Path) Writer(ctx context.Context, client *storage.Client) *storage.Writer {
	obj := client.Bucket(p.Bucket()).Object(p.Object())
	return obj.NewWriter(ctx)
}
