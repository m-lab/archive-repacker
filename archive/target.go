package archive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/m-lab/go/rtx"
)

var (
	repackerArchiveUploads = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "repacker_archive_uploads_total",
			Help: "The number of archives uploaded",
		},
	)
)

// A Target represents a single, compressed, tar archive containing files to be
// uploaded to GCS.
type Target struct {
	// Count is the number of files written to the archive.
	Count      int
	bytes      *bytes.Buffer
	tarTarget  *tar.Writer
	gzipTarget *gzip.Writer
}

// NewTarget creates a new Target for adding files to a compressed tar archive.
func NewTarget() *Target {
	buffer := &bytes.Buffer{}
	gzipTarget := gzip.NewWriter(buffer)
	tarTarget := tar.NewWriter(gzipTarget)
	return &Target{
		bytes:      buffer,
		tarTarget:  tarTarget,
		gzipTarget: gzipTarget,
	}
}

// AddFile appends a single file to the Target with the given header and file contents.
func (ar *Target) AddFile(h *tar.Header, contents []byte) error {
	if h == nil {
		return nil
	}
	err := ar.tarTarget.WriteHeader(h)
	if err != nil {
		return fmt.Errorf("writing header for %q failed: %w", h.Name, err)
	}
	for total := 0; total < len(contents); {
		n, err := ar.tarTarget.Write(contents[total:])
		if err != nil {
			return fmt.Errorf("writing contents for %q failed: %w", h.Name, err)
		}
		total += n
	}
	// Flush the data so that our in-memory filesize is accurate.
	rtx.Must(ar.tarTarget.Flush(), "Could not flush the tarTarget")
	rtx.Must(ar.gzipTarget.Flush(), "Could not flush the gzipTarget")
	ar.Count++
	return nil
}

// Close closes the Target archive.
func (ar *Target) Close() error {
	ar.tarTarget.Close()
	return ar.gzipTarget.Close()
}

// Upload writes the completed Target archive contents to the named GCS Path.
func (ar *Target) Upload(ctx context.Context, client *storage.Client, p *Path) error {
	sctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()

	ar.Close()
	err := retry(1, func() error {
		writer := p.Writer(sctx, client)
		defer writer.Close()

		contents := ar.bytes.Bytes()
		for total := 0; total < len(contents); {
			n, err := writer.Write(contents[total:])
			if err != nil {
				return fmt.Errorf("writing content to %q failed: %w", p.String(), err)
			}
			total += n
		}
		repackerArchiveUploads.Inc()
		return nil
	})
	if err != nil {
		return fmt.Errorf("closing writer for %q failed: %w", p.String(), err)
	}
	return nil
}
