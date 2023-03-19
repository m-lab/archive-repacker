package archive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
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

// A Writer represents a single, compressed, tar archive containing files to be
// uploaded to GCS.
type Writer struct {
	Bytes      *bytes.Buffer
	tarWriter  *tar.Writer
	gzipWriter *gzip.Writer
	Count      int
}

// NewWriter creates a new Writer for adding files to a compressed tar archive.
func NewWriter() *Writer {
	buffer := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(buffer)
	tarWriter := tar.NewWriter(gzipWriter)
	return &Writer{
		Bytes:      buffer,
		tarWriter:  tarWriter,
		gzipWriter: gzipWriter,
	}
}

// AddFile appends a single file to the Writer with the given header and file contents.
func (ar *Writer) AddFile(h *tar.Header, contents []byte) {
	if h != nil {
		rtx.Must(ar.tarWriter.WriteHeader(h), "Could not write the tarfile header for %v", h.Name)
		_, err := ar.tarWriter.Write(contents)
		rtx.Must(err, "Could not write the tarfile contents for %v", h.Name)
		// Flush the data so that our in-memory filesize is accurate.
		rtx.Must(ar.tarWriter.Flush(), "Could not flush the tarWriter")
		rtx.Must(ar.gzipWriter.Flush(), "Could not flush the gzipWriter")
		ar.Count++
	}
}

// Close closes the Writer archive.
func (ar *Writer) Close() error {
	ar.tarWriter.Close()
	return ar.gzipWriter.Close()
}

// Uploads the completed Writer archive contents to GCS.
func (ar *Writer) Upload(ctx context.Context, client *storage.Client, p *Path) error {
	sctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()

	writer := p.Writer(sctx, client)
	ar.Close()

	contents := ar.Bytes.Bytes()
	for total := 0; total < len(contents); {
		n, err := writer.Write(contents[total:])
		if err != nil {
			return err
		}
		total += n
	}
	repackerArchiveUploads.Inc()
	return writer.Close()
}
