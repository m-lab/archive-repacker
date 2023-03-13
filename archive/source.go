package archive

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"time"

	"cloud.google.com/go/storage"
)

var (
	ErrCorrupt        = errors.New("file data was corrupt")
	ErrNotRegularFile = errors.New("file type is not regular")
)

// TarReader provides Next and Read functions.
type TarReader interface {
	Next() (*tar.Header, error)
	Read(b []byte) (int, error)
}

// Source reads from a GCS tar file containing test files.
type Source struct {
	Path *Path
	TarReader
	io.Closer
	Bytes         *bytes.Buffer
	RetryBaseTime time.Duration // The base time for backoff and retry.
	Count         int
}

func NewFileSource(file string) (*Source, error) {
	path, err := ParseArchiveURL(file)
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadFile(path.Filename())
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(b)

	// Uncompress the archive.
	gzr, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	// Untar the uncompressed archive.
	tarReader := tar.NewReader(gzr)

	// Create a closer to manage complete cleanup of all resources.
	closer := &Closer{gzr, nil}

	s := &Source{
		Path:          path,
		TarReader:     tarReader,
		Closer:        closer,
		Bytes:         buf,
		RetryBaseTime: 16 * time.Millisecond,
	}
	return s, nil
}

// NewSource creates a Source for iterating through every archive file. Caller
// is responsible for calling Close on the returned object.  path should be of
// form gs://bucket/filename.tgz
func NewSource(ctx context.Context, client *storage.Client, url string) (*Source, error) {
	// NOTE: cancel is called by the closer.
	ctx, cancel := context.WithCancel(ctx)
	var rdr *storage.Reader
	buf := &bytes.Buffer{}

	path, err := ParseArchiveURL(url)
	if err != nil {
		cancel()
		return nil, err
	}

	// Create reader.
	err = Retry(1, func() error {
		rdr, err = path.Reader(ctx, client)
		if err != nil {
			return err
		}
		total := int64(0)
		for total < rdr.Size() {
			n, err := io.Copy(buf, rdr)
			if err != nil {
				return err
			}
			total += n
		}
		return nil
	})
	if err != nil {
		cancel()
		return nil, err
	}
	rdr.Close()

	// Uncompress the archive.
	gzr, err := gzip.NewReader(buf)
	if err != nil {
		cancel()
		rdr.Close()
		return nil, err
	}
	// Untar the uncompressed archive.
	tarReader := tar.NewReader(gzr)

	// Create a closer to manage complete cleanup of all resources.
	closer := &Closer{gzr, cancel}

	gcs := &Source{
		Path:          path,
		TarReader:     tarReader,
		Closer:        closer,
		Bytes:         buf,
		RetryBaseTime: 16 * time.Millisecond,
	}
	return gcs, nil
}

func CopyHeader(h *tar.Header) *tar.Header {
	n := &tar.Header{
		Typeflag: h.Typeflag,
		Name:     h.Name,
		Linkname: h.Linkname,
		Size:     h.Size,
		Mode:     h.Mode,
		Uid:      h.Uid,
		Gid:      h.Gid,
		Uname:    h.Uname,
		Gname:    h.Gname,

		ModTime:    h.ModTime,
		AccessTime: h.AccessTime,
		ChangeTime: h.ChangeTime,
		Devmajor:   h.Devmajor,
		Devminor:   h.Devminor,
		Format:     h.Format,
	}
	if h.PAXRecords != nil {
		n.PAXRecords = map[string]string{}
		for k, v := range h.PAXRecords {
			n.PAXRecords[k] = v
		}
	}
	return n
}

// NextFile reads the next file from the source, returning the original tar header
// and file bytes. When the archive is completely read, NextFile returns io.EOF.
func (s *Source) NextFile() (*tar.Header, []byte, error) {
	// TODO: add metrics.

	// Try to get the next file.  We retry multiple times, because sometimes
	// GCS stalls and produces stream errors.
	var err error
	var data []byte
	var h *tar.Header

	// The tar data should be in memory, so there is no need to retry errors.
	h, err = s.TarReader.Next()
	if err == io.EOF || err == io.ErrUnexpectedEOF || err != nil {
		return nil, nil, err
	}

	// Only process regular files.
	if h.Typeflag != tar.TypeReg {
		log.Println("unsupported file type:", h.Name, h.Typeflag)
		return nil, nil, ErrNotRegularFile
	}

	data, err = io.ReadAll(s.TarReader)
	if err == nil {
		s.Count++
	}
	return h, data, err
}

// Closer handles gzip files.
type Closer struct {
	zipper io.Closer // Must be non-null
	cancel func()    // Context cancel.
}

// Close invokes the gzip and body Close() functions.
func (t *Closer) Close() error {
	if t.cancel != nil {
		defer t.cancel()
	}
	var err error
	if t.zipper != nil {
		err = t.zipper.Close()
	}
	return err
}
