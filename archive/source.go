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
	ErrNotRegularFile = errors.New("file type is not regular")
)

// tarSource provides Next and Read functions.
type tarSource interface {
	Next() (*tar.Header, error)
	Read(b []byte) (int, error)
}

// Source reads from a tar archive from Path containing test files.
type Source struct {
	// Path is the original archive URL.
	Path *Path
	// Count is the number of files read from the archive.
	Count int
	// Size is the number of bytes in the archive.
	Size int
	io.Closer
	tarSource
}

// NewFileSource creates a new Source for the named file.
// The file parameter should be a URL, like file:///path/to/filename.tgz
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
	size := buf.Len()

	// Uncompress the archive.
	gzr, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	// Untar the uncompressed archive.
	tarSource := tar.NewReader(gzr)

	s := &Source{
		Path:      path,
		tarSource: tarSource,
		Closer:    gzr,
		Size:      size,
	}
	return s, nil
}

// NewGCSSource creates a new Source from the given GCS object.
// The url parameter should be a GCS URL, like gs://bucket/path/to/filename.tgz
func NewGCSSource(ctx context.Context, client *storage.Client, url string) (*Source, error) {
	path, err := ParseArchiveURL(url)
	if err != nil {
		return nil, err
	}

	var buf *bytes.Buffer
	// Create reader and load content into memory.
	err = retry(1, func() error {
		buf = &bytes.Buffer{}
		rdr, err := path.Reader(ctx, client)
		if err != nil {
			return err
		}
		defer rdr.Close()
		total := int64(0)
		for total < rdr.Attrs.Size {
			n, err := io.Copy(buf, rdr)
			if err != nil {
				return err
			}
			total += n
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	size := buf.Len()

	// Uncompress the archive.
	gzr, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	// Untar the uncompressed archive.
	tarSource := tar.NewReader(gzr)

	// Create a closer to manage complete cleanup of all resources.
	gcs := &Source{
		Path:      path,
		tarSource: tarSource,
		Closer:    gzr,
		Size:      size,
	}
	return gcs, nil
}

// CopyHeader duplicates the given tar.Header, suitable for use in a new tar archive.
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
	var err error
	var data []byte
	var h *tar.Header

	// The tar data should be in memory, so there is no need to retry errors.
	h, err = s.tarSource.Next()
	if err != nil {
		return nil, nil, err
	}

	// Only process regular files.
	if h.Typeflag != tar.TypeReg {
		log.Println("unsupported file type:", h.Name, h.Typeflag)
		return nil, nil, ErrNotRegularFile
	}

	data, err = io.ReadAll(s.tarSource)
	if err == nil {
		s.Count++
	}
	return h, data, err
}

func retry(maxTries int, f func() error) error {
	tries := 0
	waitTime := time.Second
	var err error
	for err = f(); err != nil && tries < maxTries; err = f() {
		time.Sleep(waitTime)
		waitTime *= 2
		tries++
	}
	return err
}
