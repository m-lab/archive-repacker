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

// tarReader provides Next and Read functions.
type tarReader interface {
	Next() (*tar.Header, error)
	Read(b []byte) (int, error)
}

// Reader reads from a tar archive from Path containing test files.
type Reader struct {
	// Path is the original archive URL.
	Path *Path
	// Count is the number of files read from the archive.
	Count int
	io.Closer
	tarReader
}

// NewFileReader creates a new Reader for the named file.
// The file parameter should be a URL, like file:///path/to/filename.tgz
func NewFileReader(file string) (*Reader, error) {
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

	s := &Reader{
		Path:      path,
		tarReader: tarReader,
		Closer:    gzr,
	}
	return s, nil
}

// NewGCSReader creates a new Reader from the given GCS object.
// The url parameter should be a GCS URL, like gs://bucket/path/to/filename.tgz
func NewGCSReader(ctx context.Context, client *storage.Client, url string) (*Reader, error) {
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

	// Uncompress the archive.
	gzr, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	// Untar the uncompressed archive.
	tarReader := tar.NewReader(gzr)

	// Create a closer to manage complete cleanup of all resources.
	gcs := &Reader{
		Path:      path,
		tarReader: tarReader,
		Closer:    gzr,
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
func (s *Reader) NextFile() (*tar.Header, []byte, error) {
	var err error
	var data []byte
	var h *tar.Header

	// The tar data should be in memory, so there is no need to retry errors.
	h, err = s.tarReader.Next()
	if err != nil {
		return nil, nil, err
	}

	// Only process regular files.
	if h.Typeflag != tar.TypeReg {
		log.Println("unsupported file type:", h.Name, h.Typeflag)
		return nil, nil, ErrNotRegularFile
	}

	data, err = io.ReadAll(s.tarReader)
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
