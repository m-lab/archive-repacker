package annotation

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"

	"github.com/m-lab/archive-repacker/archive"
	"github.com/m-lab/go/storagex"
)

// Renamer manages GCS operations to rename objects from one datatype to a new datatype.
type Renamer struct {
	client       *storage.Client
	bucket       string
	experiment   string
	fromDatatype string
	newDatatype  string
}

// NewRenamer creates a new Renamer. Objects are listed from bucket and written to bucket.
func NewRenamer(client *storage.Client, bucket, experiment, fromDatatype, newDatatype string) *Renamer {
	return &Renamer{
		client:       client,
		bucket:       bucket,
		experiment:   experiment,
		fromDatatype: fromDatatype,
		newDatatype:  newDatatype,
	}
}

// List returns GCS URLs for every fromDatatype object under the given date prefix.
func (r *Renamer) List(ctx context.Context, date string) ([]string, error) {
	d, err := civil.ParseDate(date)
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("%s/%s/%04d/%02d/%02d", r.experiment, r.fromDatatype, d.Year, d.Month, d.Day)
	log.Printf("Listing files under: gs://%s/%s", r.bucket, prefix)

	// Individual days should only have 10-20k files.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	var results []string
	bucket := storagex.NewBucket(r.client.Bucket(r.bucket))
	for trial := 0; trial < 2; trial++ {
		results = []string{}
		err = bucket.Walk(ctx, prefix+"/", func(o *storagex.Object) error {
			object := "gs://" + r.bucket + "/" + o.ObjectName()
			results = append(results, object)
			return nil
		})
		if err != nil {
			log.Printf("retrying; list of %q returned error: %v", prefix, err)
			continue
		}
		break
	}
	if err != nil {
		return nil, err
	}
	log.Printf("List found %d files for %q", len(results), prefix)
	return results, err
}

// Rename copies the named URL to a new object, replacing fromDatatype with
// newDatatype in the object name.
func (r *Renamer) Rename(ctx context.Context, url string) (string, error) {
	src, err := archive.ParseArchiveURL(url)
	if err != nil {
		return "", err
	}
	// Copy bucket & object path.
	dst := src.Dup(r.bucket)
	// Example annotation object path:
	// * src: gs:/bucket1/ndt/annotation/2023/03/01/20230302T031500.576788Z-annotation-mlab1-chs0t-ndt.tgz
	// * dst: gs:/bucket2/ndt/annotation2/2023/03/01/20230302T031500.576788Z-annotation2-mlab1-chs0t-ndt.tgz
	//
	// Note: bucket1 and bucket2 could be the same value.
	//
	// Replace the original datatype with the new one.
	dst.Path = strings.ReplaceAll(dst.Path, r.fromDatatype+"-", r.newDatatype+"-")
	dst.Path = strings.ReplaceAll(dst.Path, r.fromDatatype+"/", r.newDatatype+"/")

	// Individual files (under ~50MB) should not take longer than an hour.
	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()

	srcObj := r.client.Bucket(src.Bucket()).Object(src.Object())
	dstObj := r.client.Bucket(dst.Bucket()).Object(dst.Object())

	// Unconditionally overwrite the dst object.
	// Note: Copiers go through the client: read from GCS then write to GCS.
	for trial := 0; trial < 2; trial++ {
		_, err := dstObj.CopierFrom(srcObj).Run(ctx)
		if err != nil {
			log.Printf("Failed to copy %q, err: %v", dst, err)
			continue
		}
		break
	}
	return dst.String(), err
}
