package annotation

import (
	"archive/tar"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/m-lab/archive-repacker/internal/process"

	"github.com/m-lab/archive-repacker/archive"
	"github.com/m-lab/archive-repacker/routeview"
	"github.com/m-lab/go/content"
	"github.com/m-lab/go/logx"
	"github.com/m-lab/go/pretty"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/uuid-annotator/annotator"
	"github.com/m-lab/uuid-annotator/asnannotator"

	"cloud.google.com/go/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	repackerQueryFilesMissing = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "repacker_query_files_missing_total",
			Help: "The number of files returned by the query not found in archives",
		},
	)
	repackerQueryFilesCorrupt = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "repacker_query_files_corrupt_total",
			Help: "The number of files returned by the query that appear corrupt",
		},
	)
	repackerAnnotations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "repacker_annotations_total",
			Help: "The number of annotation files processed",
		},
		[]string{"status"},
	)
)

// Processor maintains state for reprocessing annotation archives.
type Processor struct {
	asn       asnannotator.ASNAnnotator
	rv4       *url.URL          // IPv4 routeview prefix2as dataset.
	rv6       *url.URL          // IPv6 routeview prefix2as dataset.
	names     *url.URL          // asname dataset.
	files     map[string]string // files found in the current archive.
	src       *archive.Source   // source archive.
	outBucket string            // output GCS bucket.
	client    *storage.Client
}

// Result is a structure to read BigQuery result rows.
type Result struct {
	// ArchiveURL is the GCS URL of the archive that contains Files.
	ArchiveURL string
	// Files is an array of tuples for annotation Filename and client DstIP addresses.
	Files []struct {
		DstIP    string
		Filename string
	}
}

// NewProcessor creates a new annotation processor.
func NewProcessor(client *storage.Client, outBucket string, rv4, rv6, asnames *url.URL) *Processor {
	return &Processor{
		rv4:       rv4,
		rv6:       rv6,
		names:     asnames,
		outBucket: outBucket,
		client:    client,
	}
}

// gcsProvider loads archives from GCS.
type gcsProvider struct {
	Path   *archive.Path
	Client *storage.Client
}

// Get satisfies the content.Provider interface for the asn annotatator.
func (g *gcsProvider) Get(ctx context.Context) ([]byte, error) {
	o := g.Client.Bucket(g.Path.Bucket()).Object(g.Path.Object())
	r, err := o.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

// Init downloads the routeview datasets for the given date and initializes the ASN annotator.
func (p *Processor) Init(ctx context.Context, date string) {
	// Download ipv4 routeview data for given date.
	u, err := routeview.NewURLGenerator(p.client, p.rv4.String()).Next(ctx, date)
	rtx.Must(err, "Could generate routeview v4 URL")
	p4 := &gcsProvider{Path: &archive.Path{URL: u}, Client: p.client}

	// Download ipv6 routeview data for given date.
	u, err = routeview.NewURLGenerator(p.client, p.rv6.String()).Next(ctx, date)
	rtx.Must(err, "Could generate routeview v6 URL")
	p6 := &gcsProvider{Path: &archive.Path{URL: u}, Client: p.client}

	// Load asnames.
	asnames, err := content.FromURL(ctx, p.names)
	rtx.Must(err, "Could not load AS names URL")

	// Create asn annotator.
	p.asn = asnannotator.New(ctx, p4, p6, asnames, []net.IP{})
}

// Source generates a new archive.Reader for the result row.ArchiveURL.
func (p *Processor) Source(ctx context.Context, row Result) *archive.Source {
	log.Println("Starting", row.ArchiveURL)
	// Collect files from query to count missing files.
	files := map[string]string{}
	for i := range row.Files {
		f := row.Files[i]
		files[f.Filename] = f.DstIP
	}
	p.files = files
	logx.Debug.Println(row.ArchiveURL, len(row.Files), len(files))

	// Download GCS archive.
	src, err := archive.NewGCSSource(ctx, p.client, row.ArchiveURL)
	rtx.Must(err, "failed to create new source for %s", row.ArchiveURL)
	p.src = src
	return src
}

// File processes the given file header and file contents. File returns the new
// file content or process.ErrCorrupt.
func (p *Processor) File(h *tar.Header, b []byte) ([]byte, error) {
	// Parse annotation.
	an := annotator.Annotations{}
	err := json.Unmarshal(b, &an)
	if err != nil {
		log.Println("Error Unmarshal file:", h.Name, err)
		delete(p.files, h.Name)
		repackerQueryFilesCorrupt.Inc()
		// Since file is corrupt, do not add to output.
		return nil, process.ErrCorrupt
	}

	// Lookup IP for replacement network annotation.
	ip, ok := p.files[h.Name]
	if !ok {
		log.Println("Missing file from query results:", h.Name)
		repackerQueryFilesMissing.Inc()
		// Since we can't update this one, return original content.
		return b, nil
	}

	delete(p.files, h.Name)
	before := an.Client.Network
	// Recreate Network annotation using client IP.
	an.Client.Network = p.asn.AnnotateIP(ip)

	// Track how frequently the annotation was previously missing or updated.
	if before == nil || before.Missing {
		repackerAnnotations.WithLabelValues("was-missed").Inc()
	} else if before.ASNumber != an.Client.Network.ASNumber {
		repackerAnnotations.WithLabelValues("asn-update").Inc()
	} else {
		repackerAnnotations.WithLabelValues("equal").Inc()
	}

	// Serialize annotation again.
	b, err = json.Marshal(an)
	rtx.Must(err, "failed to marshal new annotation")
	return b, nil
}

// Finish completes processing of the given output archive by uploading to GCS
// to an alternate bucket and object name.
func (p *Processor) Finish(ctx context.Context, out *archive.Target) error {
	if len(p.files) != 0 {
		log.Println("FILES FROM QUERY NOT UPDATED IN ARCHIVE:", len(p.files), p.src.Path.String())
		pretty.Print(p.files)
		log.Println("")
	}
	uctx, ucancel := context.WithTimeout(ctx, 10*time.Minute)
	defer ucancel()
	o := p.src.Path.Dup(p.outBucket)
	// ndt/annotation/2023/03/01/20230302T031500.576788Z-annotation-mlab1-chs0t-ndt.tgz
	o.Path = strings.ReplaceAll(o.Path, "annotation-", "annotation2-")
	o.Path = strings.ReplaceAll(o.Path, "annotation/", "annotation2/")
	return out.Upload(uctx, p.client, o)
}
