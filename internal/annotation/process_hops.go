package annotation

import (
	"archive/tar"
	"context"
	"encoding/json"
	"log"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/m-lab/archive-repacker/archive"
	"github.com/m-lab/archive-repacker/internal/process"
	"github.com/m-lab/archive-repacker/routeview"
	"github.com/m-lab/go/content"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/uuid-annotator/annotator"
	"github.com/m-lab/uuid-annotator/asnannotator"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"cloud.google.com/go/storage"
)

var (
	repackerHopFileUnparsable = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "repacker_hop_file_unparsable_total",
			Help: "The number of hop annotation files that could not be parsed",
		},
	)
)

// Processor maintains state for reprocessing annotation archives.
type HopProcessor struct {
	asn       asnannotator.ASNAnnotator
	rv4       *url.URL        // IPv4 routeview prefix2as dataset.
	rv6       *url.URL        // IPv6 routeview prefix2as dataset.
	names     *url.URL        // asname dataset.
	src       *archive.Source // source archive.
	outBucket string          // output GCS bucket.
	client    *storage.Client
}

// NewHopProcessor creates a new annotation processor.
func NewHopProcessor(client *storage.Client, outBucket string, rv4, rv6, asnames *url.URL) *HopProcessor {
	return &HopProcessor{
		rv4:       rv4,
		rv6:       rv6,
		names:     asnames,
		outBucket: outBucket,
		client:    client,
	}
}

// Init downloads the routeview datasets for the given date and initializes the ASN annotator.
func (p *HopProcessor) Init(ctx context.Context, date string) {
	// Download ipv4 routeview data for given date.
	u, err := routeview.NewURLGenerator(p.client, p.rv4.String()).Next(ctx, date)
	rtx.Must(err, "Could not generate routeview v4 URL")
	p4 := &gcsProvider{Path: &archive.Path{URL: u}, Client: p.client}

	// Download ipv6 routeview data for given date.
	u, err = routeview.NewURLGenerator(p.client, p.rv6.String()).Next(ctx, date)
	rtx.Must(err, "Could not generate routeview v6 URL")
	p6 := &gcsProvider{Path: &archive.Path{URL: u}, Client: p.client}

	// Load asnames.
	asnames, err := content.FromURL(ctx, p.names)
	rtx.Must(err, "Could not load AS names URL")

	// Create asn annotator.
	p.asn = asnannotator.New(ctx, p4, p6, asnames, []net.IP{})
}

// Source generates a new archive.Reader for the result row.ArchiveURL.
func (p *HopProcessor) Source(ctx context.Context, row Result) *archive.Source {
	log.Println("Starting", row.ArchiveURL)
	// Download GCS archive.
	src, err := archive.NewGCSSource(ctx, p.client, row.ArchiveURL)
	rtx.Must(err, "failed to create new source for %s", row.ArchiveURL)
	p.src = src
	return src
}

// File processes the given file header and file contents. File returns the new
// file content or process.ErrCorrupt.
func (p *HopProcessor) File(h *tar.Header, b []byte) ([]byte, error) {
	// Parse annotation.
	an := annotator.Annotations{}
	err := json.Unmarshal(b, &an)
	if err != nil {
		log.Println("Error Unmarshal file:", h.Name, err)
		repackerQueryFilesCorrupt.Inc()
		// Since file is corrupt, do not add to output.
		return nil, process.ErrCorrupt
	}

	fields := strings.Split(strings.ReplaceAll(h.Name, ".json", ""), "_")
	if len(fields) != 3 {
		// We cannot parse this filename to identify the IP.
		log.Println("Skipping unparsable filename:", h.Name)
		repackerHopFileUnparsable.Inc()
		return b, nil
	}

	before := an.Client.Network
	// Recreate Network annotation using client IP.
	an.Client.Network = p.asn.AnnotateIP(fields[2])

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
func (p *HopProcessor) Finish(ctx context.Context, out *archive.Target) error {
	uctx, ucancel := context.WithTimeout(ctx, 10*time.Minute)
	defer ucancel()
	o := p.src.Path.Dup(p.outBucket)
	return out.Upload(uctx, p.client, o)
}
