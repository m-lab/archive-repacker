package process

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/m-lab/archive-repacker/archive"
	"github.com/m-lab/archive-repacker/internal/jobs"
	"github.com/m-lab/archive-repacker/query"
)

var (
	// MaxDelaySeconds is the maximum number of seconds to randomly wait in
	// response to BigQuery errors.
	MaxDelaySeconds = 60
	// QueryRetries is the maximum number of times to retry a query.
	QueryRetries = 2
	// ErrCorrupt may be returned by a processor implementation if the file
	// content should be considered corrupt and not included in the output archive.
	ErrCorrupt = errors.New("file content is corrupt")
)

var (
	repackerArchives = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "repacker_archives_total",
			Help: "The number of archives processed",
		},
	)
	repackerQueryErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "repacker_query_errors_total",
			Help: "The number of times the query returned an error",
		},
	)
	repackerQueryCompletionTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "repacker_query_completion_time",
			Help: "Histogram of query completion times",
			Buckets: []float64{
				1.0, 2.15, 4.64,
				10, 21.5, 46.4,
				100, 215, 464,
				1000, 2150, 4640,
			},
		},
	)
	repackerArchiveCompletionTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "repacker_archive_completion_time",
			Help: "Histogram of archive completion times",
			Buckets: []float64{
				1.0, 2.15, 4.64,
				10, 21.5, 46.4,
				100, 215, 464,
				1000, 2150, 4640,
			},
		},
	)
)

// Processor interface uses a type parameter for BigQuery result row types. The
// interface expects all jobs to be batched per date, and process every file of
// every archive.
type Processor[Row any] interface {
	// Init sets up the processor for processing the given date, e.g. downloading daily databases.
	Init(ctx context.Context, date string)
	// Create a new archive source to read archive files to process.
	Source(ctx context.Context, row Row) *archive.Source
	// File processes the given file content. File should only return ErrCorrupt
	// if the content is corrupt. If the file content cannot be processed for other
	// reasons, then return the original data with no error.
	File(h *tar.Header, b []byte) ([]byte, error)
	// Finish concludes an archive after all files have been processed.
	Finish(ctx context.Context, out *archive.Target) error
}

// Manager uses the Processor to act on every result returned by the Querier.
// Manager uses a type parameter for the query result and Processor type.
type Manager[Row any] struct {
	Jobs              jobs.Client
	Process           Processor[Row]
	OutBucket         string
	Client            query.Querier
	Query             string
	RetryQueryOnError bool
}

// ProcessDate processes all archives found on a given date.
func (r *Manager[Row]) ProcessDate(ctx context.Context, date string) error {
	// Initialize process with current date.
	r.Process.Init(ctx, date)

	// Collect BigQuery results quickly, and then process in a second loop below.
	// Processing results one row at a time (regardless of timeout) causes long
	// invocations of query.Run, and the query fails too frequently due to 503
	// or similar errors.
	results, err := r.runQuery(ctx, date)
	if err != nil {
		return err
	}

	// Processing all results can take several hours.
	log.Println(date, "Operating on archives:", len(results))
	for i := 0; i < len(results); i++ {
		t := time.Now()
		err = r.ProcessRow(ctx, date, results[i])
		if err != nil {
			log.Printf("Row processing failed %d due to err: %v", i, err)
			return err
		}
		repackerArchiveCompletionTime.Observe(time.Since(t).Seconds())
	}
	return nil
}

func (r *Manager[Row]) ProcessRow(ctx context.Context, date string, row Row) error {
	// Update job server that this date is still in progress.
	uctx, ucancel := context.WithTimeout(ctx, time.Minute)
	defer ucancel()
	r.Jobs.Update(uctx, date)
	repackerArchives.Inc()

	// Create a new source and output archive, which load the contents in memory.
	sctx, scancel := context.WithTimeout(ctx, 20*time.Minute)
	defer scancel()
	src := r.Process.Source(sctx, row)
	out := archive.NewTarget()

	var h *tar.Header
	var b []byte
	var err error
	corrupt := 0

	for {
		// Read each file from the source.
		h, b, err = src.NextFile()
		if err != nil {
			break
		}

		// Process the file.
		b, err = r.Process.File(h, b)
		if err == ErrCorrupt {
			corrupt++
			// Since file is corrupt, do not add to output archive.
			continue
		}

		// Add updated file content to new archive.
		n := archive.CopyHeader(h)
		n.Size = int64(len(b))
		out.AddFile(n, b)
	}

	// Verify that input and output file counts match.
	if src.Count-corrupt != out.Count {
		log.Printf("COUNTS DO NOT MACHT: corrupt:%d, in:%d, out:%d, %s",
			corrupt, src.Count, out.Count, src.Path.String())
		return errors.New("archive count mismatch")
	}

	// Abort for novel errors.
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		return err
	}

	// All files from the source were processed and added back to the output
	// archive. Finish archive processing, e.g. upload to alternate bucket.
	src.Close()
	return r.Process.Finish(ctx, out)
}

// runQuery runs the configured Reprocessor query for the given date then collects
// and returns all results.
func (r *Manager[Row]) runQuery(ctx context.Context, date string) ([]Row, error) {
	qctx, qcancel := context.WithTimeout(ctx, time.Hour)
	defer qcancel()

	var err error
	var results []Row
	t := time.Now()
	for trial := 0; trial < QueryRetries; trial++ {
		param := []bigquery.QueryParameter{
			{Name: "date", Value: date},
		}
		results, err = query.Run[Row](qctx, r.Client, r.Query, param)
		if err != nil {
			repackerQueryErrors.Inc()
			log.Println("Failed to run query (retrying after ~1m):", err)
			time.Sleep(time.Second * time.Duration(rand.Intn(MaxDelaySeconds)))
			continue
		}
		break
	}
	if err != nil {
		log.Println("query failed too many times:", err)
		return nil, fmt.Errorf("query failed too many times: %v", err)
	}
	repackerQueryCompletionTime.Observe(time.Since(t).Seconds())
	return results, nil
}
