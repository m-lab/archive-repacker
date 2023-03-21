package archive

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"log"
	"math/rand"
	"time"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"

	"cloud.google.com/go/bigquery"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/m-lab/archive-repacker/query"
)

var (
	// MaxDelaySeconds is the maximum number of seconds to randomly wait in
	// response to BigQuery errors.
	MaxDelaySeconds = 60
	ProcessRetries  = 10
	QueryRetries    = 2

	// ErrCorrupt may be returned by a processor implementation if the file
	// content should be considered corrupt.
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

// Processor is a generic interface for processing BigQuery result rows. The
// interface expects all jobs to be batched per date, and process every file of
// every archive.
type Processor[Row any] interface {
	// Init sets up the processor for processing the given date, e.g. downloading daily databases.
	Init(ctx context.Context, date string)
	// Create a new archive source to read archive files to process.
	Source(ctx context.Context, row Row) *Reader
	// File processes the given file content.
	File(h *tar.Header, b []byte) ([]byte, error)
	// Finish concludes an archive after all files have been processed.
	Finish(ctx context.Context, out *Writer) error
}

// Reprocessor is a generic type that can enumerate and process all archives
// returned in a BigQuery result Row and processed by a Processor that can act
// on the same row type.
type Reprocessor[Row any] struct {
	// Jobs              jobs.Client
	Process           Processor[Row]
	OutBucket         string
	Client            *bigquery.Client
	Query             string
	RetryQueryOnError bool
}

// ProcessDate processes all archives found on a given date.
func (r *Reprocessor[Row]) ProcessDate(ctx context.Context, date string) error {
	r.Process.Init(ctx, date)

	// Here we collect results quickly, and then process in a second loop below.
	// Processing results one row at a time (regardless of timeout) causes long
	// invocations of query.Run, and the query fails too frequently due to 503
	// or similar errors.
	qctx, qcancel := context.WithTimeout(ctx, time.Hour)
	defer qcancel()

	var err error
	var results []Row
	t := time.Now()
	err = retry(QueryRetries, func() error {
		param := []bigquery.QueryParameter{
			{Name: "date", Value: date},
		}
		results, err = query.Run[Row](qctx, bqiface.AdaptClient(r.Client), r.Query, param)
		if err != nil {
			// Retry
			repackerQueryErrors.Inc()
			log.Println("Failed to run query (retrying after ~1m):", err)
			time.Sleep(time.Second * time.Duration(rand.Intn(MaxDelaySeconds)))
			return err
		}
		return nil
	})
	if err != nil {
		log.Println("query failed too many times:", err)
		return err
	}
	repackerQueryCompletionTime.Observe(time.Since(t).Seconds())

	// Processing all results can take several hours or longer.
	pctx, pcancel := context.WithCancel(ctx)
	defer pcancel()

	log.Println(date, "Operating on archives:", len(results))
	for i := 0; i < len(results); i++ {
		t := time.Now()
		err = retry(ProcessRetries, func() error {
			err = r.ProcessRow(pctx, date, results[i])
			if err != nil {
				log.Printf("Retrying job %d due to err: %v", i, err)
				return err
			}
			return nil
		})
		if err != nil {
			log.Printf("Job %d failed too many times: %v", i, err)
			return err
		}
		repackerArchiveCompletionTime.Observe(time.Since(t).Seconds())
	}
	return nil
}

func (r *Reprocessor[Row]) ProcessRow(ctx context.Context, date string, row Row) error {
	// Update job server that this date is still in progress.
	//uctx, ucancel := context.WithTimeout(ctx, time.Minute)
	//defer ucancel()
	//r.Jobs.Update(uctx, date)
	repackerArchives.Inc()

	// Create a new source and output archive, which may download the data in memory.
	src := r.Process.Source(ctx, row)
	out := NewWriter()

	var err error
	var h *tar.Header
	var b []byte
	corrupt := 0

	for {
		// Read all files from the source.
		h, b, err = src.NextFile()
		if err != nil {
			break
		}

		// Process this file. Note: File should only return one error; even if
		// it cannot operate on the file data, it should return the original
		// data rather than errors.
		b, err = r.Process.File(h, b)
		if err == ErrCorrupt {
			// Since file is corrupt, do not add to output archive.
			corrupt++
			continue
		}

		// Add updated annotation to new archive.
		n := CopyHeader(h)
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
