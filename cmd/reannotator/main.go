package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/api/option"

	"github.com/m-lab/archive-repacker/internal/annotation"
	"github.com/m-lab/archive-repacker/internal/jobs"
	"github.com/m-lab/archive-repacker/internal/process"
	"github.com/m-lab/archive-repacker/query"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/logx"
	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/go/timex"
)

var (
	queryset    = flagx.File{}
	routeviewv4 = flagx.URL{}
	routeviewv6 = flagx.URL{}
	asnameurl   = flagx.URL{}
	jobservice  = flagx.URL{}
	bqDelay     = time.Millisecond

	project   string
	outBucket string

	mainCtx, mainCancel = context.WithCancel(context.Background())

	repackerDatesStarted = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "repacker_dates_started_total",
			Help: "The number of dates started",
		},
	)
	repackerDate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "repacker_date",
			Help: "Most recent date processed.",
		},
		[]string{"state"},
	)
	repackerDateCompletionTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "repacker_date_completion_time",
			Help: "Histogram of job completion times",
			Buckets: []float64{
				10, 21.5, 46.4,
				100, 215, 464,
				1000, 2150, 4640,
				10000, 21500, 46400,
			},
		},
	)
)

func init() {
	flag.Var(&queryset, "query", "filename with query to generate repacker archive list and options")
	flag.Var(&routeviewv4, "routeview-v4.url", "The URL for the RouteViewIPv4 file containing ASN metadata. gs:// and file:// schemes accepted.")
	flag.Var(&routeviewv6, "routeview-v6.url", "The URL for the RouteViewIPv6 file containing ASN metadata. gs:// and file:// schemes accepted.")
	flag.Var(&asnameurl, "asname.url", "The URL for the ASName CSV file containing a mapping of AS numbers to AS names provided by IPInfo.io")
	flag.Var(&jobservice, "jobservice.url", "The URL for the job service providing dates to process")
	flag.StringVar(&project, "project", "", "GCP project name.")
	flag.StringVar(&outBucket, "output", "soltesz-tfstate-0", "")
	flag.DurationVar(&bqDelay, "bq-delay", time.Millisecond, "how long to wait between bq row requests")

	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from env")
	prometheusx.MustServeMetrics()

	defer mainCancel()
	if project == "" {
		log.Fatal("-project is a required flag")
	}
	// TODO: add http client with timeouts.
	c := jobs.NewClient(jobservice.URL, http.DefaultClient)
	sclient, err := storage.NewClient(mainCtx, option.WithScopes(storage.ScopeReadWrite))
	rtx.Must(err, "failed to create new read/write storage client")
	p := annotation.NewProcessor(
		sclient,
		outBucket,
		routeviewv4.URL,
		routeviewv6.URL,
		asnameurl.URL)

	// Many clients running a query can generate quota exceeded errors.
	// e.g. googleapi: Error 403: Quota exceeded: Your project exceeded
	//      quota for tabledata.list bytes per second per project.
	//
	// The aggregate quota is ~500Mbps.
	// * For 64 clients, that's < 8Mbps each.
	// * If each row is ~1k a 1ms pause is about 1Mbps
	// * 500us will add ~0.5s per 1000 results.
	// * ~128Mbps aggregate for 64 clients.
	query.RowDelay = time.Millisecond / 2

	// Run query to get list of archives to operate on.
	bqclient, err := bigquery.NewClient(mainCtx, project)
	rtx.Must(err, "failed to create bigquery client")
	err = bqclient.EnableStorageReadClient(mainCtx)
	rtx.Must(err, "failed to enable storage read client")

	r := process.Manager[annotation.Result]{
		Jobs:        c,
		Process:     p,
		OutBucket:   outBucket,
		QueryClient: bqiface.AdaptClient(bqclient),
		Query:       queryset.Content(),
	}
	logx.Debug.Println(queryset.Content())
	for {
		date, err := c.Lease(mainCtx)
		switch err {
		case jobs.ErrEmpty:
			log.Println("Work queue empty; exiting")
			return
		case jobs.ErrWait:
			log.Println("Waiting for job; sleeping 1m")
			time.Sleep(time.Minute)
			continue
		}
		rtx.Must(err, "failed to request job lease")

		t := time.Now()
		log.Println("Processing:", date)
		d, err := time.Parse(timex.YYYYMMDDWithDash, date)
		rtx.Must(err, "failed to parse job date")
		repackerDate.WithLabelValues("starting").Set(float64(d.Unix()))
		repackerDatesStarted.Inc()

		r.ProcessDate(mainCtx, date)

		err = c.Complete(mainCtx, date)
		rtx.Must(err, "failed to run complete with job service")
		log.Println("Completed date:", date, time.Since(t))
		repackerDate.WithLabelValues("completed").Set(float64(d.Unix()))
		repackerDateCompletionTime.Observe(time.Since(t).Seconds())
	}
}
