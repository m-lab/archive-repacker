package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/api/option"

	"github.com/m-lab/archive-repacker/internal/annotation"
	"github.com/m-lab/archive-repacker/internal/jobs"
	"github.com/m-lab/archive-repacker/internal/process"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/go/timex"
)

var (
	jobservice   = flagx.URL{}
	outBucket    string
	experiment   string
	fromDatatype string
	newDatatype  string
	oneDate      string

	mainCtx, mainCancel = context.WithCancel(context.Background())
)
var (
	renamerDatesStarted = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "renamer_dates_started_total",
			Help: "The number of dates started",
		},
	)
	renamerDate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "renamer_date",
			Help: "Most recent date processed.",
		},
		[]string{"state"},
	)
	renamerDateCompletionTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "renamer_date_completion_time",
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
	flag.Var(&jobservice, "jobservice.url", "The URL for the job service providing dates to process.")
	flag.StringVar(&outBucket, "output", "annotation2-output-mlab-sandbox", "Write generated archives to this GCS Bucket.")
	flag.StringVar(&experiment, "experiment", "ndt", "Name of experiment.")
	flag.StringVar(&fromDatatype, "from-datatype", "annotation", "Name of original datatype to read in.")
	flag.StringVar(&newDatatype, "new-datatype", "annotation2", "Name of new datatype to write out.")
	flag.StringVar(&oneDate, "date", "", "If provided, process only this single date.")
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	defer mainCancel()
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from env")

	prometheusx.MustServeMetrics()
	sclient, err := storage.NewClient(mainCtx, option.WithScopes(storage.ScopeReadWrite))
	rtx.Must(err, "failed to create new read/write storage client")
	p := annotation.NewRenamer(sclient, outBucket, experiment, fromDatatype, newDatatype)
	r := &process.Copier{
		Process: p,
	}
	if oneDate != "" {
		processDate(r, oneDate)
		return
	}
	r.Jobs = jobs.NewClient(jobservice.URL, http.DefaultClient)

	// Process jobs indefinitely.
	for {
		date, err := r.Jobs.Lease(mainCtx)
		switch err {
		case jobs.ErrEmpty:
			log.Println("Work queue empty; exiting")
			return
		case jobs.ErrWait:
			log.Println("Work queue pending; waiting for 1m")
			time.Sleep(time.Minute)
			continue
		}
		rtx.Must(err, "failed to request job lease")

		processDate(r, date)

		err = r.Jobs.Complete(mainCtx, date)
		rtx.Must(err, "failed to complete job with job service: %s", date)
	}
}

func processDate(r *process.Copier, date string) {

	d, err := time.Parse(timex.YYYYMMDDWithDash, date)
	rtx.Must(err, "failed to parse job date")

	t := time.Now()
	log.Println("Starting date:", date)
	renamerDate.WithLabelValues("starting").Set(float64(d.Unix()))
	renamerDatesStarted.Inc()

	err = r.ProcessDate(mainCtx, date)
	rtx.Must(err, "failed to run job to completion: %s", date)

	log.Println("Completed date:", date, time.Since(t))
	renamerDate.WithLabelValues("completed").Set(float64(d.Unix()))
	renamerDateCompletionTime.Observe(time.Since(t).Seconds())
}
