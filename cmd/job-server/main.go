package main

import (
	"context"
	"flag"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/m-lab/archive-repacker/internal/jobs"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/rtx"
)

var (
	output  string
	addr    string
	timeout time.Duration

	mainCtx, mainCancel = context.WithCancel(context.Background())
)

var (
	leaseDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "jobserver_lease_duration_seconds",
			Help: "A histogram of request latencies to the job server handlers.",
		},
		[]string{"code"},
	)
	updateDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "jobserver_update_duration_seconds",
			Help: "A histogram of request latencies to the job server handlers.",
		},
		[]string{"code"},
	)
	completeDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "jobserver_complete_duration_seconds",
			Help: "A histogram of request latencies to the job server handlers.",
		},
		[]string{"code"},
	)
)

func init() {
	flag.StringVar(&addr, "addr", "", "listen on the given address")
	flag.StringVar(&output, "output", "", "")
	flag.DurationVar(&timeout, "timeout", 2*time.Hour, "timeout for leased jobs to be retried")
}

func main() {
	defer mainCancel()
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from env")
	s := prometheusx.MustServeMetrics()

	// Setup handler.
	h := &jobs.Handler{Output: output, Timeout: timeout}
	rtx.Must(h.Load(output), "failed to load saved jobs data")
	go h.Save(mainCtx, time.NewTicker(5*time.Second))

	// mux := http.NewServeMux()
	mux := s.Handler.(*http.ServeMux)
	mux.HandleFunc("/v1/init", h.Init)
	mux.HandleFunc("/v1/lease",
		promhttp.InstrumentHandlerDuration(leaseDuration, http.HandlerFunc(h.Lease)))
	mux.HandleFunc("/v1/update",
		promhttp.InstrumentHandlerDuration(updateDuration, http.HandlerFunc(h.Update)))
	mux.HandleFunc("/v1/complete",
		promhttp.InstrumentHandlerDuration(completeDuration, http.HandlerFunc(h.Complete)))

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
		// NOTE: set absolute read and write timeouts for server connections.
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	}
	rtx.Must(srv.ListenAndServe(), "failed to listen and serve")
}
