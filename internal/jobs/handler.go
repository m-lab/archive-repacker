package jobs

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/m-lab/go/logx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/go/timex"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	jobStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "jobserver_status_count",
			Help: "Current counts of job statuses.",
		},
		[]string{"status"},
	)
)

// Job represents a date task.
type Job struct {
	Date    string
	Updated time.Time
}

// Sequence contains a list of strings.
type Sequence []string

// LPop removes the first element of Sequence and returns it.
func (s *Sequence) LPop() string {
	old := *s
	if len(old) == 0 {
		return ""
	}
	x := old[0]
	*s = old[1:]
	return x
}

// Jobs represents a collection of pending, currently leased, and completed
// Jobs. A given date will only be in one of Pending, Leased, or Completed. A
// Jobs instance can be Marshalled to JSON.
type Jobs struct {
	// Pending is the list of dates to be processed.
	Pending Sequence
	// Leased is the set of currently leased jobs.
	Leased map[string]Job
	// Completed is the set of all completed jobs.
	Completed map[string]Job
}

// Handler is an HTTP handler for a jobs server.
type Handler struct {
	// JobsStateFile is the file name where the handler will periodically write the job state.
	JobsStateFile string
	// Timeout is the maximum time a leased job should remain leased without
	// updates before being returned to the pending queue.
	Timeout time.Duration

	lock sync.Mutex
	jobs Jobs
}

// Save periodically seriaizes the collection of Jobs to Handler.JobsStateFile, and
// periodically checks for leased jobs that have been processing longer than
// Handler.Timeout and returns them to Pending.
func (h *Handler) Save(ctx context.Context, t *time.Ticker) {
	for {
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
		h.lock.Lock()
		h.checkLeaseTimeout()
		jobStatus.WithLabelValues("pending").Set(float64(len(h.jobs.Pending)))
		jobStatus.WithLabelValues("leased").Set(float64(len(h.jobs.Leased)))
		jobStatus.WithLabelValues("completed").Set(float64(len(h.jobs.Completed)))
		b, err := json.MarshalIndent(&h.jobs, "", " ")
		rtx.Must(err, "failed to marshal jobs")
		err = ioutil.WriteFile(h.JobsStateFile, b, 0o666)
		if err != nil {
			log.Printf("failed to write %s: %v", h.JobsStateFile, err)
		}
		h.lock.Unlock()
	}
}

// Load reads the content of the named file to initialize the Handler jobs.
func (h *Handler) Load(file string) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	_, err := os.Stat(file)
	if err != nil {
		// This is not an error; there is just no file to load.
		return nil
	}
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, &h.jobs)
	if err != nil {
		// This is also not fatal.
		log.Println("Failed to unmarshal jobs, file may be corrupt:", err)
	}
	h.checkLeaseTimeout()
	return nil
}

// checkLeaseTimeout checks for leased jobs that are older than the handler timeout and returns
// them to pending, if so.
func (h *Handler) checkLeaseTimeout() {
	// Look for leased jobs that should be retried.
	for date, j := range h.jobs.Leased {
		if time.Since(j.Updated) > h.Timeout {
			h.jobs.Pending = append(h.jobs.Pending, date)
			delete(h.jobs.Leased, date)
		}
	}
}

// Lease takes a job from the pending queue. If no jobs are currently pending,
// but still leased, the Lease handler will return "Status Too Early" to signal
// a client to retry after waiting.  If no jobs are currently pending, the Lease
// handler will return "Status No Content" to signal a client to stop attempting
// leases.
func (h *Handler) Lease(w http.ResponseWriter, r *http.Request) {
	h.lock.Lock()
	defer h.lock.Unlock()
	date := h.jobs.Pending.LPop()
	if date == "" {
		if len(h.jobs.Leased) > 0 {
			w.WriteHeader(http.StatusTooEarly)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
		return
	}
	j := Job{
		Date:    date,
		Updated: time.Now(),
	}
	logx.Debug.Println("Lease date:", j.Date)
	h.jobs.Leased[j.Date] = j
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(j.Date))
}

// Update sets the "Updated" timestamp of a leased job to the current time.
func (h *Handler) Update(w http.ResponseWriter, r *http.Request) {
	h.lock.Lock()
	defer h.lock.Unlock()
	date := r.URL.Query().Get("date")
	if date == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	j, ok := h.jobs.Leased[date]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	logx.Debug.Println("Updating job:", date)
	j.Updated = time.Now()
	h.jobs.Leased[date] = j
	w.WriteHeader(http.StatusOK)
}

// Complete moves a leased job to the set of completed jobs. If the job is not
// leased, Complete returns an error.
func (h *Handler) Complete(w http.ResponseWriter, r *http.Request) {
	h.lock.Lock()
	defer h.lock.Unlock()
	date := r.URL.Query().Get("date")
	if date == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	logx.Debug.Println("Complete date:", date)
	j, ok := h.jobs.Leased[date]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	j.Updated = time.Now()
	h.jobs.Completed[date] = j
	delete(h.jobs.Leased, date)
	w.WriteHeader(http.StatusOK)
}

// Init populates the set of Pending jobs with all dates between the given
// "start" and "end" (exclusive) date parameters. These dates should use the
// format YYYY-MM-DD.
func (h *Handler) Init(w http.ResponseWriter, r *http.Request) {
	h.lock.Lock()
	defer h.lock.Unlock()
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	if start == "" || end == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s, err1 := time.Parse(timex.YYYYMMDDWithDash, start)
	e, err2 := time.Parse(timex.YYYYMMDDWithDash, end)
	if err1 != nil || err2 != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	h.jobs = Jobs{
		Pending:   Sequence{},
		Leased:    map[string]Job{},
		Completed: map[string]Job{},
	}
	for t := s; t.Before(e); t = t.AddDate(0, 0, 1) {
		h.jobs.Pending = append(h.jobs.Pending, t.Format(timex.YYYYMMDDWithDash))
	}
}
