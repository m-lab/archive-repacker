package jobs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/m-lab/go/testingx"
)

func TestHandler_Init(t *testing.T) {
	tests := []struct {
		name   string
		start  string
		end    string
		status int
		want   Jobs
	}{
		{
			name:   "success",
			start:  "2023-01-01",
			end:    "2023-01-03",
			status: http.StatusOK,
			want: Jobs{
				Pending: []string{"2023-01-01", "2023-01-02"},
			},
		},
		{
			name:   "success-dates-out-of-order",
			start:  "2023-02-01",
			end:    "2023-01-01",
			status: http.StatusOK,
			want: Jobs{
				Pending: []string{},
			},
		},
		{
			name:   "bad-request-dates-empty",
			start:  "",
			end:    "",
			status: http.StatusBadRequest,
		},
		{
			name:   "bad-request-dates-dont-parse",
			start:  "bad-date1",
			end:    "bad-date2",
			status: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Handler{}
			ts := httptest.NewServer(http.HandlerFunc(h.Init))
			param := url.Values{}
			param.Add("start", tt.start)
			param.Add("end", tt.end)

			resp, err := http.Get(ts.URL + "?" + param.Encode())
			testingx.Must(t, err, "failed to get request to %s", ts.URL)

			if resp.StatusCode != tt.status {
				t.Errorf("Handler.Init() status = %d, want %d", resp.StatusCode, tt.status)
			}
			if !cmp.Equal(h.jobs.Pending, tt.want.Pending) {
				t.Errorf("Handler.Init() = %#v, want %#v", h.jobs, tt.want)
			}
		})
	}
}

func TestHandler_Lease(t *testing.T) {
	tests := []struct {
		name   string
		jobs   Jobs
		status int
		want   string
	}{
		{
			name: "success",
			jobs: Jobs{
				Pending: []string{"2023-01-01"},
				Leased:  map[string]Job{},
			},
			status: http.StatusOK,
			want:   "2023-01-01",
		},
		{
			name: "error-no-pending-with-leased",
			jobs: Jobs{
				Pending: []string{},
				Leased: map[string]Job{
					"2023-01-01": Job{Date: "2023-01-01"},
				},
			},
			status: http.StatusTooEarly,
		},
		{
			name: "error-no-pending-without-leased",
			jobs: Jobs{
				Pending: []string{},
				Leased:  map[string]Job{},
			},
			status: http.StatusNoContent,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Handler{
				jobs: tt.jobs,
			}
			ts := httptest.NewServer(http.HandlerFunc(h.Lease))

			resp, err := http.Get(ts.URL)
			testingx.Must(t, err, "failed to get request to %s", ts.URL)

			if resp.StatusCode != tt.status {
				t.Errorf("Handler.Lease() status = %d, want %d", resp.StatusCode, tt.status)
			}
			if _, ok := h.jobs.Leased[tt.want]; tt.want != "" && !ok {
				t.Errorf("Handler.Lease() = %#v, want %#v", h.jobs.Leased, tt.want)
			}
		})
	}
}

func TestHandler_Update(t *testing.T) {
	tests := []struct {
		name   string
		date   string
		jobs   Jobs
		status int
	}{
		{
			name: "success",
			date: "2023-01-01",
			jobs: Jobs{
				Leased: map[string]Job{
					"2023-01-01": Job{
						Date:    "2023-01-01",
						Updated: time.Time{},
					},
				},
			},
			status: http.StatusOK,
		},
		{
			name:   "bad-request-dates-empty",
			date:   "",
			status: http.StatusBadRequest,
		},
		{
			name: "bad-request-dates-not-found",
			date: "2020-02-02",
			jobs: Jobs{
				Leased: map[string]Job{
					"2023-01-01": Job{
						Date:    "2023-01-01",
						Updated: time.Time{},
					},
				},
			},
			status: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Handler{
				jobs: tt.jobs,
			}
			ts := httptest.NewServer(http.HandlerFunc(h.Update))
			param := url.Values{}
			param.Add("date", tt.date)

			resp, err := http.Get(ts.URL + "?" + param.Encode())
			testingx.Must(t, err, "failed to get request to %s", ts.URL)

			if resp.StatusCode != tt.status {
				t.Errorf("Handler.Update() status = %d, want %d", resp.StatusCode, tt.status)
			}
			j, ok := h.jobs.Leased[tt.date]
			if ok && j.Updated.Equal(time.Time{}) {
				t.Errorf("Handler.Update() = %#v, want %#v", j, time.Now())
			}
		})
	}
}

func TestHandler_Complete(t *testing.T) {
	tests := []struct {
		name   string
		date   string
		jobs   Jobs
		status int
	}{
		{
			name: "success",
			date: "2023-01-01",
			jobs: Jobs{
				Leased: map[string]Job{
					"2023-01-01": Job{
						Date:    "2023-01-01",
						Updated: time.Time{},
					},
				},
				Completed: map[string]Job{},
			},
			status: http.StatusOK,
		},
		{
			name:   "bad-request-dates-empty",
			date:   "",
			status: http.StatusBadRequest,
		},
		{
			name: "bad-request-dates-not-found",
			date: "2020-02-02",
			jobs: Jobs{
				Leased: map[string]Job{
					"2023-01-01": Job{
						Date:    "2023-01-01",
						Updated: time.Time{},
					},
				},
			},
			status: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Handler{
				jobs: tt.jobs,
			}
			ts := httptest.NewServer(http.HandlerFunc(h.Complete))
			param := url.Values{}
			param.Add("date", tt.date)

			resp, err := http.Get(ts.URL + "?" + param.Encode())
			testingx.Must(t, err, "failed to get request to %s", ts.URL)

			if resp.StatusCode != tt.status {
				t.Errorf("Handler.Complete() status = %d, want %d", resp.StatusCode, tt.status)
			}
			j, ok := h.jobs.Completed[tt.date]
			if ok && j.Updated.Equal(time.Time{}) {
				t.Errorf("Handler.Complete() = %#v, want %#v", j, time.Now())
			}
		})
	}
}

func TestHandler_SaveAndLoad(t *testing.T) {
	tests := []struct {
		name string
		jobs Jobs
	}{
		{
			name: "success",
			jobs: Jobs{
				Pending: []string{},
				Leased: map[string]Job{
					"2023-01-01": Job{
						Date:    "2023-01-01",
						Updated: time.Time{},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := path.Join(t.TempDir(), "out.json")
			h := &Handler{
				Output: output,
				jobs:   tt.jobs,
			}
			err := h.Load(output) // before file exists.
			testingx.Must(t, err, "failed to load file")
			ctx, cancel := context.WithCancel(context.Background())
			tick := time.NewTicker(time.Millisecond)
			go func() {
				time.Sleep(time.Second)
				cancel()
			}()
			h.Save(ctx, tick)
			_, err = os.Stat(output)
			if err != nil {
				t.Errorf("Handler.Save() missing output")
			}
			err = h.Load(output) // after file exists.
			testingx.Must(t, err, "failed to load file")
		})
	}
}
