package jobs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestClient_Lease(t *testing.T) {
	h := &Handler{}
	tests := []struct {
		name       string
		jobs       Jobs
		want       string
		handler    http.HandlerFunc
		clientErr  bool
		requestErr bool
		wantErr    bool
		expectErr  error
	}{
		{
			name: "success-lease",
			jobs: Jobs{
				Pending: []string{"2023-01-01"},
				Leased:  map[string]Job{},
			},
			handler: h.Lease,
			want:    "2023-01-01",
		},
		{
			name: "error-wait-leased-but-no-pending",
			jobs: Jobs{
				Pending: []string{},
				Leased: map[string]Job{
					"2023-01-01": Job{Date: "2023-01-01", Updated: time.Now()},
				},
			},
			handler:   h.Lease,
			wantErr:   true,
			expectErr: ErrWait,
		},
		{
			name: "error-empty-no-leased-no-pending",
			jobs: Jobs{
				Pending: []string{},
				Leased:  map[string]Job{},
			},
			handler:   h.Lease,
			wantErr:   true,
			expectErr: ErrEmpty,
		},
		{
			name: "error-status-not-okay",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			},
			wantErr: true,
		},
		{
			name:       "error-corrupt-server-url",
			handler:    func(w http.ResponseWriter, r *http.Request) {},
			requestErr: true,
			wantErr:    true,
		},
		{
			name:      "error-http-client-do",
			handler:   func(w http.ResponseWriter, r *http.Request) {},
			clientErr: true,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h.jobs = tt.jobs
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/lease", tt.handler)
			s := httptest.NewServer(mux)
			if tt.clientErr {
				// Shutdown server so client connection fails.
				s.Close()
			}
			u, _ := url.Parse(s.URL)
			if tt.requestErr {
				// Corrupt server URL so new request fails.
				u.Scheme = "-not-a-scheme-"
			}

			c := NewClient(u, http.DefaultClient)
			got, err := c.Lease(context.Background())

			// Verify results.
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.Lease() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.expectErr != nil && tt.expectErr != err {
				t.Errorf("Client.Lease() = %v, want %v", err, tt.expectErr)
			}
			if got != tt.want {
				t.Errorf("Client.Lease() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_Update_and_Complete(t *testing.T) {
	h := &Handler{}
	tests := []struct {
		name       string
		jobs       Jobs
		update     string
		requestErr bool
		wantErr    bool
	}{
		{
			name: "success-update-and-complete",
			jobs: Jobs{
				Pending: []string{},
				Leased: map[string]Job{
					"2023-01-01": Job{Date: "2023-01-01", Updated: time.Time{}},
				},
				Completed: map[string]Job{},
			},
			update: "2023-01-01",
		},
		{
			name:    "error-no-jobs-status-not-found",
			wantErr: true,
		},
		{
			name:       "error-corrupt-server-url",
			requestErr: true,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h.jobs = tt.jobs
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/update", h.Update)
			mux.HandleFunc("/v1/complete", h.Complete)
			s := httptest.NewServer(mux)
			u, _ := url.Parse(s.URL)
			if tt.requestErr {
				// Corrupt server URL so new request fails.
				u.Scheme = "-not-a-scheme-"
			}

			c := NewClient(u, http.DefaultClient)
			err := c.Update(context.Background(), tt.update)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.Update() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			err = c.Complete(context.Background(), tt.update)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.Complete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if _, ok := h.jobs.Completed[tt.update]; !ok {
				t.Errorf("Client.Complete() missing date: %s", tt.update)
			}
		})
	}
}
