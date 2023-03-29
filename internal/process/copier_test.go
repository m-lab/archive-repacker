package process

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/m-lab/archive-repacker/internal/jobs"
	"github.com/m-lab/go/testingx"
)

type fakeRenamer struct {
	list      []string
	listErr   error
	renameErr error
	Count     int
}

func (f *fakeRenamer) List(ctx context.Context, date string) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.list, nil
}

func (f *fakeRenamer) Rename(ctx context.Context, url string) (string, error) {
	f.Count++
	return "", f.renameErr
}

func TestCopier_ProcessDate(t *testing.T) {
	tests := []struct {
		name      string
		date      string
		renamer   *fakeRenamer
		wantCount int
		wantErr   bool
	}{
		{
			name: "success",
			date: "2023-03-01",
			renamer: &fakeRenamer{
				list: []string{"a", "b", "c", "d", "e"},
			},
			wantCount: 5,
		},
		{
			name: "error-list-fails",
			date: "2023-03-01",
			renamer: &fakeRenamer{
				listErr: errors.New("fake-list-error"),
			},
			wantErr: true,
		},
		{
			name: "error-rename-fails",
			date: "2023-03-01",
			renamer: &fakeRenamer{
				list:      []string{"a", "b", "c", "d", "e"},
				renameErr: errors.New("fake-rename-error"),
			},
			wantCount: 1,
			wantErr:   true,
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/update", http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
	}))
	s := httptest.NewServer(mux)
	u, err := url.Parse(s.URL)
	testingx.Must(t, err, "failed to parse server url")
	js := jobs.NewClient(u, http.DefaultClient)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Copier{
				Jobs:    js,
				Process: tt.renamer,
			}
			ctx := context.Background()
			if err := c.ProcessDate(ctx, tt.date); (err != nil) != tt.wantErr {
				t.Errorf("Copier.ProcessDate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.renamer.Count != tt.wantCount {
				t.Errorf("Copier.ProcessDate() wrong count = %d, want %d", tt.renamer.Count, tt.wantCount)
			}
		})
	}
}
