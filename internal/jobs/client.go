package jobs

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
)

var (
	// ErrEmpty is returned when the job-server has no more dates available.
	ErrEmpty = errors.New("no dates available")
	// ErrWait is returned when the job-server may have more dates in the future.
	ErrWait = errors.New("more dates may become available")
)

// Client manages requests to the job-server.
type Client struct {
	Server *url.URL
	*http.Client
}

// NewClient creates a new job client.
func NewClient(server *url.URL, client *http.Client) *Client {
	return &Client{
		Server: server,
		Client: client,
	}
}

func makeRequest(ctx context.Context, c *http.Client, l *url.URL) (*http.Response, error) {
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, l.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(r)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Lease attempts to claim a new job and returns the leased date if successful.
// Lease may also return ErrWait, if more jobs may be available in the future;
// clients should try again after a delay. Lease may also return ErrEmpty if no
// more jobs are available.
func (c *Client) Lease(ctx context.Context) (string, error) {
	l := *c.Server
	l.Path = "/v1/lease"
	resp, err := makeRequest(ctx, c.Client, &l)
	if err != nil {
		return "", err
	}
	switch resp.StatusCode {
	case http.StatusNoContent:
		return "", ErrEmpty
	case http.StatusTooEarly:
		return "", ErrWait
	case http.StatusOK:
		break
	default:
		return "", errors.New("bad status: " + resp.Status)
	}

	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	return string(b), err
}
