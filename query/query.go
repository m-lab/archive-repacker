package query

import (
	"context"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/go/logx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/api/iterator"
)

var (
	// RowDelay pauses query result collection on every row.
	RowDelay = time.Duration(0)
)

var (
	repackerQueryRows = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "repacker_query_results_total",
			Help: "The number of query rows processed",
		},
	)
	repackerQueryErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "repacker_query_read_errors_total",
			Help: "The number of query errors",
		},
	)
)

// Querier interface are types supporting a Query operation.
type Querier interface {
	Query(s string) bqiface.Query
}

// Run collects all rows of the parameterized type from a job based on the given
// query and any query parameters.
func Run[Row any](ctx context.Context, c Querier, query string, params map[string]interface{}) ([]Row, error) {
	t := time.Now()

	var queryParams []bigquery.QueryParameter
	for key, value := range params {
		queryParams = append(queryParams, bigquery.QueryParameter{Name: key, Value: value})
	}

	q := c.Query(query)
	q.SetQueryConfig(bqiface.QueryConfig{
		QueryConfig: bigquery.QueryConfig{
			Q:          query,
			Priority:   bigquery.BatchPriority,
			Parameters: queryParams,
		}})
	it, err := q.Read(ctx)
	if err != nil {
		repackerQueryErrors.Inc()
		return nil, err
	}
	results := make([]Row, 0, 1000)
	logx.Debug.Printf("context %p: start query rows: %d, %s", ctx, it.TotalRows(), time.Since(t))
	for {
		var row Row
		err = it.Next(&row)
		if err != nil {
			break
		}
		results = append(results, row)
		time.Sleep(RowDelay)
		repackerQueryRows.Inc()
	}
	if err != iterator.Done {
		repackerQueryErrors.Inc()
		return nil, err
	}
	log.Printf("context %p: complete query rows: %d, %s", ctx, it.TotalRows(), time.Since(t))
	return results, nil
}
