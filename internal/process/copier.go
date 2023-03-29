package process

import (
	"context"
	"fmt"
	"log"

	"github.com/m-lab/archive-repacker/internal/jobs"
)

// Renamer is an interface for types that support renaming.
type Renamer interface {
	List(ctx context.Context, date string) ([]string, error)
	Rename(ctx context.Context, url string) (string, error)
}

// Copier manages bulk rename operations.
type Copier struct {
	Jobs    *jobs.Client
	Process Renamer
}

// ProcessDate applies the renamer to the given date.
func (c *Copier) ProcessDate(ctx context.Context, date string) error {
	l, err := c.Process.List(ctx, date)
	if err != nil {
		return fmt.Errorf("failed to list %s: %w", date, err)
	}
	for i := range l {
		if i%1000 == 0 && c.Jobs != nil {
			// Update every 100 operations.
			log.Printf("Renamed %d objects for %s", i, date)
			c.Jobs.Update(ctx, date)
		}
		_, err := c.Process.Rename(ctx, l[i])
		if err != nil {
			log.Printf("Failed to rename %q: %v", l[i], err)
			return fmt.Errorf("failed rename of %q: %w", l[i], err)
		}
	}
	log.Printf("Renamed %d objects for %s", len(l), date)
	return nil
}
