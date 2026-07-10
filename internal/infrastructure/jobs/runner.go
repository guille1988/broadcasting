package jobs

import (
	"context"
	"log/slog"
	"time"
)

/*
Run invokes execute every interval until ctx is canceled. Must be called in
its own goroutine. Execution errors are logged, not fatal: a failed run is
retried on the next tick.
*/
func Run(ctx context.Context, name string, interval time.Duration, execute func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	slog.Info("job started", "job", name, "interval", interval.String())

	for {
		select {
		case <-ctx.Done():
			slog.Info("job stopped", "job", name)
			return

		case <-ticker.C:
			slog.Debug("job tick", "job", name)

			if err := execute(ctx); err != nil {
				slog.Error("job execution failed", "job", name, "error", err)
			}
		}
	}
}
