package middlewares

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
)

func Logger() gin.HandlerFunc {
	return func(context *gin.Context) {
		start := time.Now()
		path := context.Request.URL.Path
		query := context.Request.URL.RawQuery

		context.Next()

		status := context.Writer.Status()

		slog.Info("request",
			slog.Int("status", status),
			slog.String("method", context.Request.Method),
			slog.String("path", path),
			slog.String("query", query),
			slog.String("ip", context.ClientIP()),
			slog.Duration("latency", time.Since(start)),
			slog.String("user-agent", context.Request.UserAgent()),
		)
	}
}
