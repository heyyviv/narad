package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

func DefaultMiddleware() []func(http.Handler) http.Handler {
	return []func(http.Handler) http.Handler{
		middleware.RequestID,
		middleware.RealIP,
		middleware.Logger,
		middleware.Recoverer,
		middleware.Timeout(60 * time.Second),
	}
}
