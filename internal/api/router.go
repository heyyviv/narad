package api

import (
	"github.com/go-chi/chi/v5"
	"github.com/narad/narad/internal/storage"
)

func NewRouter(store *storage.Storage) *chi.Mux {
	r := chi.NewRouter()
	
	r.Use(DefaultMiddleware()...)
	
	api := NewAPI(store)

	r.Get("/v1/health", api.HandleHealth)

	r.Get("/v1/logs", api.HandleGetLogs)
	r.Get("/v1/trace/{key}/{value}", api.HandleTrace)
	r.Get("/v1/dims/keys", api.HandleDimKeys)
	r.Get("/v1/dims/values", api.HandleDimValues)

	return r
}
