package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/narad/narad/internal/storage"
)

type API struct {
	store *storage.Storage
}

func NewAPI(store *storage.Storage) *API {
	return &API{
		store: store,
	}
}

func (api *API) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (api *API) HandleGetLogs(w http.ResponseWriter, r *http.Request) {
	params := storage.QueryParams{
		Dims: make(map[string]string),
	}

	q := r.URL.Query()
	for k, v := range q {
		if len(k) > 4 && k[:4] == "dim." {
			params.Dims[k[4:]] = v[0]
		}
	}

	params.Service = q.Get("service")
	params.Level = q.Get("level")
	params.Code = q.Get("code")
	
	if from, err := strconv.ParseInt(q.Get("from"), 10, 64); err == nil {
		params.From = time.UnixMilli(from)
	}
	if to, err := strconv.ParseInt(q.Get("to"), 10, 64); err == nil {
		params.To = time.UnixMilli(to)
	}
	if limit, err := strconv.Atoi(q.Get("limit")); err == nil {
		params.Limit = limit
	}
	if tier, err := strconv.Atoi(q.Get("tier")); err == nil {
		params.Tier = tier
	}
	if conf, err := strconv.ParseFloat(q.Get("min_confidence"), 32); err == nil {
		params.MinConfidence = float32(conf)
	}

	start := time.Now()
	logs, err := api.store.QueryLogs(r.Context(), params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	queryMs := time.Since(start).Milliseconds()

	resp := map[string]interface{}{
		"logs":     logs,
		"total":    len(logs),
		"query_ms": queryMs,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (api *API) HandleTrace(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	value := chi.URLParam(r, "value")

	params := storage.QueryParams{
		Dims: map[string]string{key: value},
		Limit: 1000,
	}

	start := time.Now()
	logs, err := api.store.QueryLogs(r.Context(), params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for i, j := 0, len(logs)-1; i < j; i, j = i+1, j-1 {
		logs[i], logs[j] = logs[j], logs[i]
	}

	queryMs := time.Since(start).Milliseconds()
	resp := map[string]interface{}{
		"logs":     logs,
		"total":    len(logs),
		"query_ms": queryMs,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (api *API) HandleDimKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := api.store.ListDimKeys(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"keys": keys})
}

func (api *API) HandleDimValues(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key parameter", http.StatusBadRequest)
		return
	}
	vals, err := api.store.ListDimValues(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"values": vals})
}
