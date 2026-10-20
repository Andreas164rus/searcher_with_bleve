package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	httpSwagger "github.com/swaggo/http-swagger/v2"

	"local-search/internal/config"
	"local-search/internal/service"
)

// Server держит маршруты HTTP и ссылку на сервис.
type Server struct {
	cfg     config.Config
	service *service.Service
	mux     *http.ServeMux
}

// New собираем mux и вешаем ручки.
func New(cfg config.Config, svc *service.Service) *Server {
	s := &Server{
		cfg:     cfg,
		service: svc,
		mux:     http.NewServeMux(),
	}
	s.routes()
	return s
}

// Handler отдаём mux стандартному http.Server.
func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /search", s.handleSearch)
	s.mux.HandleFunc("GET /stats", s.handleStats)
	s.mux.HandleFunc("POST /index/rebuild", s.handleRebuild)
	s.mux.HandleFunc("GET /openapi.yaml", s.handleOpenAPI)
	// Swagger UI читает наш ручной openapi.yaml.
	s.mux.HandleFunc("GET /docs/", httpSwagger.Handler(httpSwagger.URL("/openapi.yaml")))
}

// handleSearch ищем по q и limit, затем отдаём JSON.
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeError(w, http.StatusBadRequest, "q обязательный")
		return
	}

	limit, err := s.parseLimit(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	results, err := s.service.Search(query, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, results)
}

// handleStats отдаём текущее состояние индекса.
func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.service.Stats())
}

// handleRebuild полностью пересобираем индекс. Поиск на время rebuild ждёт lock.
func (s *Server) handleRebuild(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	if err := s.service.Rebuild(ctx); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "rebuilt"})
}

// handleOpenAPI отдаём файл спецификации для Swagger UI.
func (s *Server) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	http.ServeFile(w, r, "openapi.yaml")
}

// parseLimit читаем ?limit=; пустой — defaultLimit, слишком большой — ошибка.
func (s *Server) parseLimit(r *http.Request) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return s.cfg.DefaultLimit, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return 0, errors.New("limit must be a positive integer")
	}
	if limit > s.cfg.MaxLimit {
		return 0, errors.New("limit is greater than configured maxLimit")
	}
	return limit, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
