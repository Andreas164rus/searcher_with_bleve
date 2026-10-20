package service

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"

	"local-search/internal/config"
	"local-search/internal/document"
	"local-search/internal/scanner"
	"local-search/internal/search"
)

// Service связывает сканер и индекс. Через него работают API и watcher.
type Service struct {
	cfg     config.Config
	scanner *scanner.Scanner

	mu    sync.RWMutex // rebuild берёт write-lock, поиск и upsert — read-lock
	index *search.Index
}

// New создаём сканер и пересоздаём индекс на диске.
func New(cfg config.Config) (*Service, error) {
	scn, err := scanner.New(cfg.RootDir, cfg.SupportedExts, cfg.IgnoredDirs, cfg.MaxFileSizeBytes)
	if err != nil {
		return nil, err
	}
	// Recreate: при старте строим индекс заново из текущих файлов.
	idx, err := search.Recreate(cfg.IndexPath)
	if err != nil {
		return nil, err
	}
	return &Service{
		cfg:     cfg,
		scanner: scn,
		index:   idx,
	}, nil
}

// Close закрываем Bleve. Write-lock, чтобы никто не искал в закрывающемся индексе.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.index.Close()
}

// BuildInitialIndex обходим папку и кладём каждый документ в индекс.
func (s *Service) BuildInitialIndex(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.scanner.Scan(func(doc document.Document) error {
		if err := ctx.Err(); err != nil {
			// Если нажали Ctrl+C, останавливаем обход.
			return err
		}
		return s.index.IndexDocument(doc)
	})
}

// Search ищем по индексу, файлы заново не читаем.
func (s *Service) Search(query string, limit int) ([]document.Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.index.Search(query, limit)
}

// Stats отдаём количество документов, размер и время обновления.
func (s *Service) Stats() search.Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.index.Stats()
}

// Rebuild полностью пересобираем индекс. Поиск в это время ждёт write-lock.
func (s *Service) Rebuild(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.index.Close(); err != nil {
		return err
	}
	idx, err := search.Recreate(s.cfg.IndexPath)
	if err != nil {
		return err
	}
	if err := s.scanner.Scan(func(doc document.Document) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return idx.IndexDocument(doc)
	}); err != nil {
		_ = idx.Close()
		return err
	}
	s.index = idx
	return nil
}

// UpsertPath читаем один файл и обновляем только его документ в индексе.
func (s *Service) UpsertPath(path string) error {
	if s.scanner.ShouldIgnorePath(path) || !s.scanner.IsSupported(path) {
		// Ignored-папку или чужое расширение пропускаем.
		return nil
	}
	doc, ok, err := s.scanner.ReadDocument(path)
	if err != nil {
		return err
	}
	if !ok {
		// Файл не подходит — не трогаем индекс.
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.IndexDocument(doc)
}

// DeletePath удаляем документ по относительному пути.
func (s *Service) DeletePath(path string) error {
	id, err := filepath.Rel(s.scanner.RootDir(), path)
	if err != nil {
		return err
	}
	id = filepath.ToSlash(id)

	s.mu.RLock() // не меняй указатель s.index
	defer s.mu.RUnlock()
	if err := s.index.DeleteDocument(id); err != nil {
		slog.Warn("delete from index failed", "path", path, "error", err)
		return err
	}
	return nil
}

func (s *Service) RootDir() string {
	return s.scanner.RootDir()
}

func (s *Service) IsSupported(path string) bool {
	return s.scanner.IsSupported(path)
}

func (s *Service) ShouldIgnorePath(path string) bool {
	return s.scanner.ShouldIgnorePath(path)
}
