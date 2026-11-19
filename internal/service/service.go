package service

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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
// op: "and" — все слова, "or" — хотя бы одно.
func (s *Service) Search(query string, limit int, op string) ([]document.Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.index.Search(query, limit, op)
}

// UpdateCount сколько раз индекс принимал документ.
func (s *Service) UpdateCount() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.UpdateCount()
}

// Stats отдаём количество документов, размер и время обновления.
func (s *Service) Stats() search.Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.index.Stats()
}

// Rebuild полностью пересобираем индекс. Поиск в это время ждёт write-lock.
// Новый индекс сначала собираем рядом: закрытый Bleve не всегда сразу отпускает свою папку.
func (s *Service) Rebuild(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tmpPath := s.cfg.IndexPath + ".rebuild"
	if err := search.RemoveIndexDir(tmpPath); err != nil {
		return err
	}

	idx, err := search.New(tmpPath)
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
		_ = search.RemoveIndexDir(tmpPath)
		return err
	}
	// Закрываем временный индекс, чтобы его папку можно было перенести.
	if err := idx.Close(); err != nil {
		_ = search.RemoveIndexDir(tmpPath)
		return err
	}

	if err := s.index.Close(); err != nil {
		return s.keepOldIndex(tmpPath, fmt.Errorf("close index: %w", err))
	}
	if err := search.RemoveIndexDir(s.cfg.IndexPath); err != nil {
		return s.keepOldIndex(tmpPath, err)
	}
	if err := os.Rename(tmpPath, s.cfg.IndexPath); err != nil {
		// Старой папки уже нет. Открываем собранный индекс там, где он остался.
		if opened, openErr := search.Open(tmpPath); openErr == nil {
			opened.CopyCountersFrom(idx)
			s.index = opened
		}
		return err
	}

	opened, err := search.Open(s.cfg.IndexPath)
	if err != nil {
		// Папка уже на месте. Пробуем открыть её в старом объекте, чтобы поиск не остался закрытым.
		if reopenErr := s.index.Reopen(); reopenErr != nil {
			return fmt.Errorf("open rebuilt index: %w (reopen: %v)", err, reopenErr)
		}
		s.index.CopyCountersFrom(idx)
		return nil
	}
	opened.CopyCountersFrom(idx)
	s.index = opened
	return nil
}

// keepOldIndex снова открываем прежний индекс и убираем временную папку.
func (s *Service) keepOldIndex(tmpPath string, cause error) error {
	reopenErr := s.index.Reopen()
	_ = search.RemoveIndexDir(tmpPath)
	if reopenErr != nil {
		return fmt.Errorf("%w (reopen: %v)", cause, reopenErr)
	}
	return cause
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
		// Файл больше лимита, пропал или стал ссылкой: старый документ в индексе не оставляем.
		return s.DeletePath(path)
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

// DeletePrefix удаляет документ и всё, что проиндексировано внутри этой папки.
func (s *Service) DeletePrefix(path string) error {
	id, err := filepath.Rel(s.scanner.RootDir(), path)
	if err != nil {
		return err
	}
	id = filepath.ToSlash(id)
	if id == "." || id == "" || id == ".." || strings.HasPrefix(id, "../") {
		return nil
	}

	s.mu.RLock() // не меняй указатель s.index
	defer s.mu.RUnlock()
	if err := s.index.DeletePrefix(id); err != nil {
		slog.Warn("delete prefix from index failed", "path", path, "error", err)
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
