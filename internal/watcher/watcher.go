package watcher

import (
	"context"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"local-search/internal/config"
	"local-search/internal/service"
)

type operation int

const (
	opUpsert operation = iota // создать или обновить документ
	opDelete                  // удалить документ из индекса
)

// update — одна задача в очередь после debounce: уже не сырое событие ОС.
type update struct {
	path string
	op   operation
}

type Watcher struct {
	cfg     config.Config
	service *service.Service // тот же service, что у HTTP: UpsertPath / DeletePath
}

func New(cfg config.Config, svc *service.Service) *Watcher {
	return &Watcher{cfg: cfg, service: svc}
}

// Run подписываемся на папки, запускаем воркеры и собираем события, пока жив ctx.
func (w *Watcher) Run(ctx context.Context) error {
	// fsWatcher — слушатель ОС (не наша структура Watcher): события приходят в его каналы.
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer fsWatcher.Close()

	if err := w.watchDirRecursive(fsWatcher, w.service.RootDir()); err != nil {
		return err
	}

	// Очередь задач индекса. Буфер, чтобы сборщик не блокировался, пока воркеры читают файлы.
	updates := make(chan update, w.cfg.UpdateQueueSize)
	var workers sync.WaitGroup
	for range w.cfg.UpdateWorkers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			w.worker(ctx, updates)
		}()
	}

	// eventsDone нужен, потому что collectEvents крутится в другой горутине:
	// Run должен дождаться и сборщика, и воркеров.
	eventsDone := make(chan struct{})
	go func() {
		defer close(eventsDone)
		// Сначала события, потом закрываем очередь — воркеры видят !ok и выходят.
		w.collectEvents(ctx, fsWatcher, updates) // тут говорим fsnotify читай каналы в цикле
		close(updates)
	}()

	<-eventsDone
	workers.Wait()
	return ctx.Err()
}

// collectEvents копит события и после паузы debounce отправляет по одному обновлению на файл.
func (w *Watcher) collectEvents(ctx context.Context, fsWatcher *fsnotify.Watcher, updates chan<- update) {
	// Несколько Write/Chmod одного файла схлопываются: в map остаётся последняя операция.
	pending := make(map[string]update) // сюда приходят все изменения, чтобы не копить одинаковые изменния,
	// которые приходят очень быстро, они раскидываются по ключу, и спустя 300 мс отправляем их в updates
	timer := time.NewTimer(w.cfg.DebounceInterval)
	if !timer.Stop() {
		// NewTimer мог сразу сработать; забираем значение, чтобы канал был пустой.
		<-timer.C
	}

	flush := func() {
		for key, item := range pending {
			select {
			case updates <- item:
			case <-ctx.Done():
				return
			}
			delete(pending, key)
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case err, ok := <-fsWatcher.Errors:
			if !ok {
				return
			}
			slog.Warn("ошибка слежения за файлами", "error", err)
		case event, ok := <-fsWatcher.Events:
			if !ok {
				flush()
				return
			}
			w.handleEvent(fsWatcher, event, pending)
			// Каждое новое событие сбрасывает таймер: ждём тишину debounceMs.
			timer.Reset(w.cfg.DebounceInterval)
		case <-timer.C:
			flush()
		}
	}
}

// handleEvent превращаем событие ОС в upsert или delete. Несколько событий одного файла перезаписывают pending.
func (w *Watcher) handleEvent(fsWatcher *fsnotify.Watcher, event fsnotify.Event, pending map[string]update) {
	if w.service.ShouldIgnorePath(event.Name) {
		return
	}

	if event.Has(fsnotify.Create) {
		if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
			// Новую папку тоже начинаем слушать: подписка родителя не покрывает вложенные каталоги.
			if err := w.watchDirRecursive(fsWatcher, event.Name); err != nil {
				slog.Warn("не удалось подписаться на новую папку", "path", event.Name, "error", err)
			}
			return
		}
		pending[event.Name] = update{path: event.Name, op: opUpsert}
	}
	if event.Has(fsnotify.Write) || event.Has(fsnotify.Chmod) {
		pending[event.Name] = update{path: event.Name, op: opUpsert}
	}
	if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
		// Rename для старого пути — это удаление; новый путь обычно приходит отдельным CREATE.
		pending[event.Name] = update{path: event.Name, op: opDelete}
	}
}

// worker читает очередь и обновляет индекс. Тяжёлую работу не делаем внутри collectEvents.
func (w *Watcher) worker(ctx context.Context, updates <-chan update) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-updates:
			if !ok {
				return
			}
			if item.op == opDelete {
				if err := w.service.DeletePath(item.path); err != nil {
					slog.Warn("не удалось удалить из индекса", "path", item.path, "error", err)
				}
				continue
			}
			if err := w.service.UpsertPath(item.path); err != nil {
				slog.Warn("не удалось обновить документ в индексе", "path", item.path, "error", err)
			}
		}
	}
}

// watchDirRecursive вешаем fsnotify на все вложенные папки, ignored пропускаем.
func (w *Watcher) watchDirRecursive(fsWatcher *fsnotify.Watcher, root string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			slog.Warn("пропускаем путь при подписке", "path", path, "error", walkErr)
			return nil
		}
		if !entry.IsDir() {
			return nil
		}
		if path != root && w.service.ShouldIgnorePath(path) {
			return filepath.SkipDir
		}
		// Add только на папку: события про файлы внутри придут сами.
		if err := fsWatcher.Add(path); err != nil {
			slog.Warn("не удалось подписаться на папку", "path", path, "error", err)
		}
		return nil
	})
}
