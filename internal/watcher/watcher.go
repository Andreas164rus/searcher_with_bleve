package watcher

import (
	"context"
	"hash/fnv"
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

// Run подписывается на папки до syncFn и только потом применяет события.
// Пока идёт первичный обход, события копятся и не затирают индекс старой версией файла.
// syncFn == nil значит, что обход уже сделан снаружи и события можно применять сразу.
func (w *Watcher) Run(ctx context.Context, syncFn func(context.Context) error) error {
	// fsWatcher — слушатель ОС (не наша структура Watcher): события приходят в его каналы.
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer fsWatcher.Close()

	// Дочерний контекст гасим, если syncFn упала: буфер тогда не проигрываем.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// У каждого воркера своя очередь. Путь всегда попадает в одну и ту же,
	// поэтому два обновления одного файла не выполняются параллельно.
	queues := w.workerQueues()
	var workers sync.WaitGroup
	for _, queue := range queues {
		workers.Add(1)
		go func(queue <-chan update) {
			defer workers.Done()
			w.worker(runCtx, queue)
		}(queue)
	}

	// Сборщик стартует до подписки
	scanDone := make(chan struct{}) // канал для сигнала о том, что индексирование завершено
	eventsDone := make(chan struct{}) // канал для сигнала о том, что события обработаны
	go func() {
		defer close(eventsDone)
		w.collectEvents(runCtx, fsWatcher, queues, scanDone)
		for _, queue := range queues {
			close(queue)
		}
	}()

	finish := func(err error) error {
		cancel()
		<-eventsDone
		workers.Wait()
		if err != nil {
			return err
		}
		return ctx.Err()
	}

	if err := w.watchDirRecursive(fsWatcher, w.service.RootDir()); err != nil {
		return finish(err)
	}
	if syncFn != nil {
		if err := syncFn(runCtx); err != nil {
			// Ошибку обхода отдаём наверх и не применяем хвост событий.
			return finish(err)
		}
	}
	close(scanDone)

	<-eventsDone
	workers.Wait()
	return ctx.Err()
}

// workerQueues делит updateQueueSize между воркерами. У каждого свой канал, буфер не меньше 1.
func (w *Watcher) workerQueues() []chan update {
	n := w.cfg.UpdateWorkers
	if n < 1 {
		n = 1
	}
	size := w.cfg.UpdateQueueSize
	base := size / n
	extra := size % n
	if base < 1 {
		// Иначе при маленькой очереди часть воркеров получила бы буфер 0.
		base = 1
		extra = 0
	}

	queues := make([]chan update, n)
	for i := range queues {
		buf := base
		if i < extra {
			buf++
		}
		queues[i] = make(chan update, buf)
	}
	return queues
}

// shard всегда возвращает один и тот же номер очереди для одного пути.
func shard(path string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(path))
	return int(h.Sum32() % uint32(n))
}

// collectEvents копит события и после паузы debounce отправляет по одному обновлению на файл.
// До закрытия scanDone события только складываются в буфер: обход индекса в это время ещё идёт.
func (w *Watcher) collectEvents(ctx context.Context, fsWatcher *fsnotify.Watcher, updates []chan update, scanDone <-chan struct{}) {
	// Несколько Write/Chmod одного файла схлопываются: в map остаётся последняя операция.
	pending := make(map[string]update)
	// quiet сбрасывается каждым новым событием: ждём тишину DebounceInterval.
	// maxWait стартует один раз на пачку и не сбрасывается, иначе непрерывный поток
	// событий никогда не дойдёт до flush.
	quiet := time.NewTimer(w.cfg.DebounceInterval)
	maxWait := time.NewTimer(2 * w.cfg.DebounceInterval)
	quiet.Stop()
	maxWait.Stop()
	defer quiet.Stop()
	defer maxWait.Stop()
	window := false // true, пока в этой пачке уже запущен maxWait
	var buffered []fsnotify.Event

	flush := func() {
		for key, item := range pending {
			select {
			case updates[shard(item.path, len(updates))] <- item:
			case <-ctx.Done():
				return
			}
			delete(pending, key)
		}
	}
	// Закрываем окно после flush, чтобы следующее событие снова запустило maxWait.
	disarm := func() {
		window = false
		quiet.Stop()
		maxWait.Stop()
	}
	note := func(event fsnotify.Event) {
		if !w.handleEvent(fsWatcher, event, pending) {
			// Игнор и пустая папка в pending не попадают и таймеры не двигают.
			return
		}
		if !window {
			maxWait.Reset(2 * w.cfg.DebounceInterval)
			window = true
		}
		quiet.Reset(w.cfg.DebounceInterval)
	}

	for {
		select {
		case <-ctx.Done():
			// Обход не закончился: буфер не применяем, чтобы не писать в закрывающийся индекс.
			return
		case <-scanDone:
			for _, event := range buffered {
				note(event)
			}
			buffered = nil
			goto live
		case err, ok := <-fsWatcher.Errors:
			if !ok {
				return
			}
			slog.Warn("ошибка слежения за файлами", "error", err)
		case event, ok := <-fsWatcher.Events:
			if !ok {
				return
			}
			buffered = append(buffered, event)
		}
	}

live:
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
			note(event)
		case <-quiet.C:
			flush()
			disarm()
		case <-maxWait.C:
			flush()
			disarm()
		}
	}
}

// handleEvent превращаем событие ОС в upsert или delete. Несколько событий одного файла перезаписывают pending.
// true — в pending появилась или обновилась задача, и debounce нужно завести.
func (w *Watcher) handleEvent(fsWatcher *fsnotify.Watcher, event fsnotify.Event, pending map[string]update) bool {
	if w.service.ShouldIgnorePath(event.Name) {
		return false
	}

	queued := false
	if event.Has(fsnotify.Create) {
		if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
			// Новую папку тоже начинаем слушать: подписка родителя не покрывает вложенные каталоги.
			if err := w.watchDirRecursive(fsWatcher, event.Name); err != nil {
				slog.Warn("не удалось подписаться на новую папку", "path", event.Name, "error", err)
			}
			// Переименование папки приходит как Create без событий на каждый файл внутри.
			if w.queueTree(event.Name, pending) {
				queued = true
			}
		} else {
			pending[event.Name] = update{path: event.Name, op: opUpsert}
			queued = true
		}
	}
	if event.Has(fsnotify.Write) || event.Has(fsnotify.Chmod) {
		pending[event.Name] = update{path: event.Name, op: opUpsert}
		queued = true
	}
	if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
		// Rename старого пути удаляет документ или всю папку. Новый путь приходит отдельным CREATE.
		pending[event.Name] = update{path: event.Name, op: opDelete}
		queued = true
	}
	return queued
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
				// Для файла снимется один документ, для папки — все документы внутри.
				if err := w.service.DeletePrefix(item.path); err != nil {
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

// queueTree ставит в очередь все поддерживаемые файлы внутри папки. Саму папку не индексируем.
func (w *Watcher) queueTree(root string, pending map[string]update) bool {
	queued := false
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			slog.Warn("пропускаем путь при обходе новой папки", "path", path, "error", walkErr)
			return nil
		}
		if entry.IsDir() {
			if path != root && w.service.ShouldIgnorePath(path) {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if w.service.ShouldIgnorePath(path) || !w.service.IsSupported(path) {
			return nil
		}
		pending[path] = update{path: path, op: opUpsert}
		queued = true
		return nil
	})
	if err != nil {
		slog.Warn("не удалось обойти новую папку", "path", root, "error", err)
	}
	return queued
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
