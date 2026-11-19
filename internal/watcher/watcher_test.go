package watcher

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"local-search/internal/config"
	"local-search/internal/service"
)

// Проверяем, что переименование папки убирает старые пути и индексирует файлы заново.
func TestRenameDirectoryUpdatesIndex(t *testing.T) {
	root := t.TempDir()
	cfg := config.Default()
	cfg.RootDir = root
	cfg.IndexPath = filepath.Join(t.TempDir(), "index")
	cfg.DebounceMS = 30
	cfg.DebounceInterval = 30 * time.Millisecond

	svc, err := service.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	if err := svc.BuildInitialIndex(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- New(cfg, svc).Run(ctx, nil)
	}()

	oldNotes := filepath.Join(root, "old", "notes.txt")
	waitUntil(t, 5*time.Second, func() bool {
		if err := os.MkdirAll(filepath.Dir(oldNotes), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(oldNotes, []byte("goroutine scheduler"), 0o644); err != nil {
			t.Fatal(err)
		}
		return hasPath(t, svc, "old/notes.txt")
	})

	if err := os.Rename(filepath.Join(root, "old"), filepath.Join(root, "new")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, func() bool {
		return hasPath(t, svc, "new/notes.txt") && !hasPath(t, svc, "old/notes.txt")
	})

	cancel()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not stop")
	}
}

// Проверяем, что правка файла во время первичного обхода не теряется.
func TestChangeDuringInitialIndexIsNotLost(t *testing.T) {
	root := t.TempDir()
	notes := filepath.Join(root, "notes.txt")
	if err := os.WriteFile(notes, []byte("goroutine"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.RootDir = root
	cfg.IndexPath = filepath.Join(t.TempDir(), "index")
	cfg.DebounceMS = 30
	cfg.DebounceInterval = 30 * time.Millisecond

	svc, err := service.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- New(cfg, svc).Run(ctx, func(ctx context.Context) error {
			if err := svc.BuildInitialIndex(ctx); err != nil {
				return err
			}
			// Подписка уже висит: это событие должно попасть в буфер, а не потеряться.
			return os.WriteFile(notes, []byte("channel buffer"), 0o644)
		})
	}()

	waitUntil(t, 5*time.Second, func() bool {
		updated, err := svc.Search("channel", 10, "and")
		if err != nil {
			t.Fatal(err)
		}
		old, err := svc.Search("goroutine", 10, "and")
		if err != nil {
			t.Fatal(err)
		}
		return len(updated) == 1 && updated[0].Path == "notes.txt" && len(old) == 0
	})

	cancel()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not stop")
	}
}

func hasPath(t *testing.T, svc *service.Service, want string) bool {
	t.Helper()
	results, err := svc.Search("goroutine", 10, "and")
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range results {
		if doc.Path == want {
			return true
		}
	}
	return false
}

func startWatcher(t *testing.T, root string, debounce time.Duration) (*service.Service, context.CancelFunc, <-chan error) {
	t.Helper()
	cfg := config.Default()
	cfg.RootDir = root
	cfg.IndexPath = filepath.Join(t.TempDir(), "index")
	cfg.DebounceMS = int(debounce / time.Millisecond)
	cfg.DebounceInterval = debounce

	svc, err := service.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	if err := svc.BuildInitialIndex(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	ready := make(chan struct{})
	go func() {
		errCh <- New(cfg, svc).Run(ctx, func(context.Context) error {
			close(ready)
			return nil
		})
	}()
	select {
	case <-ready:
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not subscribe")
	}
	return svc, cancel, errCh
}

func stopWatcher(t *testing.T, cancel context.CancelFunc, errCh <-chan error) {
	t.Helper()
	cancel()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not stop")
	}
}

func TestCreateFileAppearsInSearch(t *testing.T) {
	root := t.TempDir()
	svc, cancel, errCh := startWatcher(t, root, 30*time.Millisecond)
	defer stopWatcher(t, cancel, errCh)

	path := filepath.Join(root, "new.md")
	waitUntil(t, 5*time.Second, func() bool {
		if err := os.WriteFile(path, []byte("goroutine scheduler"), 0o644); err != nil {
			t.Fatal(err)
		}
		return hasPath(t, svc, "new.md")
	})
}

func TestUpdateFileReplacesSearchHit(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.md")
	if err := os.WriteFile(path, []byte("goroutine"), 0o644); err != nil {
		t.Fatal(err)
	}
	svc, cancel, errCh := startWatcher(t, root, 30*time.Millisecond)
	defer stopWatcher(t, cancel, errCh)

	if err := os.WriteFile(path, []byte("channel"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, func() bool {
		old, err := svc.Search("goroutine", 10, "and")
		if err != nil {
			t.Fatal(err)
		}
		updated, err := svc.Search("channel", 10, "and")
		if err != nil {
			t.Fatal(err)
		}
		return len(old) == 0 && len(updated) == 1 && updated[0].Path == "notes.md"
	})
}

func TestDeleteFileDropsSearchHit(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.md")
	if err := os.WriteFile(path, []byte("goroutine"), 0o644); err != nil {
		t.Fatal(err)
	}
	svc, cancel, errCh := startWatcher(t, root, 30*time.Millisecond)
	defer stopWatcher(t, cancel, errCh)

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, func() bool {
		results, err := svc.Search("goroutine", 10, "and")
		if err != nil {
			t.Fatal(err)
		}
		return len(results) == 0
	})
}

func TestRenameFileReplacesPath(t *testing.T) {
	root := t.TempDir()
	oldPath := filepath.Join(root, "old.md")
	if err := os.WriteFile(oldPath, []byte("goroutine"), 0o644); err != nil {
		t.Fatal(err)
	}
	svc, cancel, errCh := startWatcher(t, root, 30*time.Millisecond)
	defer stopWatcher(t, cancel, errCh)

	if err := os.Rename(oldPath, filepath.Join(root, "new.md")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, func() bool {
		return hasPath(t, svc, "new.md") && !hasPath(t, svc, "old.md")
	})
}

func TestDebounceCollapsesWrites(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.txt")
	if err := os.WriteFile(path, []byte("version 0 goroutine"), 0o644); err != nil {
		t.Fatal(err)
	}
	svc, cancel, errCh := startWatcher(t, root, 300*time.Millisecond)
	defer stopWatcher(t, cancel, errCh)

	before := svc.UpdateCount()
	for i := 1; i <= 20; i++ {
		body := []byte("version final goroutine")
		if err := os.WriteFile(path, body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, 5*time.Second, func() bool {
		results, err := svc.Search("final", 10, "and")
		if err != nil {
			t.Fatal(err)
		}
		return len(results) == 1
	})
	if got := svc.UpdateCount() - before; got != 1 {
		t.Fatalf("index updates = %d, want 1", got)
	}
}

func TestConcurrentSearchDuringUpdates(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 8; i++ {
		path := filepath.Join(root, "f"+string(rune('a'+i))+".txt")
		if err := os.WriteFile(path, []byte("goroutine"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	svc, cancel, errCh := startWatcher(t, root, 20*time.Millisecond)
	defer stopWatcher(t, cancel, errCh)

	runCtx, stop := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer stop()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			path := filepath.Join(root, "f"+string(rune('a'+i))+".txt")
			for runCtx.Err() == nil {
				_ = os.WriteFile(path, []byte("goroutine update"), 0o644)
			}
		}(i)
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for runCtx.Err() == nil {
				if _, err := svc.Search("goroutine", 10, "and"); err != nil {
					t.Errorf("search: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func waitUntil(t *testing.T, limit time.Duration, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
