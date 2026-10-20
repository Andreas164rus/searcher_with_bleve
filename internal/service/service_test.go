package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"local-search/internal/config"
)

// Проверяем первичное индексирование и поиск: png не должен попасть в выдачу.
func TestBuildInitialIndexAndSearch(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "notes.txt"), "Go scheduler executes goroutines")
	mustWrite(t, filepath.Join(root, "image.png"), "scheduler in unsupported file")

	svc := newTestService(t, root)
	defer svc.Close()

	if err := svc.BuildInitialIndex(context.Background()); err != nil {
		t.Fatal(err)
	}

	results, err := svc.Search("scheduler", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("results count = %d, want 1", len(results))
	}
	if results[0].Path != "notes.txt" {
		t.Fatalf("path = %q, want notes.txt", results[0].Path)
	}
}

// Проверяем обновление одного файла и удаление из индекса без полного rebuild.
func TestUpsertAndDeletePath(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.md")
	mustWrite(t, path, "goroutine")

	svc := newTestService(t, root)
	defer svc.Close()

	if err := svc.BuildInitialIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Меняем содержимое и обновляем только этот документ.
	mustWrite(t, path, "channel")
	if err := svc.UpsertPath(path); err != nil {
		t.Fatal(err)
	}

	oldResults, err := svc.Search("goroutine", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(oldResults) != 0 {
		t.Fatalf("old query results count = %d, want 0", len(oldResults))
	}

	newResults, err := svc.Search("channel", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(newResults) != 1 {
		t.Fatalf("new query results count = %d, want 1", len(newResults))
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := svc.DeletePath(path); err != nil {
		t.Fatal(err)
	}

	resultsAfterDelete, err := svc.Search("channel", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(resultsAfterDelete) != 0 {
		t.Fatalf("results after delete = %d, want 0", len(resultsAfterDelete))
	}
}

// newTestService поднимаем сервис на временных папках, чтобы не трогать ./data/index.
func newTestService(t *testing.T, root string) *Service {
	t.Helper()
	cfg := config.Default()
	cfg.RootDir = root
	cfg.IndexPath = filepath.Join(t.TempDir(), "index")

	svc, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
