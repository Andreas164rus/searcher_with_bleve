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

	results, err := svc.Search("scheduler", 10, "and")
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

// Проверяем, что Rebuild подхватывает новый текст и не обнуляет статистику.
func TestRebuildKeepsSearchAndStats(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.txt")
	mustWrite(t, path, "goroutine")

	svc := newTestService(t, root)
	defer svc.Close()

	if err := svc.BuildInitialIndex(context.Background()); err != nil {
		t.Fatal(err)
	}

	mustWrite(t, path, "channel buffer")
	if err := svc.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}

	oldResults, err := svc.Search("goroutine", 10, "and")
	if err != nil {
		t.Fatal(err)
	}
	if len(oldResults) != 0 {
		t.Fatalf("old query results count = %d, want 0", len(oldResults))
	}

	newResults, err := svc.Search("channel", 10, "and")
	if err != nil {
		t.Fatal(err)
	}
	if len(newResults) != 1 {
		t.Fatalf("new query results count = %d, want 1", len(newResults))
	}

	stats := svc.Stats()
	if stats.Documents != 1 {
		t.Fatalf("documents = %d, want 1", stats.Documents)
	}

	// Второй rebuild проверяет, что индекс после первой замены снова можно закрыть.
	if err := svc.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats = svc.Stats()
	if stats.Documents != 1 {
		t.Fatalf("documents after second rebuild = %d, want 1", stats.Documents)
	}
	results, err := svc.Search("channel", 10, "and")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("results after second rebuild = %d, want 1", len(results))
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

	oldResults, err := svc.Search("goroutine", 10, "and")
	if err != nil {
		t.Fatal(err)
	}
	if len(oldResults) != 0 {
		t.Fatalf("old query results count = %d, want 0", len(oldResults))
	}

	newResults, err := svc.Search("channel", 10, "and")
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

	resultsAfterDelete, err := svc.Search("channel", 10, "and")
	if err != nil {
		t.Fatal(err)
	}
	if len(resultsAfterDelete) != 0 {
		t.Fatalf("results after delete = %d, want 0", len(resultsAfterDelete))
	}
}

// Проверяем, что файл, выросший больше maxFileSizeBytes, уходит из индекса.
func TestUpsertDropsFileOverMaxSize(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.txt")
	mustWrite(t, path, "goroutine")

	cfg := config.Default()
	cfg.RootDir = root
	cfg.IndexPath = filepath.Join(t.TempDir(), "index")
	cfg.MaxFileSizeBytes = 32

	svc, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	if err := svc.BuildInitialIndex(context.Background()); err != nil {
		t.Fatal(err)
	}

	mustWrite(t, path, "goroutine is now much longer than the limit")
	if err := svc.UpsertPath(path); err != nil {
		t.Fatal(err)
	}

	results, err := svc.Search("goroutine", 10, "and")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("results count = %d, want 0", len(results))
	}
	if stats := svc.Stats(); stats.Documents != 0 {
		t.Fatalf("documents = %d, want 0", stats.Documents)
	}
}

// Проверяем AND и OR: все слова против любого из слов.
func TestSearchAndOr(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "only.txt"), "goroutine")
	mustWrite(t, filepath.Join(root, "both.txt"), "goroutine scheduler")

	svc := newTestService(t, root)
	defer svc.Close()
	if err := svc.BuildInitialIndex(context.Background()); err != nil {
		t.Fatal(err)
	}

	andResults, err := svc.Search("goroutine scheduler", 10, "and")
	if err != nil {
		t.Fatal(err)
	}
	if len(andResults) != 1 || andResults[0].Path != "both.txt" {
		t.Fatalf("and results = %+v", andResults)
	}

	orResults, err := svc.Search("goroutine scheduler", 10, "or")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, doc := range orResults {
		got[doc.Path] = true
	}
	if len(got) != 2 || !got["only.txt"] || !got["both.txt"] {
		t.Fatalf("or results = %+v", orResults)
	}
}

// Проверяем, что удаление префикса папки снимает вложенные документы и не трогает соседние.
func TestDeletePrefixRemovesNestedDocuments(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "old", "a.txt"), "alpha goroutine")
	mustWrite(t, filepath.Join(root, "old", "nested", "b.txt"), "beta goroutine")
	mustWrite(t, filepath.Join(root, "old2", "c.txt"), "delta goroutine")
	mustWrite(t, filepath.Join(root, "other.txt"), "gamma goroutine")

	svc := newTestService(t, root)
	defer svc.Close()

	if err := svc.BuildInitialIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := svc.DeletePrefix(filepath.Join(root, "old")); err != nil {
		t.Fatal(err)
	}

	results, err := svc.Search("goroutine", 10, "and")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, doc := range results {
		got[doc.Path] = true
	}
	if got["old/a.txt"] || got["old/nested/b.txt"] {
		t.Fatalf("old paths still indexed: %v", got)
	}
	if !got["old2/c.txt"] || !got["other.txt"] || len(got) != 2 {
		t.Fatalf("remaining paths = %v", got)
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
