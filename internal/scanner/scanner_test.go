package scanner

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"local-search/internal/document"
)

// Проверяем, что в обход попадают только .txt, .md и .go, а png/mp4 пропускаем.
func TestScanIndexesOnlySupportedFiles(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "notes.txt"), "scheduler")
	mustWrite(t, filepath.Join(root, "lecture.md"), "goroutine")
	mustWrite(t, filepath.Join(root, "main.go"), "package main")
	mustWrite(t, filepath.Join(root, "image.png"), "not text")
	mustWrite(t, filepath.Join(root, "movie.mp4"), "not text")

	scn, err := New(root, []string{".txt", ".md", ".go"}, []string{".git"}, 1024)
	if err != nil {
		t.Fatal(err)
	}

	var ids []string
	// Собираем ID документов, которые сканер считает подходящими.
	if err := scn.Scan(func(doc document.Document) error {
		ids = append(ids, doc.ID)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	want := []string{"lecture.md", "main.go", "notes.txt"}
	slices.Sort(ids)
	if !slices.Equal(ids, want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
}

// mustWrite создаём файл вместе с родительскими папками.
func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
