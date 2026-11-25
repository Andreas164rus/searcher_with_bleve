package search

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveIndexDirKeepsOrdinaryDirectory(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(marker, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := RemoveIndexDir(dir); err == nil {
		t.Fatal("RemoveIndexDir error = nil, want refusal")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("ordinary file was removed: %v", err)
	}
}

func TestRemoveIndexDirRemovesBleveDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index_meta.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := RemoveIndexDir(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("index dir still exists: %v", err)
	}
}

func TestRemoveIndexDirMissingPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-index")
	if err := RemoveIndexDir(missing); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveIndexDirKeepsProjectDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index_meta.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := RemoveIndexDir(dir); err == nil {
		t.Fatal("RemoveIndexDir error = nil, want refusal")
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
		t.Fatalf("project file was removed: %v", err)
	}
}
