// Замеры индексации и поиска на sample_data/100, 1000 и 5000.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"local-search/internal/config"
	"local-search/internal/scanner"
	"local-search/internal/search"
	"local-search/internal/service"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "update" {
		measureUpdate()
		return
	}
	roots := []string{"sample_data/100", "sample_data/1000", "sample_data/5000"}
	fmt.Println("files | index time | indexed bytes | index size | avg search")
	var big *service.Service
	var bigCfg config.Config
	for _, root := range roots {
		cfg := config.Default()
		cfg.RootDir = root
		cfg.IndexPath = filepath.Join("data", "bench-"+filepath.Base(root))
		svc, err := service.New(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", root, err)
			os.Exit(1)
		}
		start := time.Now()
		if err := svc.BuildInitialIndex(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "%s index: %v\n", root, err)
			os.Exit(1)
		}
		indexTime := time.Since(start)
		stats := svc.Stats()
		searchStart := time.Now()
		const searches = 20
		for range searches {
			if _, err := svc.Search("scheduler", 10, "and"); err != nil {
				fmt.Fprintf(os.Stderr, "%s search: %v\n", root, err)
				os.Exit(1)
			}
		}
		avgSearch := time.Since(searchStart) / searches
		size := dirSize(cfg.IndexPath)
		fmt.Printf("%s | %s | %d | %d | %s\n", filepath.Base(root), indexTime.Round(time.Millisecond), stats.IndexedBytes, size, avgSearch.Round(time.Microsecond))
		if filepath.Base(root) == "5000" {
			big = svc
			bigCfg = cfg
			continue
		}
		_ = svc.Close()
	}

	one := filepath.Join(bigCfg.RootDir, "file-0001.txt")
	body, err := os.ReadFile(one)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read one: %v\n", err)
		os.Exit(1)
	}
	body = append(body, []byte(" incremental update marker\n")...)
	if err := os.WriteFile(one, body, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write one: %v\n", err)
		os.Exit(1)
	}
	start := time.Now()
	if err := big.UpsertPath(one); err != nil {
		fmt.Fprintf(os.Stderr, "upsert: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("update one | %s\n", time.Since(start).Round(time.Millisecond))

	start = time.Now()
	if err := big.Rebuild(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "rebuild: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("rebuild | %s\n", time.Since(start).Round(time.Millisecond))
	_ = big.Close()
}

func dirSize(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

// measureUpdate считает обновление одного документа и полный rebuild на уже собранном индексе 5000.
func measureUpdate() {
	root, err := filepath.Abs("sample_data/5000")
	if err != nil {
		fmt.Fprintf(os.Stderr, "root: %v\n", err)
		os.Exit(1)
	}
	indexPath, err := filepath.Abs(filepath.Join("data", "bench-5000"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "index: %v\n", err)
		os.Exit(1)
	}

	cfg := config.Default()
	cfg.RootDir = root
	cfg.IndexPath = indexPath
	scn, err := scanner.New(cfg.RootDir, cfg.SupportedExts, cfg.IgnoredDirs, cfg.MaxFileSizeBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scanner: %v\n", err)
		os.Exit(1)
	}
	idx, err := search.Open(indexPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	one := filepath.Join(root, "file-0001.txt")
	doc, ok, err := scn.ReadDocument(one)
	if err != nil || !ok {
		fmt.Fprintf(os.Stderr, "read document: ok=%v err=%v\n", ok, err)
		os.Exit(1)
	}
	start := time.Now()
	if err := idx.IndexDocument(doc); err != nil {
		fmt.Fprintf(os.Stderr, "upsert: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("update one | %s\n", time.Since(start).Round(time.Millisecond))
	if err := idx.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close: %v\n", err)
		os.Exit(1)
	}

	svc, err := service.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "service: %v\n", err)
		os.Exit(1)
	}
	defer svc.Close()
	start = time.Now()
	if err := svc.Rebuild(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "rebuild: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("rebuild | %s\n", time.Since(start).Round(time.Millisecond))
}
