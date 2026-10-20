package scanner

import (
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"local-search/internal/document"
)

// Scanner обходит папку, фильтрует файлы и собирает Document.
type Scanner struct {
	rootDir        string              // абсолютный путь папки, которую индексируем
	extensions     map[string]struct{} // множество разрешённых расширений: ключ есть/нет
	ignoredDirs    map[string]struct{} // множество имён папок, в которые не заходим
	maxFileSize    int64               // файлы больше этого размера пропускаем
	indexRootClean string              // нормализованный root, чтобы сравнивать пути
}

// New готовит сканер: считаем абсолютный root и нормализуем списки из конфига.
func New(rootDir string, supportedExts, ignoredDirs []string, maxFileSize int64) (*Scanner, error) {
	// Делаем полный путь, чтобы относительный ID потом считался стабильно.
	absRoot, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, err
	}

	// Приводим расширения к одному виду: ".txt", не "TXT" и не "txt"
	extensions := make(map[string]struct{}, len(supportedExts))
	for _, ext := range supportedExts {
		ext = strings.ToLower(strings.TrimSpace(ext)) // убираем пробелы и делаем lowercase
		if ext == "" {
			continue
		}
		if !strings.HasPrefix(ext, ".") {
			ext = "." + ext // добавляем точку, если нет
		}
		extensions[ext] = struct{}{} // помечаем расширение как разрешённое
	}

	ignored := make(map[string]struct{}, len(ignoredDirs))
	for _, dir := range ignoredDirs {
		dir = strings.TrimSpace(dir)
		if dir != "" {
			ignored[dir] = struct{}{}
		}
	}

	return &Scanner{
		rootDir:        absRoot,
		extensions:     extensions,
		ignoredDirs:    ignored,
		maxFileSize:    maxFileSize,
		indexRootClean: filepath.Clean(absRoot),
	}, nil
}

// Scan рекурсивно обходит root и для каждого подходящего файла вызывает fn.
func (s *Scanner) Scan(fn func(document.Document) error) error {
	// fn — callback: мы находим документ, снаружи кладем в индекс.

	// тут обходим папку root и на каждый путь вызываем fn
	return filepath.WalkDir(s.rootDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			slog.Warn("пропущен путь после walk error", "path", path, "error", walkErr)
			return nil
		}

		if entry.IsDir() {
			if path != s.rootDir && s.shouldIgnoreDir(entry.Name()) {
				return filepath.SkipDir // не заходить внутрь .git, node_modules и т.п.
			}
			// Обычную папку не читаем как документ, просто идём дальше.
			return nil
		}

		// Читаем файл в Document или решаем, что его надо пропустить.
		doc, ok, err := s.ReadDocument(path)
		if err != nil {
			slog.Warn("пропустили файл из-за ошибки чтения", "path", path, "error", err)
			return nil
		}
		if !ok {
			// Файл просто не наш: png, слишком большой, symlink.
			return nil
		}
		if err := fn(doc); err != nil {
			// Ошибка callback останавливает весь обход, например отмена context.
			return err
		}
		return nil
	})
}

// ReadDocument читает один путь. Возвращаем (документ, ok, error).
func (s *Scanner) ReadDocument(path string) (document.Document, bool, error) {
	// Lstat не идёт по symlink, поэтому ярлык можно отсечь отдельно.
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Файла уже нет — возвращаем пустой документ и ok=false.
			return document.Document{}, false, nil
		}
		return document.Document{}, false, err
	}
	if info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		// Папку или ярлык пропускаем.
		return document.Document{}, false, nil
	}
	if !s.IsSupported(path) {
		// Неподдерживаемое расширение пропускаем.
		return document.Document{}, false, nil
	}
	if info.Size() > s.maxFileSize {
		return document.Document{}, false, nil
	}

	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Между Lstat и чтением файл могли удалить — пропускаем.
			return document.Document{}, false, nil
		}
		return document.Document{}, false, err
	}

	// Считаем путь относительно root: это будет ID документа.
	rel, err := filepath.Rel(s.rootDir, path)
	if err != nil {
		return document.Document{}, false, err
	}
	id := filepath.ToSlash(rel) // делает одинаковый / во всех путях

	return document.Document{
		ID:      id,
		Path:    id,
		Name:    filepath.Base(path), // берём только имя файла
		ModTime: info.ModTime(),
		Size:    info.Size(),
		Content: string(content), // байты превращаем в текст для Bleve
	}, true, nil
}

// IsSupported проверяем, есть ли расширение файла в разрешённом множестве.
func (s *Scanner) IsSupported(path string) bool {
	_, ok := s.extensions[strings.ToLower(filepath.Ext(path))]
	return ok
}

// ShouldIgnorePath смотрит, лежит ли путь внутри ignored-директории. Нужен для watcher.
func (s *Scanner) ShouldIgnorePath(path string) bool {
	abs, err := filepath.Abs(path)
	if err != nil {
		return true // кривой путь пропускаем
	}
	rel, err := filepath.Rel(s.indexRootClean, filepath.Clean(abs)) // считает относительный путь от indexRootClean до файла
	if err != nil || rel == "." {
		return false // "." — это сам root, его не игнорируем
	}
	// Режем путь на части и проверяем, есть ли среди них ignored-имя.
	parts := strings.Split(filepath.ToSlash(rel), "/")
	return slices.ContainsFunc(parts, s.shouldIgnoreDir) // проверяет, есть ли в игнорируемых директориях
}

// RootDir отдаём абсолютный корень сканера.
func (s *Scanner) RootDir() string {
	return s.rootDir
}

// shouldIgnoreDir проверяем, есть ли имя папки в ignore-списке.
func (s *Scanner) shouldIgnoreDir(name string) bool {
	_, ok := s.ignoredDirs[name]
	return ok
}
