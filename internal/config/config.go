package config

import (
	"encoding/json"
	"errors"
	"os"
	"time"
)

type Config struct {
	Address          string        `json:"address"`             // порт
	RootDir          string        `json:"rootDir"`             // папка, которую индексируем
	IndexPath        string        `json:"indexPath"`           // где на диске лежит индекс Bleve
	SupportedExts    []string      `json:"supportedExtensions"` // какие расширения берём
	IgnoredDirs      []string      `json:"ignoredDirectories"`  // в какие папки не заходим
	MaxFileSizeBytes int64         `json:"maxFileSizeBytes"`
	DefaultLimit     int           `json:"defaultLimit"`    // сколько результатов, если limit не передали
	MaxLimit         int           `json:"maxLimit"`        // верхняя граница limit
	DebounceInterval time.Duration `json:"-"`               // то же debounce, но как Duration; из JSON не читаем
	DebounceMS       int           `json:"debounceMs"`      // debounce в миллисекундах из JSON
	UpdateQueueSize  int           `json:"updateQueueSize"` // размер очереди обновлений индекса
	UpdateWorkers    int           `json:"updateWorkers"`
}

// Default возвращает настройки, если файла нет или поле пустое.
func Default() Config {
	return Config{
		Address:          ":8080",
		RootDir:          ".",            // ищем файлы в текущей папке
		IndexPath:        "./data/index", // кладём индекс Bleve сюда
		SupportedExts:    []string{".txt", ".md", ".go"},
		IgnoredDirs:      []string{".git", "node_modules", "vendor", ".idea", "bin", "data"},
		MaxFileSizeBytes: 10 * 1024 * 1024, // 10 МБ
		DefaultLimit:     10,               // столько результатов отдаём по умолчанию
		MaxLimit:         100,              // больше этого limit не принимаем
		DebounceInterval: 300 * time.Millisecond,
		DebounceMS:       300, // ждём 300 мс, прежде чем обновить индекс
		UpdateQueueSize:  128, // сколько задач обновления держим в очереди
		UpdateWorkers:    2,   // два воркера читают очередь
	}
}

// Load читает JSON и накладывает его на значения по умолчанию
func Load(path string) (Config, error) {
	// Сначала берём дефолты, чтобы отсутствующие ключи JSON не оставили нули
	cfg := Default()
	if path == "" { // если путь не передан, возвращаем дефолт
		return cfg, nil
	}

	// Читаем файл как байты, JSON ещё не разбираем.
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cfg, nil
		}
		return cfg, err
	}

	// Пишем JSON прямо в cfg, поэтому передаём адрес.
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	// Подставляем дефолты в пустые и нулевые поля, считаем DebounceInterval
	cfg.applyDefaults()
	return cfg, nil
}

// тут просто заполняем пустые значения по умолчанию
func (c *Config) applyDefaults() {
	if c.Address == "" {
		c.Address = ":8080"
	}
	if c.RootDir == "" {
		c.RootDir = "."
	}
	if c.IndexPath == "" {
		c.IndexPath = "./data/index"
	}
	if len(c.SupportedExts) == 0 {
		c.SupportedExts = []string{".txt", ".md", ".go"}
	}
	if len(c.IgnoredDirs) == 0 {
		c.IgnoredDirs = []string{".git", "node_modules", "vendor", ".idea", "bin", "data"}
	}
	if c.MaxFileSizeBytes <= 0 {
		c.MaxFileSizeBytes = 10 * 1024 * 1024
	}
	if c.DefaultLimit <= 0 {
		c.DefaultLimit = 10
	}
	if c.MaxLimit <= 0 {
		c.MaxLimit = 100
	}
	if c.DefaultLimit > c.MaxLimit {
		// Не даём defaultLimit стать больше maxLimit.
		c.DefaultLimit = c.MaxLimit
	}
	if c.DebounceMS <= 0 {
		c.DebounceMS = 300
	}
	// Переводим число миллисекунд в time.Duration для таймера watcher.
	c.DebounceInterval = time.Duration(c.DebounceMS) * time.Millisecond
	if c.UpdateQueueSize <= 0 {
		c.UpdateQueueSize = 128
	}
	if c.UpdateWorkers <= 0 {
		c.UpdateWorkers = 2
	}
}
