package document

import "time"

// один файл с полями
type Document struct {
	ID       string    `json:"id"`      // относительный путь
	Path     string    `json:"path"`    // путь, который отдаём в API
	Name     string    `json:"name"`    // только имя файла, без папок
	ModTime  time.Time `json:"modTime"` // время изменения файла на диске
	Size     int64     `json:"size"`
	Content  string    `json:"content"`            // полный текст, его индексирует Bleve
	Fragment string    `json:"fragment,omitempty"` // кусок текста вокруг совпадения; omitempty — не пишем пустое поле
	Score    float64   `json:"score,omitempty"`    // оценка релевантности от Bleve
}
