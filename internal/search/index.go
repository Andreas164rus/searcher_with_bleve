package search

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"

	"local-search/internal/document"
)

// Stats — то, что отдаём в GET /stats.
type Stats struct {
	Documents    int       `json:"documents"`    // сколько файлов сейчас в индексе
	IndexedBytes int64     `json:"indexedBytes"` // суммарный размер содержимого
	LastUpdate   time.Time `json:"lastUpdate"`   // когда индекс меняли в последний раз
}

// Index — обёртка над Bleve плюс наша статистика.
type Index struct {
	path  string
	index bleve.Index
	mu    sync.RWMutex // защищаем индекс и stats от гонок поиска и обновления

	docs map[string]int64 // id -> размер, чтобы считать Documents и IndexedBytes
	stat Stats
}

// New открываем существующий индекс или создаём новый.
func New(path string) (*Index, error) {
	idx, err := openOrCreate(path)
	if err != nil {
		return nil, err
	}
	return &Index{
		path:  path,
		index: idx,
		docs:  make(map[string]int64),
	}, nil
}

// Recreate удаляем папку индекса и создаём его заново.
func Recreate(path string) (*Index, error) {
	if err := os.RemoveAll(path); err != nil {
		return nil, err
	}
	return New(path)
}

// Close закрываем Bleve. Write-lock, чтобы поиск в этот момент не шёл.
func (i *Index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.index == nil {
		return nil
	}
	err := i.index.Close()
	i.index = nil
	return err
}

// IndexDocument кладём или обновляем документ с тем же ID.
func (i *Index) IndexDocument(doc document.Document) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.index == nil {
		return errors.New("Индекс закрыт")
	}
	// Явно задаём поля, которые совпадают с mapping Bleve.
	fields := map[string]any{
		"path":    doc.Path,
		"name":    doc.Name,
		"modTime": doc.ModTime,
		"size":    doc.Size,
		"content": doc.Content, // именно ищем content
	}
	if err := i.index.Index(doc.ID, fields); err != nil {
		return err
	}
	i.setDocLocked(doc.ID, doc.Size) // обновляем статистику
	return nil
}

// DeleteDocument удаляем документ из Bleve по ID.
func (i *Index) DeleteDocument(id string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.index == nil {
		return errors.New("Индекс закрыт")
	}
	if err := i.index.Delete(id); err != nil {
		return err
	}
	i.deleteDocLocked(id) // обновляем статистику
	return nil
}

// Search ищем по индексу, не читая файлы заново.
func (i *Index) Search(query string, limit int) ([]document.Document, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.index == nil {
		return nil, errors.New("Индекс закрыт")
	}

	q := strings.TrimSpace(query)
	if q == "" {
		return []document.Document{}, nil
	}

	// MatchQuery: несколько слов ищем как полнотекстовый запрос Bleve.
	searchQuery := bleve.NewMatchQuery(q)
	req := bleve.NewSearchRequestOptions(searchQuery, limit, 0, false)
	req.Fields = []string{"path", "name", "content", "size", "modTime"}
	// Highlight: Bleve вырезает кусок текста вокруг найденного слова и помечает его, например <mark>scheduler</mark>.
	req.Highlight = bleve.NewHighlightWithStyle("html")

	res, err := i.index.Search(req)
	if err != nil {
		return nil, err
	}

	out := make([]document.Document, 0, len(res.Hits))
	for _, hit := range res.Hits {
		doc := document.Document{
			ID:    hit.ID,
			Path:  fieldString(hit.Fields["path"], hit.ID),
			Name:  fieldString(hit.Fields["name"], ""),
			Score: hit.Score, // чем больше, тем выше документ в выдаче
		}
		content := fieldString(hit.Fields["content"], "")
		doc.Fragment = firstHighlight(hit.Fragments["content"]) // готовый кусок от Bleve
		if doc.Fragment == "" {
			// Если highlight нет, вырезаем кусок текста сами.
			doc.Fragment = makeFragment(content, q)
		}
		out = append(out, doc)
	}

	return out, nil
}

// Stats отдаём копию текущей статистики.
func (i *Index) Stats() Stats {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.stat
}

// ReplaceWith подменяем внутренний индекс на новый, старый закрываем.
func (i *Index) ReplaceWith(newIndex *Index) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	old := i.index
	i.index = newIndex.index
	i.docs = newIndex.docs
	i.stat = newIndex.stat

	newIndex.index = nil // чтобы Close нового объекта не закрыл уже переданный индекс
	if old != nil {
		return old.Close()
	}
	return nil
}

// setDocLocked обновляем счётчики. Вызываем только под lock.
func (i *Index) setDocLocked(id string, size int64) {
	oldSize, existed := i.docs[id]
	if !existed {
		i.stat.Documents++
	}
	i.docs[id] = size
	i.stat.IndexedBytes += size - oldSize // при обновлении вычитаем старый размер
	i.stat.LastUpdate = time.Now().UTC()
}

// deleteDocLocked убираем документ из статистики.
func (i *Index) deleteDocLocked(id string) {
	oldSize, existed := i.docs[id]
	if !existed {
		i.stat.LastUpdate = time.Now().UTC()
		return
	}
	delete(i.docs, id)
	i.stat.Documents--
	i.stat.IndexedBytes -= oldSize
	i.stat.LastUpdate = time.Now().UTC()
}

// openOrCreate открываем индекс на диске или создаём его, если папки нет.
func openOrCreate(path string) (bleve.Index, error) {
	idx, err := bleve.Open(path)
	if err == nil {
		return idx, nil
	}
	if err != bleve.ErrorIndexPathDoesNotExist {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return bleve.New(path, buildMapping())
}

// buildMapping описываем схему документа: какое поле как хранить и искать.
func buildMapping() mapping.IndexMapping {
	indexMapping := bleve.NewIndexMapping()
	// Analyzer по умолчанию для всего индекса: нижний регистр, пунктуация, слова.
	indexMapping.DefaultAnalyzer = "standard"

	docMapping := bleve.NewDocumentMapping() // правила одного документа (у нас все файлы одного типа)

	// textField — полнотекстовое поле, вешаем на content.
	textField := bleve.NewTextFieldMapping()
	textField.Store = true              // сохраняем исходный текст в индексе, чтобы потом отдать fragment
	textField.IncludeTermVectors = true // храним позиции слов; без этого highlight хуже работает
	textField.IncludeInAll = true       // поле участвует в общем поиске MatchQuery
	textField.Analyzer = "standard"     // как режем content на термины

	// keywordField — путь и имя: храним строки, в общий поиск не мешаем.
	keywordField := bleve.NewTextFieldMapping()
	keywordField.Store = true         // path и name можно вернуть в JSON
	keywordField.IncludeInAll = false // запрос q=main не должен искать в первую очередь по имени main.go

	// numberField — размер файла, не текст.
	numberField := bleve.NewNumericFieldMapping()
	numberField.Store = true         // размер можно прочитать из индекса
	numberField.IncludeInAll = false // число не участвует в поиске слов

	// dateField — время изменения файла.
	dateField := bleve.NewDateTimeFieldMapping()
	dateField.Store = true         // дату храним
	dateField.IncludeInAll = false // дату не ищем как слово

	docMapping.AddFieldMappingsAt("content", textField) // единственное поле, по которому ищем текст
	docMapping.AddFieldMappingsAt("path", keywordField) // путь для ответа API
	docMapping.AddFieldMappingsAt("name", keywordField) // имя файла для ответа API
	docMapping.AddFieldMappingsAt("size", numberField)  // размер
	docMapping.AddFieldMappingsAt("modTime", dateField) // время изменения
	indexMapping.DefaultMapping = docMapping            // все документы разбираем по этой схеме
	return indexMapping
}

func fieldString(value any, fallback string) string {
	if value == nil {
		return fallback
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fallback
}

// firstHighlight берём самый короткий highlight-фрагмент.
func firstHighlight(fragments []string) string {
	if len(fragments) == 0 {
		return ""
	}
	sort.SliceStable(fragments, func(i, j int) bool {
		return len(fragments[i]) < len(fragments[j])
	})
	return fragments[0]
}

// makeFragment вырезаем кусок текста вокруг первого найденного слова запроса.
func makeFragment(content, query string) string {
	if content == "" {
		return ""
	}
	lowerContent := strings.ToLower(content)
	for _, term := range strings.Fields(strings.ToLower(query)) {
		if idx := strings.Index(lowerContent, term); idx >= 0 {
			start := max(0, idx-60)
			end := min(len(content), idx+len(term)+60)
			return strings.TrimSpace(content[start:end])
		}
	}
	if len(content) <= 160 {
		return strings.TrimSpace(content)
	}
	return strings.TrimSpace(content[:160])
}
