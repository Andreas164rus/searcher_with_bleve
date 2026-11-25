// Генератор текстовых корпусов для замеров индексации и поиска.
// Создаёт sample_data/100, sample_data/1000 и sample_data/5000:
// в каждой папке ровно N файлов .txt по 1 МиБ.
package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
)

const (
	outDir   = "sample_data"
	fileSize = 1 << 20 // 1 МиБ, меньше лимита maxFileSizeBytes (10 МиБ)
)

// Учебный текст: в каждом файле есть термины, по которым потом можно искать.
var paragraph = []byte("The Go runtime contains a scheduler that decides which goroutine runs on which operating system thread, moves a waiting goroutine from the global run queue into a local run queue, and parks a blocked goroutine until a channel send, a channel receive, or a timer wakes it up again.\n")

func main() {
	counts := []int{100, 1000, 5000}

	// Общий блок повторяющегося текста. Уникальная метка файла дописывается отдельно.
	body := bytes.Repeat(paragraph, (fileSize/len(paragraph))+1)
	buf := make([]byte, fileSize)

	for _, count := range counts {
		dir := filepath.Join(outDir, fmt.Sprintf("%d", count))
		if err := writeCorpus(dir, count, body, buf); err != nil {
			fmt.Fprintf(os.Stderr, "sample_data/%d: %v\n", count, err)
			os.Exit(1)
		}
	}
}

// writeCorpus пересоздаёт папку и пишет count файлов ровно fileSize байт.
func writeCorpus(dir string, count int, body, buf []byte) error {
	// Старые файлы в этой папке не оставляем: корпус должен совпадать с count.
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	fmt.Printf("%s: пишу %d файлов\n", dir, count)

	var total int64
	for i := 1; i <= count; i++ {
		label := fmt.Sprintf("file-%04d", i)
		header := []byte("document " + label + "\n")
		fill(buf, header, body)

		path := filepath.Join(dir, label+".txt")
		if err := os.WriteFile(path, buf, 0o644); err != nil {
			return err
		}
		total += int64(len(buf))
	}

	fmt.Printf("%s: %d файлов, %d байт\n", dir, count, total)
	return nil
}

// fill заполняет dst целиком: сначала короткая метка файла, затем повтор paragraph.
func fill(dst, header, body []byte) {
	n := copy(dst, header)
	copy(dst[n:], body)
}
