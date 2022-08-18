package csv

import (
	"encoding/csv"
	"os"
	"sync"
)

// Cấu trúc ghi file cho việc ghi đồng thời giữa các goroutine
type CsvWriter struct {
	mutex     *sync.Mutex
	csvWriter *csv.Writer
}

func NewCsvWriter(fileName string) (*CsvWriter, error) {
	csvFile, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(csvFile)
	return &CsvWriter{csvWriter: w, mutex: &sync.Mutex{}}, nil
}

func (w *CsvWriter) WriteAll(rows [][]string) error {
	w.mutex.Lock()
	err := w.csvWriter.WriteAll(rows)
	if err != nil {
		return err
	}

	w.mutex.Unlock()
	return nil
}

func (w *CsvWriter) Write(row []string) error {
	w.mutex.Lock()
	err := w.csvWriter.Write(row)
	if err != nil {
		return err
	}

	w.mutex.Unlock()
	return nil
}

func (w *CsvWriter) Flush() {
	w.mutex.Lock()
	w.csvWriter.Flush()
	w.mutex.Unlock()
}
