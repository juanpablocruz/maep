package feed

import (
	"encoding/csv"
	"os"
)

func readCsvFile(path string) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	csvReader.Comma = ';'
	csvReader.LazyQuotes = true
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}
	return records, nil
}

func Feed() ([][]string, error) {
	return readCsvFile("feed.csv")
}
