//go:generate easyjson -all

package handler

import (
	"bufio"
	"context"
	"io"
	"sync"

	"github.com/mailru/easyjson"
)

type LogTs int
type LogLevel string
type LogMessage string

// easyjson:json
type LogEntry struct {
	Ts      LogTs      `json:"-"`
	Level   LogLevel   `json:"level"`
	Message LogMessage `json:"-"`
}

func createWorker(lines <-chan [][]byte, results chan<- map[string]int64) {
	localResult := make(map[string]int64)

	for bag := range lines {
		for _, line := range bag {
			entry := &LogEntry{}
			if err := easyjson.Unmarshal(line, entry); err == nil {
				localResult[string(entry.Level)]++
			}
		}
	}
	results <- localResult
}

func scanFile(ctx context.Context, input io.Reader, lines chan<- [][]byte) error {
	defer close(lines)
	scanner := bufio.NewScanner(input)

	maxCapacity := 10 * 1024 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	batchSize := 100000
	batch := make([][]byte, 0, batchSize)

	for scanner.Scan() {
		line := make([]byte, len(scanner.Bytes()))
		copy(line, scanner.Bytes())
		batch = append(batch, line)

		if len(batch) >= batchSize {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case lines <- batch:
				batch = make([][]byte, 0, batchSize)
			}
		}
	}

	if len(batch) > 0 {
		lines <- batch
	}

	return scanner.Err()
}

func getResult(results chan map[string]int64) map[string]int64 {
	finalResult := make(map[string]int64)
	for res := range results {
		for level, count := range res {
			finalResult[level] += count
		}
	}
	return finalResult
}

func Parse(ctx context.Context, n int, input io.Reader) (map[string]int64, error) {
	results := make(chan map[string]int64, n)
	lines := make(chan [][]byte, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			createWorker(lines, results)
		}()
	}

	if err := scanFile(ctx, input, lines); err != nil {
		return nil, err
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var finalResult = getResult(results)

	return finalResult, nil
}
