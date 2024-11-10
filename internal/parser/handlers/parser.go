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

func createWorker(lines <-chan [][]byte, results chan<- map[LogLevel]int32) {
	localResult := make(map[LogLevel]int32)

	for bag := range lines {
		for _, line := range bag {
			entry := &LogEntry{}
			if err := easyjson.Unmarshal(line, entry); err == nil {
				localResult[entry.Level]++
			}
		}
	}
	results <- localResult
}

func scanFile(ctx context.Context, input io.Reader, lines chan<- [][]byte) error {
	defer close(lines)

	var (
		BUF_MAX_CAPACITY = 10 * 1024 * 1024
		BATCH_MAX_SIZE   = 10000
	)

	buf := make([]byte, BUF_MAX_CAPACITY)

	scanner := bufio.NewScanner(input)
	scanner.Buffer(buf, BUF_MAX_CAPACITY)

	batch := make([][]byte, 0, BATCH_MAX_SIZE)

	for scanner.Scan() {
		line := make([]byte, len(scanner.Bytes()))
		copy(line, scanner.Bytes())
		batch = append(batch, line)

		if len(batch) >= BATCH_MAX_SIZE {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case lines <- batch:
				batch = make([][]byte, 0, BATCH_MAX_SIZE)
			}
		}
	}

	if len(batch) > 0 {
		lines <- batch
	}

	return scanner.Err()
}

func getResult(results chan map[LogLevel]int32) map[LogLevel]int32 {
	finalResult := make(map[LogLevel]int32)
	for res := range results {
		for level, count := range res {
			finalResult[level] += count
		}
	}
	return finalResult
}

func Parse(ctx context.Context, n int, input io.Reader) (map[LogLevel]int32, error) {
	var (
		results = make(chan map[LogLevel]int32, n)
		lines   = make(chan [][]byte, n*2)
	)

	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
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
