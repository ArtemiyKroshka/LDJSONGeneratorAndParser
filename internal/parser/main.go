package main

import (
	"GeneratorAndParser/internal/handlers"
	parser "GeneratorAndParser/internal/parser/handlers"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"
)

var levels = []parser.LogLevel{"trace", "debug", "info", "warn", "error"}

var (
	n int
)

func init() {
	flag.IntVar(&n, "n", runtime.NumCPU(), "Number of streams")
}

func main() {
	flag.Parse()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic: %v", r)
		}
	}()

	startTime := time.Now()

	ctx := handlers.SetupContext()

	result, err := parser.Parse(ctx, n, os.Stdin)

	if err != nil {
		fmt.Printf("Error: %v", err)
		os.Exit(1)
	}

	fmt.Println("Log's level statistics:")
	for _, v := range levels {
		fmt.Printf("%s: %d\n", v, result[v])
	}

	elapsedTime := time.Since(startTime)

	fmt.Printf("File was parsed in %d milliseconds.\n", elapsedTime.Milliseconds())
}
