package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/soyart/hog"
	"github.com/soyart/hog/examples/pkg/fib"
)

func main() {
	// n = 10 will create 10 tasks for finding nth fibo value (0-9th fibo)
	n := 10
	fibs := fib.IntTasks(n)

	tasks := make(chan hog.Task)
	outputs := make(chan interface{})
	ctx := context.Background()

	// Task producer goroutine
	go func() {
		defer log.Println("[producer] closed chan")
		defer close(tasks)

		for i := range fibs {
			log.Println("[producer] producing", i)
			tasks <- fibs[i]

			time.Sleep(200 * time.Millisecond)
		}
	}()

	pool := hog.NewPool("fib-pool")

	// Result consumer goroutine
	go func() {
		for result := range outputs {
			log.Println("[consumer] result", result)
			log.Println("[consumer] received", pool.ResultsSent())
		}
	}()

	// processFn := fib.ProcessFib
	processFn := fib.ProcessFibErrIfEq(3, 6) // errors on 3rd and 6th jobs

	err := pool.RunWithOutputs(ctx, tasks, outputs, processFn, hog.Config{
		// IgnoreErrs: []error{fib.ErrFib},
		// FlagHandleErr: hog.FlagHandleErrIgnore,
	})
	if err != nil {
		log.Println("===== ERROR =====")
		log.Println("exited with error", err.Error())
		log.Println("tasks completed", pool.Processed())

		os.Exit(2)
	}

	log.Println("===== DONE =====")
	log.Println("tasks completed", pool.Processed())
}
