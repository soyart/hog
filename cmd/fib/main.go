package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"example.com/playground-workers/pkg/worker"
)

const (
	ignoreErr = true
	exitOnErr = !ignoreErr
)

func main() {
	// n = 10 will create 10 tasks for finding nth fibo value (0-9th fibo)
	n := 10
	fibs := fibTasks(n)

	tasks := make(chan worker.Task)
	outputs := make(chan interface{})
	ctx := context.Background()

	// Task producer goroutine
	go func() {
		for i := range fibs {
			log.Println("[producer] producing", i)
			tasks <- fibs[i]

			time.Sleep(700 * time.Millisecond)
		}

		defer log.Println("[producer] closed chan")
		defer close(tasks)
	}()

	pool := worker.NewPool("fib-pool")

	// Result consumer goroutine
	go func() {
		for result := range outputs {
			log.Println("[consumer] result", result)
			log.Println("[consumer] received", pool.ResultsSent())
		}
	}()

	err := pool.RunWithOutputs(ctx, tasks, outputs, processFib, exitOnErr)
	if err != nil {
		log.Println("===== ERROR =====")
		log.Println("exited with error", err.Error())
		log.Println("tasks completed", pool.Processed())

		os.Exit(2)
	}

	log.Println("===== DONE =====")
	log.Println("tasks completed", pool.Processed())
}

func fib(n int) int {
	switch {
	case n <= 0:
		return 0
	case n == 1:
		return 1

	default:
		return fib(n-1) + fib(n-2)
	}
}

func processFib(ctx context.Context, task worker.Task) (interface{}, error) {
	n, ok := task.Payload.(int)
	if !ok {
		panic(fmt.Sprintf("not int"))
	}

	// Fake expensive runtime here, since func fib is recursive
	time.Sleep(100 * time.Millisecond)

	return fib(n), nil
}

func fibTasks(n int) []worker.Task {
	tasks := make([]worker.Task, n)
	for i := 0; i < n; i++ {
		tasks[i] = worker.Task{
			Id:      fmt.Sprintf("fib_%d", n),
			Payload: i,
		}
	}

	return tasks
}
