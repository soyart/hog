package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

func TestRunWithOutputNoErr(t *testing.T) {
	pool := NewPool("fib-test")
	n := 10
	tasks := make(chan Task)
	outputs := make(chan interface{})

	// Task producer goroutine
	go func() {
		fibs := fibTasks(n)

		for i := range fibs {
			log.Println("[producer] producing", i)
			tasks <- fibs[i]
		}

		defer log.Println("[producer] closed chan")
		defer close(tasks)
	}()

	// Worker goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := pool.RunWithOutputs(context.Background(), tasks, outputs, processFib, false)
		if err != nil {
			t.Errorf("unexpected error from RunWithOutputs: %v", err)
		}

		close(outputs)
		t.Log("RunWithOutputs done")
	}()

	// Result consumer goroutine
	var results []int
	for result := range outputs {
		results = append(results, result.(int))
		t.Log("keep result", result)
	}

	t.Log("Waiting wg")

	wg.Wait()

	if processed := pool.Processed(); int(processed) != n {
		t.Fatalf("unexpected number of sent results, expecting %d, got %d", n, processed)
	}

	if sent := pool.ResultsSent(); int(sent) != n {
		t.Fatalf("unexpected number of sent results, expecting %d, got %d", n, sent)
	}

	expecteds := expectedFibs(n)

outer:
	for i := range expecteds {
		expected := expecteds[i]
		for j := range results {
			if results[j] == expected {
				continue outer
			}
		}

		t.Fatalf("missing expected result %d (actual results %v)", expected, results)
	}
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

func processFib(ctx context.Context, task Task) (interface{}, error) {
	time.Sleep(100 * time.Millisecond)
	n, ok := task.Payload.(int)
	if !ok {
		panic(fmt.Sprintf("not int"))
	}

	return fib(n), nil
}

func fibTasks(n int) []Task {
	tasks := make([]Task, n)
	for i := int(0); i < n; i++ {
		tasks[i] = Task{
			Id:      fmt.Sprintf("fib_%d", n),
			Payload: i,
		}
	}

	return tasks
}

func expectedFibs(n int) []int {
	expecteds := make([]int, n)
	for i := int(0); i < n; i++ {
		expecteds[i] = fib(i)
	}

	return expecteds
}
