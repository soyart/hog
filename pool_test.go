package hog_test

import (
	"context"
	"errors"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/soyart/hog"
	"github.com/soyart/hog/examples/pkg/fib"
)

func TestPool_RunWithOutputs_WithErr(t *testing.T) {
	type testCaseFibIgnoreErr struct {
		config        hog.Config
		tasksFib      []hog.Task
		processFn     hog.ProcessFnWithOutput
		expectedError bool
	}

	tests := []testCaseFibIgnoreErr{
		{
			tasksFib:      fib.IntTasks(10),
			processFn:     fib.ProcessFibErrIfEq(3),
			expectedError: true,
		},
		{
			config: hog.Config{
				FlagHandleErr: hog.FlagHandleErrIgnore,
			},
			tasksFib:      fib.IntTasks(10),
			processFn:     fib.ProcessFibErrIfEq(3),
			expectedError: false,
		},
		{
			config: hog.Config{
				FlagHandleErr: hog.FlagHandleErrIgnore,
				IgnoreErrs:    []error{errors.New("foo")}, // this overwrites FlagHandleErr above
			},
			tasksFib:      fib.IntTasks(10),
			processFn:     fib.ProcessFibErrIfEq(3),
			expectedError: true,
		},
		{
			config: hog.Config{
				IgnoreErrs: []error{errors.New("fib error")}, // not fib.Err..
			},
			tasksFib:      fib.IntTasks(10),
			processFn:     fib.ProcessFibErrIfEq(3),
			expectedError: true,
		},
		{
			config: hog.Config{
				IgnoreErrs: []error{fib.ErrFib},
			},
			tasksFib:      fib.IntTasks(10),
			processFn:     fib.ProcessFibErrIfEq(3),
			expectedError: false,
		},
	}

	for i := range tests {
		testCase := &tests[i]

		tasks := make(chan hog.Task)
		outputs := make(chan interface{})
		ctx := context.Background()

		// Task producer goroutine
		go func() {
			defer log.Println("[producer] closed chan")
			defer close(tasks)

			for i := range testCase.tasksFib {
				log.Println("[producer] producing", i)
				tasks <- testCase.tasksFib[i]

				time.Sleep(100 * time.Millisecond)
			}
		}()

		pool := hog.NewPool("fib-pool")

		// Result consumer goroutine
		go func() {
			for result := range outputs {
				log.Println("[consumer] result", result)
				log.Println("[consumer] received", pool.ResultsSent())

				time.Sleep(120 * time.Millisecond)
			}
		}()

		err := pool.RunWithOutputs(ctx, tasks, outputs, testCase.processFn, testCase.config)

		if err != nil && !testCase.expectedError {
			t.Errorf("testCase: %+v", testCase)
			t.Fatalf("Unexpected error")
		}

		if err == nil && testCase.expectedError {
			t.Errorf("testCase: %+v", testCase)
			t.Fatalf("Unexpected nil error")
		}
	}
}

func TestPool_RunWithOutput_NoErr(t *testing.T) {
	pool := hog.NewPool("fib-test")
	n := 10
	tasks := make(chan hog.Task)
	outputs := make(chan interface{})

	// Task producer goroutine
	go func() {
		defer log.Println("[producer] closed chan")
		defer close(tasks)

		fibs := fib.IntTasks(n)

		for i := range fibs {
			log.Println("[producer] producing", i)
			tasks <- fibs[i]
		}
	}()

	// Worker goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer t.Log("RunWithOutputs done")
		defer close(outputs)
		defer wg.Done()

		processFn := fib.ProcessFib
		err := pool.RunWithOutputs(context.Background(), tasks, outputs, processFn, hog.Config{})
		if err != nil {
			t.Errorf("unexpected error from RunWithOutputs: %v", err)
		}
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

func expectedFibs(n int) []int {
	expecteds := make([]int, n)
	for i := int(0); i < n; i++ {
		expecteds[i] = fib.Fib(i)
	}

	return expecteds
}
