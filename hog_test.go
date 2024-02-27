package hog_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/soyart/hog"
)

type job int

func processJobNoOutput(store []job) hog.ProcessFn {
	mut := new(sync.Mutex)
	return func(ctx context.Context, task hog.Task) error {
		j, ok := task.Payload.(job)
		if !ok {
			panic("not job")
		}

		mut.Lock()
		defer mut.Unlock()

		store = append(store, j)

		return nil
	}
}

func rangeJobs(start, end int) []hog.Task {
	if start > end {
		panic("bad start/end")
	}

	tasks := make([]hog.Task, end-start)
	for i := start; i < end; i++ {
		tasks[i] = hog.Task{
			Id:      fmt.Sprintf("job_%d", i),
			Payload: job(i),
		}
	}

	return tasks
}

func TestGo_Job_NoErr(t *testing.T) {
	n := 10
	jobs := rangeJobs(0, n)

	var store []job
	processFn := processJobNoOutput(store)
	ch := make(chan hog.Task)

	go func() {
		defer close(ch)

		for i := range jobs {
			ch <- jobs[i]
		}
	}()

	err := hog.Go(context.Background(), ch, processFn)
	if err != nil {
		t.Error("unexpected error", err)
	}

	for i := range store {
		expected := store[i]
		for j := range jobs {
			if expected == jobs[j].Payload.(job) {
				continue
			}
		}

		t.Log("store", store)
		t.Fatal("missing job value", expected)
	}
}
