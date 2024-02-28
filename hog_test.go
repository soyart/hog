package hog_test

import (
	"context"
	"testing"

	"github.com/soyart/hog"
	"github.com/soyart/hog/examples/pkg/job"
)

func TestGo_Job_NoErr(t *testing.T) {
	n := 10
	jobs := job.RangeJobs(0, n)

	var store []job.Job
	processFn := job.ProcessJobNoOutput(store)
	ch := make(chan hog.Task)

	go func() {
		defer close(ch)

		for i := range jobs {
			ch <- jobs[i]
		}
	}()

	err := hog.Go(context.Background(), ch, processFn, hog.Config{})
	if err != nil {
		t.Error("unexpected error", err)
	}

	for i := range store {
		expected := store[i]
		for j := range jobs {
			if expected == jobs[j].Payload.(job.Job) {
				continue
			}
		}

		t.Log("store", store)
		t.Fatal("missing job value", expected)
	}
}
