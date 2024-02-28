package main

import (
	"context"

	"github.com/soyart/hog"
	"github.com/soyart/hog/examples/pkg/job"
)

func main() {
	var tasks []hog.Task = job.SomeTasks()
	ch := make(chan hog.Task)

	// Concurrently but sequentially publishes task
	go func() {
		defer close(ch)

		for i := range tasks {
			ch <- tasks[i]
		}
	}()

	hog.Go(context.Background(), ch, job.ProcessJob, hog.Config{})
}
