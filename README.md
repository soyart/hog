# hog

hog provides a simple task concurrent processor library,
implemented using [Go errgroup](https://pkg.go.dev/golang.org/x/sync/errgroup).

It focuses on simple interface, and opinionated features.

## [Examples](./examples/)

### No output, no error handling, with `hog.Go`:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/soyart/hog"
)

type job int

func processJob(ctx context.Context, task hog.Task) error {
	j, ok := task.Payload.(job)
	if !ok {
		panic("not job")
	}

	fmt.Println("processing:", j)
	time.Sleep(200 * time.Millisecond)
	fmt.Println("done:", j)

	return nil
}

func someTasks() []hog.Task {
	return []hog.Task{
		{Id: "1", Payload: job(1)},
		{Id: "2", Payload: job(2)},
		{Id: "3", Payload: job(3)},
	}
}

func main() {
	var tasks []hog.Task = someTasks()
	ch := make(chan hog.Task)

	// Concurrently but sequentially publishes task
	go func() {
		defer close(ch)

		for i := range tasks {
			ch <- tasks[i]
		}
	}()

	hog.Go(context.Background(), ch, processJob)
}
```
