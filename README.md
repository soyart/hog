# hog

hog provides a simple task concurrent processor library,
implemented using [Go errgroup](https://pkg.go.dev/golang.org/x/sync/errgroup).

It focuses on simple interface, and opinionated features.

## Behavior

hog will launch each incoming task to execute in its goroutine,
passing with them the shared context.

As with errgroup, which hog internally uses, once an error happened,
all worker goroutines will get their context canceled.

In addition to wrapping errgroup with channels, hog also provides
error handling strategies via its configuration.

The configuration describes how task errors are handled.

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

	hog.Go(context.Background(), ch, processJob, hog.Config{})
}
```

## Common types

These types are simple contracts for using hog.

At the end of the day, what hog does is just wrapping
some function inside a `func() error` for errgroup.

In hog, we can launch a worker pool by supplying either
`ProcessFn`, `ProcessFnWithOutput`, or `AdapterErrgroupFn`.

This snippet demonstrates core theme in hog:

```go
handleErr := handleErrFn(config)
errGroup, errGroupCtx := errgroup.WithContext(ctx)

// other code

for {
	select {
	//
	// other cases for concurrent error handling
	//

	case task, open := <- ch:
		errGroup.Go(func() error {
			// This inner closure is ProcessFn
			func(ctx context.Context, task hog.Task) {
				err := someProcessFn(ctx, task)
				if err != nil {
					// Error handling according to config
					return handleErr(err)
				}

				return nil
			}
		})
	}
}
```

- `ProcessFn`

Probably the most common form of function signature in Go. This type is central in hog,
because `errgroup` package needs `func() error` to launch for `errgroup.Group.Go`.

As you can see, this function signature does not provide any result return value.

- `ProcessFnWithOutput`

Because `ProcessFn` cannot returns result return value, if we want hog to do something
with outputs, we'd need another form of contract.

This is where `ProcessFnWithOutput` comes in. This type is aliased to `func() (interface{}, error)`,
which provides ample opportunities for use as a processing function.

Internally, hog wraps `ProcessFnWithOutput` inside `ProcessFn` before handing the closure
to errgroup group, and that wrapper functions will send the return value to an outputs channel.

- `AdapterErrgroupFn`

The most complex hog contract. Its main purpose is to capture context and task from hog,
and returns a `func() error` for errgroup to use.

Users may choose to use the adapter is if you'd like your `ProcessFn`
to ignore shared context cancellation.
