package hog

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type (
	ProcessFn           = func(context.Context, Task) error
	ProcessFnWithOutput = func(context.Context, Task) (interface{}, error)
	CaptureErrgroupFn   = func(context.Context, Task) func() error // Used internally to capture context and task to a closure for errgroup
)

// Task represents hog unit of task.
type Task struct {
	Id      string
	Payload interface{}
}

// Go receives values from tasks, and calls processFn on each task.
// Go exits when the first call to processFn returns non-nil error.
//
// The context passed to processFn is a local one, created with errgroup
// on the argument ctx.
func Go(
	ctx context.Context,
	tasks <-chan Task,
	processFn ProcessFn,
) error {
	return Run(ctx, tasks, WrapErrGroupFn(processFn))
}

// Run is similar to Go in execution, but its argument is a function
// used to capture context and task into a closure accepted by errgroup.
//
// It is exported in case callers want to implement the capture themselves,
// e.g. for ignoring the local context created in Run.
func Run(
	ctx context.Context,
	tasks <-chan Task,
	capture CaptureErrgroupFn,
) error {
	tasksGroup, tasksCtx := errgroup.WithContext(ctx)
	allDone := make(chan struct{})

	for {
		select {
		// All launched tasks are doing ok so far,
		// and channel tasks has been closed
		case <-allDone:
			return tasksGroup.Wait()

		// Some task failed (i.e. context canceled by errGroup)
		case <-tasksCtx.Done():
			return tasksGroup.Wait()

		case task, open := <-tasks:
			if !open {
				go func() {
					allDone <- struct{}{}
				}()

				continue
			}

			// We use errgroup's context here in capture
			// to be able to cancel all goroutines on first non-nil err
			tasksGroup.Go(capture(tasksCtx, task))
		}
	}
}

// WrapErrGroupFn can be used to wrap a ProcessFn into CaptureErrgroupFn
func WrapErrGroupFn(processFn ProcessFn) CaptureErrgroupFn {
	return func(ctx context.Context, task Task) func() error {
		return func() error {
			return processFn(ctx, task)
		}
	}
}
