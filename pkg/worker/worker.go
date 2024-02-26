package worker

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type (
	ProcessFn           = func(context.Context, Task) error
	ProcessFnWithOutput = func(context.Context, Task) (interface{}, error)
	CaptureErrgroupFn   = func(context.Context, Task) func() error // Used internally to capture context and task to a closure for errgroup
)

type Task struct {
	Id      string
	Payload interface{}
}

// RunWithOutputs consumes each task from tasks,
// maps it to a result using processFunc,
// and send the result back to outputs.
func (p *Pool) RunWithOutputs(
	ctx context.Context,
	tasks <-chan Task,
	outputs chan<- interface{},
	processFn ProcessFnWithOutput,
	ignoreErr bool,
) error {
	return Run(
		ctx,
		tasks,
		p.wrapErrgroupWithOutput(processFn, outputs, ignoreErr),
	)
}

// Run is a minimal template for concurrently executing each Task
// until all tasks are done (channel closed).
//
// `Process` or `*Pool.Run` can be used in simple cases for better ergonomics.
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

func Process(ctx context.Context, tasks <-chan Task, processFn ProcessFn) error {
	return Run(ctx, tasks, WrapErrGroupFn(processFn))
}

func WrapErrGroupFn(processFn ProcessFn) CaptureErrgroupFn {
	return func(ctx context.Context, task Task) func() error {
		return func() error {
			return processFn(ctx, task)
		}
	}
}
