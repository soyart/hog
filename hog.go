package hog

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"
)

const (
	FlagDefaultErr Flag = iota // Fails on first error
	FlagIgnoreErr              // Never fails on any errors
	FlagIgnoreErrs             // Never fails on some errors
)

type (
	Flag                = uint8
	ProcessFn           = func(context.Context, Task) error
	ProcessFnWithOutput = func(context.Context, Task) (interface{}, error)
	AdapterErrgroupFn   = func(context.Context, Task) func() error // Used internally to capture context and task to a closure for errgroup
)

// Task represents hog unit of task.
type Task struct {
	Id      string
	Payload interface{}
}

type Config struct {
	Flag       Flag
	IgnoreErrs []error
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
	return Run(ctx, tasks, adapterFn(processFn, Config{}))
}

func GoConfig(
	ctx context.Context,
	tasks <-chan Task,
	processFn ProcessFn,
	conf Config,
) error {
	return Run(ctx, tasks, adapterFn(processFn, conf))
}

// Run is similar to Go in execution, but its argument is a function
// used to capture context and task into a closure accepted by errgroup.
//
// It is exported in case callers want to implement the capture themselves,
// e.g. for ignoring the local context created in Run.
func Run(
	ctx context.Context,
	tasks <-chan Task,
	capture AdapterErrgroupFn,
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

func handleErr(conf Config) func(error) error {
	switch conf.Flag {
	case FlagDefaultErr:
		return func(err error) error { return err }

	case FlagIgnoreErr:
		return func(_ error) error { return nil }

	case FlagIgnoreErrs:
		return func(err error) error {
			for i := range conf.IgnoreErrs {
				if errors.Is(err, conf.IgnoreErrs[i]) {
					return nil
				}
			}

			return err
		}

	default:
		return nil
	}
}

func adapterFn(processFn ProcessFn, conf Config) AdapterErrgroupFn {
	handleErr := handleErr(conf)

	return func(ctx context.Context, task Task) func() error {
		return func() error {
			err := processFn(ctx, task)
			return handleErr(err)
		}
	}
}
