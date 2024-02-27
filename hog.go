package hog

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"
)

const (
	FlagHandleErrDefault    FlagHandleErr = iota // Fails on first error
	FlagHandleErrIgnore                          // Never fails on any errors
	FlagHandleErrIgnoreSome                      // Never fails on some errors
)

type (
	FlagHandleErr       = uint8
	ProcessFn           = func(context.Context, Task) error
	ProcessFnWithOutput = func(context.Context, Task) (interface{}, error)
	AdapterErrgroupFn   = func(context.Context, Task) func() error // Used internally to capture context and task to a closure for errgroup
)

// Task represents hog unit of task.
type Task struct {
	Id      string
	Payload interface{}
}

type ConfigErrgroup struct {
	Limit           bool
	LimitGoroutines int
}

type Config struct {
	Errgroup ConfigErrgroup

	FlagHandleErr FlagHandleErr // Error handling flag
	IgnoreErrs    []error       // Errors to ignore - if set, flag will be FlagHandleErrIgnoreSome
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
	conf Config,
) error {
	return Hog(ctx, tasks, adapterFn(processFn, conf), conf)
}

// Hog is similar to Go in execution, but its argument is a function
// used to capture context and task into a closure accepted by errgroup.
//
// It is exported in case callers want to implement the capture themselves,
// e.g. for ignoring the local context created in Hog.
func Hog(
	ctx context.Context,
	tasks <-chan Task,
	capture AdapterErrgroupFn,
	conf Config,
) error {
	tasksGroup, tasksCtx := errgroup.WithContext(ctx)
	if conf.Errgroup.Limit {
		tasksGroup.SetLimit(conf.Errgroup.LimitGoroutines)
	}

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

func handleErrFn(conf Config) func(error) error {
	switch {
	case len(conf.IgnoreErrs) > 0 || conf.FlagHandleErr == FlagHandleErrIgnoreSome:
		return func(err error) error {
			for i := range conf.IgnoreErrs {
				if errors.Is(err, conf.IgnoreErrs[i]) {
					return nil
				}
			}

			return err
		}

	case conf.FlagHandleErr == FlagHandleErrDefault:
		return func(err error) error { return err }

	case conf.FlagHandleErr == FlagHandleErrIgnore:
		return func(_ error) error { return nil }

	default:
		return nil
	}
}

func adapterFn(processFn ProcessFn, conf Config) AdapterErrgroupFn {
	handleErr := handleErrFn(conf)

	return func(ctx context.Context, task Task) func() error {
		return func() error {
			return handleErr(processFn(ctx, task))
		}
	}
}
