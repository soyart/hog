package hog

import (
	"context"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	FlagHandleErrDefault    FlagHandleErr = iota // Fails on first error
	FlagHandleErrIgnore                          // Never fails on any errors
	FlagHandleErrIgnoreSome                      // Never fails on some errors
)

type (
	FlagHandleErr       uint8
	ProcessFn           func(context.Context, Task) error
	ProcessFnWithOutput func(context.Context, Task) (interface{}, error)
	AdapterErrgroupFn   func(context.Context, Task) func() error // AdapterFn captures errgroup context and task, and returns a closure used in errgroup.Group.Go
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

// Hog is the base for all hog functions.
// It accept AdapterFn for maximum flexibility.
// If you have a ProcessFn or ProcessFnWithOutput,
// try using adapters or use Go or GoWithOutputs instead.
func Hog(
	ctx context.Context,
	tasks <-chan Task,
	adapter AdapterErrgroupFn,
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
			tasksGroup.Go(adapter(tasksCtx, task))
		}
	}
}

func Go(
	ctx context.Context,
	tasks <-chan Task,
	processFn ProcessFn,
	conf Config,
) error {
	return Hog(ctx, tasks, AdapterFn(processFn, conf), conf)
}

func GoWithOutputs(
	ctx context.Context,
	tasks <-chan Task,
	outputs chan<- interface{},
	processFn ProcessFnWithOutput,
	conf Config,
) error {
	return Hog(ctx, tasks, AdapterFnWithOutput(processFn, outputs, conf), conf)
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

func AdapterFn(processFn ProcessFn, conf Config) AdapterErrgroupFn {
	handleErr := handleErrFn(conf)
	return func(ctx context.Context, task Task) func() error {
		return func() error {
			return handleErr(processFn(ctx, task))
		}
	}
}

func AdapterFnWithOutput(
	processFn ProcessFnWithOutput,
	outputs chan<- interface{},
	conf Config,
) AdapterErrgroupFn {
	handleErr := handleErrFn(conf)
	return func(ctx context.Context, task Task) func() error {
		return func() error {
			result, err := processFn(ctx, task)
			if err != nil {
				return errors.Wrapf(handleErr(err), "task_%s", task.Id)
			}

			outputs <- result

			return nil
		}
	}
}
