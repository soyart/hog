package worker

import (
	"context"
	"sync/atomic"

	"github.com/pkg/errors"
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

type Pool struct {
	id        string
	processed uint64
	sent      uint64
}

func NewPool(id string) *Pool {
	return &Pool{id: id}
}

func (p *Pool) Id() string { return p.id }

// Processed may be subject to data race.
// Mutexes were omitted for performance tradeoffs
func (p *Pool) Processed() uint64 { return p.processed }

// ResultsSent may be subject to data race.
// Mutexes were omitted for performance tradeoffs
func (p *Pool) ResultsSent() uint64 { return p.sent }

func (p *Pool) incrProcessed() { atomic.AddUint64(&p.processed, 1) }
func (p *Pool) incrSent()      { atomic.AddUint64(&p.sent, 1) }

// Run consumes values from tasks, calls processFunc on each task,
// and blocks until all tasks are consumed
func (p *Pool) Run(
	ctx context.Context,
	tasks <-chan Task,
	processFn ProcessFn,
	ignoreErr bool,
) error {
	return Run(
		ctx,
		tasks,
		p.wrapErrgroup(processFn, ignoreErr),
	)
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
// `*Pool.Run` can be used in simple cases for better ergonomics.
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

func (p *Pool) wrapErrgroup(
	processFn ProcessFn,
	ignoreErr bool,
) CaptureErrgroupFn {
	return func(ctx context.Context, task Task) func() error {
		return func() error {
			if err := processFn(ctx, task); err != nil {
				if ignoreErr {
					return nil
				}

				return errors.Wrapf(err, "task_%s", task.Id)
			}

			p.incrProcessed()
			return nil
		}
	}
}

func (p *Pool) wrapErrgroupWithOutput(
	processFn ProcessFnWithOutput,
	outputs chan<- interface{},
	ignoreErr bool,
) CaptureErrgroupFn {
	return func(ctx context.Context, task Task) func() error {
		return func() error {
			result, err := processFn(ctx, task)
			if err != nil {
				if ignoreErr {
					return nil
				}

				return errors.Wrapf(err, "task_%s", task.Id)
			}

			p.incrProcessed()

			outputs <- result
			p.incrSent()

			return nil
		}
	}
}
