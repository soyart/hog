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

func (p *Pool) Id() string          { return p.id }
func (p *Pool) Processed() uint64   { return p.processed }
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
	return p.run(
		ctx,
		tasks,
		p.wrapProcessFn(ctx, processFn, ignoreErr),
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
	return p.run(
		ctx,
		tasks,
		p.wrapProcessFnWithOutputs(ctx, processFn, outputs, ignoreErr),
	)
}

// run is a template for concurrently executing each Task from task with f
// until all tasks are done (channel closed)
func (p *Pool) run(
	ctx context.Context,
	tasks <-chan Task,
	f func(Task) func() error,
) error {
	tasksGroup, tasksCtx := errgroup.WithContext(ctx)
	doneSignal := make(chan struct{})

	for {
		select {
		// All tasks ok
		case <-doneSignal:
			return tasksGroup.Wait()

		// Some task failed (i.e. context canceled by errGroup)
		case <-tasksCtx.Done():
			return tasksGroup.Wait()

		case task, open := <-tasks:
			if !open {
				go func() {
					doneSignal <- struct{}{}
				}()

				continue
			}

			tasksGroup.Go(f(task))
		}
	}
}

func (p *Pool) wrapProcessFn(
	ctx context.Context,
	processFn ProcessFn,
	ignoreErr bool,
) func(Task) func() error {
	return func(task Task) func() error {
		return func() error {
			err := processFn(ctx, task)
			if err != nil {
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

func (p *Pool) wrapProcessFnWithOutputs(
	ctx context.Context,
	processFn ProcessFnWithOutput,
	outputs chan<- interface{},
	ignoreErr bool,
) func(Task) func() error {
	return func(task Task) func() error {
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
