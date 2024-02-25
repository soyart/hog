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

			tasksGroup.Go(func() error {
				err := processFn(tasksCtx, task)
				if err != nil {
					if ignoreErr {
						return nil
					}

					return errors.Wrapf(err, "task_%s", task.Id)
				}

				p.incrProcessed()
				return nil
			})
		}
	}
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

			tasksGroup.Go(func() error {
				result, err := processFn(tasksCtx, task)
				if err != nil {
					if ignoreErr {
						return nil
					}

					return errors.Wrapf(err, "task_%s", task.Id)
				}

				go func() {
					outputs <- result
					p.incrSent()
				}()

				p.incrProcessed()
				return nil
			})
		}
	}
}
