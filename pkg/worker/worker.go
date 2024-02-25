package worker

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type (
	Processor           = func(context.Context, Task) error
	ProcessorWithOutpus = func(context.Context, Task) (interface{}, error)
)

type Task struct {
	Id      string
	Payload interface{}
}

type Pool struct {
	m         *sync.Mutex
	processed int
}

func NewPool() *Pool {
	return &Pool{
		m:         new(sync.Mutex),
		processed: 0,
	}
}

func (w *Pool) Processed() int {
	w.m.Lock()
	defer w.m.Unlock()

	return w.processed
}

func (w *Pool) increment() {
	w.m.Lock()
	defer w.m.Unlock()

	w.processed++
}

// Run consumes values from tasks, calls processFunc on each task,
// and blocks until all tasks are consumed
func (w *Pool) Run(
	ctx context.Context,
	tasks <-chan Task,
	processFunc Processor,
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
				err := processFunc(tasksCtx, task)
				if err != nil {
					if ignoreErr {
						return nil
					}

					return errors.Wrapf(err, "task_%s", task.Id)
				}

				w.increment()
				return nil
			})
		}
	}
}

func (w *Pool) RunWithOutputs(
	ctx context.Context,
	tasks <-chan Task,
	outputs chan<- interface{},
	processFunc ProcessorWithOutpus,
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
				result, err := processFunc(tasksCtx, task)
				if err != nil {
					if ignoreErr {
						return nil
					}

					return errors.Wrapf(err, "task_%s", task.Id)
				}

				go func() {
					outputs <- result
				}()

				w.increment()
				return nil
			})
		}
	}
}
