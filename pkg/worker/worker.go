package worker

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
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

func (w *Pool) Run(
	ctx context.Context,
	tasks <-chan Task,
	f func(ctx context.Context, task Task) error,
	ignoreErr bool,
) error {
	tasksGroup, tasksCtx := errgroup.WithContext(ctx)
	done := make(chan struct{})

	for {
		select {
		// All tasks ok
		case <-done:
			return nil

		// Some task failed (i.e. context canceled by errGroup)
		case <-tasksCtx.Done():
			return tasksGroup.Wait()

		case task, open := <-tasks:
			if !open {
				go func() {
					done <- struct{}{}
				}()

				continue
			}

			func(task Task, ignoreErr bool) {
				tasksGroup.Go(func() error {
					err := f(tasksCtx, task)
					if err != nil {
						if ignoreErr {
							return nil
						}

						return errors.Wrapf(err, "task_%s", task.Id)
					}

					w.increment()
					return nil
				})
			}(task, ignoreErr)
		}
	}
}
