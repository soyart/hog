package worker

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

type Task struct {
	Id      string
	Payload interface{}
}

type Pool struct {
	f         func(ctx context.Context, task Task) error
	tasks     <-chan Task
	m         *sync.Mutex
	processed int
}

func NewPool(
	tasks <-chan Task,
	f func(ctx context.Context, task Task) error,
) *Pool {
	return &Pool{
		f:         f,
		tasks:     tasks,
		m:         &sync.Mutex{},
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

func (w *Pool) Run(ctx context.Context) error {
	errGroup, errGroupCtx := errgroup.WithContext(ctx)

	for task := range w.tasks {
		func(task Task) {
			errGroup.Go(func() error {
				return w.work(errGroupCtx, task)
			})
		}(task)
	}

	return errGroup.Wait()
}

func (w *Pool) work(ctx context.Context, task Task) error {
	err := w.f(ctx, task)
	if err != nil {
		return err
	}

	w.increment()

	return nil
}
