package worker

import (
	"context"
	"fmt"
	"sync"

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
	fmt.Println("run ignoreErr", ignoreErr)
	errGroup, errGroupCtx := errgroup.WithContext(ctx)

	for task := range tasks {
		go func(task Task, ignoreErr bool) {
			errGroup.Go(func() error {
				err := f(errGroupCtx, task)
				if err != nil {
					if ignoreErr {
						return nil
					}

					return err
				}

				w.increment()
				return nil
			})
		}(task, ignoreErr)
	}

	return errGroup.Wait()
}
