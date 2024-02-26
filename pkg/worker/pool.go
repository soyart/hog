package worker

import (
	"context"
	"sync/atomic"

	"github.com/pkg/errors"
)

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
