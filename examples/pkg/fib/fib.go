package fib

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/soyart/hog"
)

var ErrFib = errors.New("fib error")

func Fib(n int) int {
	switch {
	case n <= 0:
		return 0
	case n == 1:
		return 1

	default:
		return Fib(n-1) + Fib(n-2)
	}
}

func ProcessFib(ctx context.Context, task hog.Task) (interface{}, error) {
	n, ok := task.Payload.(int)
	if !ok {
		panic(fmt.Sprintf("not int"))
	}

	// Fake expensive runtime here, since func fib is recursive
	// time.Sleep(100 * time.Millisecond)

	return Fib(n), nil
}

func ProcessFibErrIfEq(badInts ...int) hog.ProcessFnWithOutput {
	return func(ctx context.Context, task hog.Task) (interface{}, error) {
		n, ok := task.Payload.(int)
		if !ok {
			panic(fmt.Sprintf("not int"))
		}

		// Fake expensive runtime here, since func fib is recursive
		time.Sleep(400 * time.Millisecond)

		for i := range badInts {
			bad := badInts[i]
			if n == bad {
				return nil, errors.Wrapf(ErrFib, "bad int %d", n)
			}
		}

		return Fib(n), nil
	}
}

func IntTasks(n int) []hog.Task {
	tasks := make([]hog.Task, n)
	for i := 0; i < n; i++ {
		tasks[i] = hog.Task{
			Id:      fmt.Sprintf("fib_%d", i),
			Payload: i,
		}
	}

	return tasks
}
