package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/soyart/hog"
)

type Job int

func ProcessJob(ctx context.Context, task hog.Task) error {
	j, ok := task.Payload.(Job)
	if !ok {
		panic("not job")
	}

	fmt.Println("processing:", j)
	time.Sleep(200 * time.Millisecond)
	fmt.Println("done:", j)

	return nil
}

func SomeTasks() []hog.Task {
	return []hog.Task{
		{Id: "1", Payload: Job(1)},
		{Id: "2", Payload: Job(2)},
		{Id: "3", Payload: Job(3)},
	}
}

func ProcessJobNoOutput(store []Job) hog.ProcessFn {
	mut := new(sync.Mutex)
	return func(ctx context.Context, task hog.Task) error {
		j, ok := task.Payload.(Job)
		if !ok {
			panic("not job")
		}

		mut.Lock()
		defer mut.Unlock()

		store = append(store, j)

		return nil
	}
}

func RangeJobs(start, end int) []hog.Task {
	if start > end {
		panic("bad start/end")
	}

	tasks := make([]hog.Task, end-start)
	for i := start; i < end; i++ {
		tasks[i] = hog.Task{
			Id:      fmt.Sprintf("job_%d", i),
			Payload: Job(i),
		}
	}

	return tasks
}
