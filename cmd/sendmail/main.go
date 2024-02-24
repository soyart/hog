package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"example.com/playground-workers/pkg/worker"
)

func main() {
	tasks := make(chan worker.Task)
	emailTasks := emailTasks(5)

	go func() {
		for i := range emailTasks {
			tasks <- emailTasks[i]
			time.Sleep(time.Second * time.Duration(i))
		}

		close(tasks)
	}()

	ctx := context.Background()
	pool := worker.NewPool(tasks, process)

	err := pool.Run(ctx)
	if err != nil {
		panic(err)
	}
}

type mail struct {
	To  string `json:"to"`
	Msg string `json:"msg"`
}

func process(ctx context.Context, task worker.Task) error {
	email, ok := task.Payload.(mail)
	if !ok {
		panic("payload not mail")
	}

	return sendMail(email)
}

func sendMail(m mail) error {
	b, err := json.Marshal(m)
	if err != nil {
		panic("json marshal error")
	}

	fmt.Printf("[send-mail]: %s\n", b)

	return nil
}

func emailTasks(n int) []worker.Task {
	tasks := make([]worker.Task, n)

	for i := range tasks {
		tasks[i] = worker.Task{
			Id: fmt.Sprintf("task id %d", i),
			Payload: mail{
				To:  fmt.Sprintf("receiver_%d", i),
				Msg: fmt.Sprintf("msg_%d", i),
			},
		}
	}

	return tasks
}
