package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	"example.com/playground-workers/pkg/worker"
)

const (
	ignoreErr = true
	exitOnErr = !ignoreErr
)

func main() {
	// email with SomeInt == badInt will err
	numEmails, badInt := 5, 3
	tasks := make(chan worker.Task)
	emailTasks := emailTasks(numEmails)

	ctx := context.Background()
	processFunc := processFunc(ctx, badInt)

	// Task producer goroutine
	go func() {
		for i := range emailTasks {
			fmt.Println("[producer] producing", i)
			tasks <- emailTasks[i]
			time.Sleep(500 * time.Millisecond)
		}

		defer fmt.Println("[producer] closed chan")
		defer close(tasks)
	}()

	pool := worker.NewPool()

	err := pool.Run(ctx, tasks, processFunc, ignoreErr)
	if err != nil {
		fmt.Println("===== ERROR =====")
		fmt.Println("exited with error", err.Error())
		fmt.Println("tasks completed", pool.Processed())
		os.Exit(2)
	}

	fmt.Println("===== DONE =====")
	fmt.Println("tasks completed", pool.Processed())
}

type mail struct {
	To      string `json:"to"`
	Msg     string `json:"msg"`
	SomeInt int    `json:"some_int"`
}

func sendMail(m mail) error {
	b, err := json.Marshal(m)
	if err != nil {
		panic("json marshal error")
	}

	fmt.Printf("[send-mail] %s\n", b)
	return nil
}

func processFunc(ctx context.Context, badSomeInt int) func(context.Context, worker.Task) error {
	if badSomeInt < 0 {
		return func(ctx context.Context, task worker.Task) error {
			err := ctx.Err()
			if err != nil {
				log.Println("got context error", err.Error())
				return nil
			}

			email, ok := task.Payload.(mail)
			if !ok {
				panic("not email: " + reflect.TypeOf(email).String())
			}

			log.Println("processing", email.SomeInt)
			return sendMail(email)
		}
	}

	return func(ctx context.Context, task worker.Task) error {
		err := ctx.Err()
		if err != nil {
			log.Println("got context error", err.Error())
			return nil
		}

		email, ok := task.Payload.(mail)
		if !ok {
			panic("not email: " + reflect.TypeOf(email).String())
		}

		log.Println("processing", email.SomeInt)

		if email.SomeInt == badSomeInt {
			log.Println("got error!", email.SomeInt)
			return fmt.Errorf("errConst %d, email %+v", badSomeInt, email)
		}

		return sendMail(email)
	}
}

func emailTasks(n int) []worker.Task {
	tasks := make([]worker.Task, n)

	for i := range tasks {
		tasks[i] = worker.Task{
			Id: fmt.Sprintf("task id %d", i),
			Payload: mail{
				To:      fmt.Sprintf("receiver_%d", i),
				Msg:     fmt.Sprintf("msg_%d", i),
				SomeInt: i,
			},
		}
	}

	return tasks
}
