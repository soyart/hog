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
	numEmails, badInt := 10, 3
	tasks := make(chan worker.Task)
	mails := emailTasks(numEmails)

	ctx := context.Background()
	processFunc := processFunc(ctx, badInt)

	// Task producer goroutine
	go func() {
		for i := range mails {
			log.Println("[producer] producing", i)
			tasks <- mails[i]
			time.Sleep(200 * time.Millisecond)
		}

		defer log.Println("[producer] closed chan")
		defer close(tasks)
	}()

	pool := worker.NewPool()

	err := pool.Run(ctx, tasks, processFunc, exitOnErr)
	if err != nil {
		log.Println("===== ERROR =====")
		log.Println("exited with error", err.Error())
		log.Println("tasks completed", pool.Processed())

		os.Exit(2)
	}

	log.Println("===== DONE =====")
	log.Println("tasks completed", pool.Processed())
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

	time.Sleep(500 * time.Millisecond) // fake IO
	log.Printf("[send-mail] %s\n", b)

	return nil
}

func processFunc(ctx context.Context, badSomeInt int) func(context.Context, worker.Task) error {
	return func(_ context.Context, task worker.Task) error {
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
			Id: fmt.Sprintf("sendmail_%d", i),
			Payload: mail{
				To:      fmt.Sprintf("receiver_%d", i),
				Msg:     fmt.Sprintf("msg_%d", i),
				SomeInt: i,
			},
		}
	}

	return tasks
}
