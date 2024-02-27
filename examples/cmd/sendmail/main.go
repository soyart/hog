package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/soyart/hog"
)

func main() {
	// email with SomeInt == badInt will err
	numEmails, badInt := 10, 3
	ch := make(chan hog.Task)
	mails := emailTasks(numEmails)

	ctx := context.Background()
	processFunc := processFunc(ctx, badInt)

	// Task producer goroutine
	go func() {
		for i := range mails {
			log.Println("[producer] producing", i)
			ch <- mails[i]
			time.Sleep(200 * time.Millisecond)
		}

		defer log.Println("[producer] closed chan")
		defer close(ch)
	}()

	pool := hog.NewPool("email-servers")

	err := pool.Run(ctx, ch, processFunc, hog.Config{
		FlagHandleErr: hog.FlagHandleErrIgnore,
	})
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

func processFunc(ctx context.Context, badSomeInt int) func(context.Context, hog.Task) error {
	return func(_ context.Context, task hog.Task) error {
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

func emailTasks(n int) []hog.Task {
	jobs := make([]hog.Task, n)

	for i := range jobs {
		jobs[i] = hog.Task{
			Id: fmt.Sprintf("sendmail_%d", i),
			Payload: mail{
				To:      fmt.Sprintf("receiver_%d", i),
				Msg:     fmt.Sprintf("msg_%d", i),
				SomeInt: i,
			},
		}
	}

	return jobs
}
