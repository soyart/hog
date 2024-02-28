package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/soyart/hog"
	"github.com/soyart/hog/examples/pkg/email"
)

func main() {
	// email with SomeInt == badInt will err
	numEmails, badInt := 10, 3
	ch := make(chan hog.Task)
	mails := email.TasksEmail(numEmails)

	ctx := context.Background()
	processFunc := email.ProcessFuncBadInt(ctx, badInt)

	// Task producer goroutine
	go func() {
		defer log.Println("[producer] closed chan")
		defer close(ch)

		for i := range mails {
			log.Println("[producer] producing", i)
			ch <- mails[i]
			time.Sleep(200 * time.Millisecond)
		}
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
