package email

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/soyart/hog"
)

type Mail struct {
	To      string `json:"to"`
	Msg     string `json:"msg"`
	SomeInt int    `json:"some_int"`
}

func SendMail(m Mail) error {
	b, err := json.Marshal(m)
	if err != nil {
		panic("json marshal error")
	}

	time.Sleep(500 * time.Millisecond) // fake IO
	log.Printf("[send-mail] %s\n", b)

	return nil
}

func ProcessFuncBadInt(ctx context.Context, badInt int) func(context.Context, hog.Task) error {
	return func(_ context.Context, task hog.Task) error {
		email, ok := task.Payload.(Mail)
		if !ok {
			panic("not email: " + reflect.TypeOf(email).String())
		}

		log.Println("processing", email.SomeInt)

		if email.SomeInt == badInt {
			log.Println("got error!", email.SomeInt)
			return fmt.Errorf("errConst %d, email %+v", badInt, email)
		}

		return SendMail(email)
	}
}

func TasksEmail(n int) []hog.Task {
	jobs := make([]hog.Task, n)

	for i := range jobs {
		jobs[i] = hog.Task{
			Id: fmt.Sprintf("sendmail_%d", i),
			Payload: Mail{
				To:      fmt.Sprintf("receiver_%d", i),
				Msg:     fmt.Sprintf("msg_%d", i),
				SomeInt: i,
			},
		}
	}

	return jobs
}
