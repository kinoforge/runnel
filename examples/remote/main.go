package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/remote"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	c := remote.New(os.Getenv("RUNNEL_URL"), os.Getenv("RUNNEL_KEY"), os.Getenv("RUNNEL_NAMESPACE"))
	w := remote.NewWorker(c, runnel.WorkerConfig{Queues: []string{"thumbnails"}, Concurrency: 4})
	w.HandleFunc("image.resize", func(ctx context.Context, j *runnel.Job) error {
		var in struct {
			URL   string `json:"url"`
			Width int    `json:"width"`
		}
		if err := j.Bind(&in); err != nil {
			return runnel.Fail(err)
		}
		fmt.Println("resizing", in.URL, "to", in.Width)
		return j.SetResult(map[string]string{"url": in.URL + "?w=" + fmt.Sprint(in.Width)})
	})
	if err := w.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
