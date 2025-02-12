package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

func fatal(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "fatal: %s\n", err)
	os.Exit(1)
}

type (
	xrefArgs struct {
		CollectionID int32 `json:"collection_id" river:"unique"`
	}
	xrefWorker struct{ river.WorkerDefaults[xrefArgs] }
)

func (xrefArgs) Kind() string { return "xref" }

func (w *xrefWorker) Work(ctx context.Context, job *river.Job[xrefArgs]) error {
	out, err := exec.Command("python3", "./xref.py", strconv.Itoa(int(job.Args.CollectionID))).CombinedOutput()
	if err != nil {
		if len(out) > 10_000 {
			out = append(out[:10_000], []byte("…")...)
		}
		return fmt.Errorf("%w\n\n%s", err, out)
	}
	return nil
}

func main() {
	out, err := exec.Command("python3", "./xref.py", "0").CombinedOutput()
	if err != nil {
		fatal(fmt.Errorf("%w\n\n%s", err, out))
	}

	ctx := context.Background()

	workers := river.NewWorkers()
	river.AddWorker(workers, &xrefWorker{})

	dbpool, err := pgxpool.New(ctx, "postgres://aleph:aleph@127.0.0.1/aleph_ftm")
	fatal(err)

	rc, err := river.NewClient(riverpgxv5.New(dbpool), &river.Config{
		Workers:     workers,
		MaxAttempts: 2,
		JobTimeout:  8 * time.Hour,
		Queues:      map[string]river.QueueConfig{"xref": {MaxWorkers: 1}},
	})
	fatal(err)

	err = rc.Start(ctx)
	fatal(err)
	fmt.Println("ready")
	<-make(chan struct{})
}
