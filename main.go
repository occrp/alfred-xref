package main

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"zgo.at/slog_align"
	"zgo.at/zli"
)

type (
	xrefArgs struct {
		CollectionID int32 `json:"collection_id" river:"unique"`
	}
	xrefWorker struct {
		river.WorkerDefaults[xrefArgs]
		Dev, Debug bool
	}
)

func (xrefArgs) Kind() string { return "xref-compute" }

func (w *xrefWorker) Work(ctx context.Context, job *river.Job[xrefArgs]) error {
	args := []string{"./xref.py"}
	if w.Dev {
		args = append(args, "-dev")
	}
	if w.Debug {
		args = append(args, "-debug")
	}
	cmd := exec.Command("python3", append(args, strconv.Itoa(int(job.Args.CollectionID)))...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		return err
	}
	return nil
}

const usage = `
Run xref.py for every job in the river "xref" queue.

Flags:
    -dev        Format logs as plain text, rather than JSON.
    -debug      Enable debug logs.
`

func main() {
	f := zli.NewFlags(os.Args)
	var (
		dev   = f.Bool(false, "dev")
		debug = f.Bool(false, "debug")
	)
	err := f.Parse()
	zli.F(err)

	lvl := slog.LevelInfo
	if debug.Bool() {
		lvl = slog.LevelDebug
	}
	var lh slog.Handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	if dev.Bool() {
		lh = slog_align.NewAlignedHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	}
	log := slog.New(lh)

	out, err := exec.Command("python3", "./xref.py", "0").CombinedOutput()
	if err != nil {
		log.Error("xref.py failed", "err", err, "out", string(out))
		os.Exit(1)
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &xrefWorker{Dev: dev.Bool(), Debug: debug.Bool()})

	conn, ok := os.LookupEnv("ALFRED_DB_FTM")
	if !ok {
		conn = "postgres://aleph:aleph@127.0.0.1/aleph_ftm"
	}
	dbpool, err := pgxpool.New(context.Background(), conn)
	if err != nil {
		log.Error("connecting to PostgreSQL", "err", err, "conn", conn)
		os.Exit(1)
	}

	rc, err := river.NewClient(riverpgxv5.New(dbpool), &river.Config{
		Workers:     workers,
		MaxAttempts: 2,
		JobTimeout:  8 * time.Hour,
		Queues:      map[string]river.QueueConfig{"xref": {MaxWorkers: 1}},
		Logger:      log,
	})
	if err != nil {
		log.Error("starting river client", "err", err)
		os.Exit(1)
	}

	err = rc.Start(context.Background())
	if err != nil {
		log.Error("starting river", "err", err)
		os.Exit(1)
	}

	<-make(chan struct{})
}
