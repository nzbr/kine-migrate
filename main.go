package main

import (
	"context"
	"errors"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"os"
	"time"

	_ "github.com/k3s-io/kine/pkg/endpoint"
)

type Config struct {
	FromEndpoint string
	ToEndpoint   string
	Debug        bool
}

func main() {
	app := New()
	if err := app.Run(os.Args); err != nil {
		if !errors.Is(err, context.Canceled) {
			logrus.Fatal(err)
		}
	}
}

func New() *cli.App {
	app := cli.NewApp()
	app.Name = "kine-migrate"
	app.Usage = "Migrate data from one kine backend to another"
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:     "from",
			Usage:    "Source backend endpoint",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "to",
			Usage:    "Destination backend endpoint",
			Required: true,
		},
		&cli.BoolFlag{
			Name:  "debug",
			Usage: "Enable debug logging",
		},
		&cli.BoolFlag{
			Name:  "trace",
			Usage: "Enable query logging",
		},
	}

	app.Action = run
	return app
}

func run(c *cli.Context) error {
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
	})

	if c.Bool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}
	if c.Bool("trace") {
		logrus.SetLevel(logrus.TraceLevel)
	}

	ctx, cancel := context.WithCancel(context.Background())

	logrus.Info("Initializing source backend")

	fromBackend := connect(ctx, c.String("from"))
	toBackend := connect(ctx, c.String("to"))

	logrus.Info("Finished source backend initialization")

	_, items, err := fromBackend.List(ctx, "%", "", 0, 0)

	if err != nil {
		cancel()
		return err
	}

	logrus.Infof("Copying %d items", len(items))

	for i := 0; i < len(items); i++ {
		logrus.Debugf("Copying %s", items[i].Key)
		_, err = toBackend.Create(ctx, items[i].Key, items[i].Value, items[i].Lease)
		if err != nil {
			logrus.Warn("Overwriting existing key ", items[i].Key)
			_, _, _, err = toBackend.Update(ctx, items[i].Key, items[i].Value, 0, items[i].Lease)
			if err != nil {
				cancel()
				return err
			}
		}
	}

	logrus.Info("Done!")

	logrus.Info("Shutting down")

	cancel() // stop backends
	<-ctx.Done()
	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	return nil
}

func connect(ctx context.Context, endpoint string) (backend server.Backend) {
	_, backend, err := drivers.New(ctx, &drivers.Config{
		MetricsRegisterer: nil,
		Endpoint:          endpoint,
		BackendTLSConfig: tls.Config{
			SkipVerify: true,
		},
		ConnectionPoolConfig: generic.ConnectionPoolConfig{
			MaxIdle:     0,
			MaxOpen:     0,
			MaxLifetime: 0,
		},
	})

	if err != nil {
		panic(err)
	}

	if backend == nil {
		panic(errors.New("backend is nil"))
	}

	return backend
}
