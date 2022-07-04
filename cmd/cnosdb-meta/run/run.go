package run

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cnosdb/cnosdb/cmd/cnosdb-meta/options"
	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/pkg/logger"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var description = `Runs the CnosDB Meta Server.`

var run_examples = `  cnosdb-meta run
  cnosdb-meta`

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "run",
		Short:   description,
		Long:    description,
		Example: run_examples,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableDescriptions: true,
			DisableNoDescFlag:   true,
		},
		PreRun: func(cmd *cobra.Command, args []string) {
			if err := logger.InitZapLogger(logger.NewDefaultLogConfig()); err != nil {
				fmt.Println("Unable to configure logger.")
			}
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			config, err := ParseConfig(options.Env.GetConfigPath())
			if err != nil {
				return fmt.Errorf("parse config: %s", err)
			}

			if err := config.ApplyEnvOverrides(os.Getenv); err != nil {
				return fmt.Errorf("apply env config: %v", err)
			}

			if err := logger.InitZapLogger(config.Log); err != nil {
				return fmt.Errorf("parse log config: %s", err)
			}

			d := &CnosMetaServer{
				Server: meta.NewServer(config),
				Closed: make(chan struct{}),
			}
			s := d.Server

			if err := d.Server.Open(nil); err != nil {
				return fmt.Errorf("open server: %s", err)
			}

			signalCh := make(chan os.Signal, 1)
			signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

			// 直到接收到指定信号为止，保持阻塞
			<-signalCh

			go func() {
				defer close(d.Closed)
				s.Close()
			}()

			select {
			case <-signalCh:
				fmt.Println("Second signal received, initializing hard shutdown")
			case <-time.After(time.Second * 30):
				fmt.Println("Time limit reached, initializing hard shutdown")
			case <-d.Closed:
				fmt.Println("Server close completed")
			}

			return nil
		},
	}

	return c
}

type CnosMetaServer struct {
	Server *meta.Server
	Closed chan struct{}
}

// ParseConfig parses the config at path.
// It returns a demo configuration if path is blank.
func ParseConfig(path string) (*meta.Config, error) {
	// Use demo configuration if no config path is specified.
	if path == "" {
		logger.Info("No configuration provided, using default settings")
		return meta.NewDemoClusterConfig()
	}

	logger.Info("Loading configuration file", zap.String("path", path))

	config := meta.NewConfig()
	if err := config.FromTomlFile(path); err != nil {
		return nil, err
	}

	return config, nil
}
