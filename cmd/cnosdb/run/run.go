package run

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cnosdb/cnosdb/cmd/cnosdb/options"
	"github.com/cnosdb/cnosdb/pkg/logger"
	"github.com/cnosdb/cnosdb/server"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var run_examples = `  cnosdb run
  cnosdb`

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "run",
		Short:   "run node with existing configuration",
		Long:    "Runs the CnosDB server.",
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
		Run: func(cmd *cobra.Command, args []string) {
			config, err := ParseConfig(options.Env.GetConfigPath())
			if err != nil {
				fmt.Printf("parse config: %s\n", err)
			}

			if err := logger.InitZapLogger(config.Log); err != nil {
				fmt.Printf("parse log config: %s\n", err)
			}

			d := &CnosDB{
				Server: server.NewServer(config),
				Closed: make(chan struct{}),
			}
			s := d.Server

			if err := d.Server.Open(); err != nil {
				fmt.Printf("open server: %s\n", err)
			}

			signalCh := make(chan os.Signal, 1)
			signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

			// 直到接收到指定信号为止，保持阻塞
			<-signalCh
			s.Logger.Info("Signal received, initializing clean shutdown...")

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
		},
	}
	return c
}

type CnosDB struct {
	Server *server.Server
	Closed chan struct{}
}

// ParseConfig parses the config at path.
// It returns a demo configuration if path is blank.
func ParseConfig(path string) (*server.Config, error) {
	// Use demo configuration if no config path is specified.
	if path == "" {
		logger.Info("No configuration provided, using default settings")
		if config, err := server.NewDemoConfig(); err != nil {
			return config, err
		} else {
			if err := config.ApplyEnvOverrides(os.Getenv); err != nil {
				return config, fmt.Errorf("apply env config: %v", err)
			}
			return config, err
		}
	}

	logger.Info("Loading configuration file", zap.String("path", path))

	config := server.NewConfig()
	if err := config.FromTomlFile(path); err != nil {
		return nil, err
	}

	if err := config.ApplyEnvOverrides(os.Getenv); err != nil {
		return config, fmt.Errorf("apply env config: %v", err)
	}
	return config, nil
}
