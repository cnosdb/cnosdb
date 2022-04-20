package init

//
import (
	"errors"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_tools/generate"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_tools/server"
	"github.com/spf13/cobra"
	"io"
	"os"
	"time"
)

// Options represents the program execution for "store query".
type Options struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	server server.Interface

	configPath string
	printOnly  bool
	spec       generate.StorageSpec
}

var opt = NewOptions(server.NewSingleServer())

//// NewOptions returns a new instance of Options.
func NewOptions(server server.Interface) *Options {
	return &Options{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		server: server,
	}
}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "gen-init",
		Short: "creates database and retention policy metadata",

		RunE: func(cmd *cobra.Command, args []string) error {
			//fmt.Println(opt.configPath)
			//var path string
			//if c := cmd.Flag("config"); c != nil {
			//	path = c.Value.String()
			//}
			//
			//if path != "" {
			//	fmt.Fprintf(os.Stderr, "Merging with configuration at: %s\n", path)
			//	opt.configPath = path
			//}

			return run(opt)
		},
	}
	c.PersistentFlags().StringVar(&opt.configPath, "config", "", "Config file")
	c.PersistentFlags().BoolVar(&opt.printOnly, "print", false, "Print data spec only")

	c.PersistentFlags().StringVar(&opt.spec.StartTime, "start-time", "", "Start time")
	c.PersistentFlags().StringVar(&opt.spec.Database, "db", "db", "Name of database to create")
	c.PersistentFlags().StringVar(&opt.spec.Retention, "rp", "rp", "Name of retention policy")
	c.PersistentFlags().IntVar(&opt.spec.ReplicaN, "rf", 1, "Replication factor")
	c.PersistentFlags().IntVar(&opt.spec.ShardCount, "shards", 1, "Number of shards to create")
	c.PersistentFlags().DurationVar(&opt.spec.ShardDuration, "shard-duration", 24*time.Hour, "Shard duration (default 24h)")
	return c
}

//
func run(opt *Options) (err error) {

	err = opt.server.Open(opt.configPath)
	if err != nil {
		return err
	}

	plan, err := opt.spec.Plan(opt.server)
	if err != nil {
		return err
	}

	plan.PrintPlan(opt.Stdout)

	if !opt.printOnly {
		return plan.InitMetadata(opt.server.MetaClient())
	}

	err = parseFlags()
	if err != nil {
		return err
	}

	return nil
}

func parseFlags() error {

	if opt.spec.Database == "" {
		return errors.New("database is required")
	}

	if opt.spec.Retention == "" {
		return errors.New("retention policy is required")
	}

	return nil
}
