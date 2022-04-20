package init

//
import (
	"errors"
	"flag"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_tools/generate"
	"github.com/cnosdb/cnosdb/cmd/cnosdb_tools/server"
	"github.com/spf13/cobra"
	"io"
	"os"
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
			if err := run(opt, args); err != nil {
				return err
			}
			return nil
		},
	}
	return c
}

//
func run(opt *Options, args []string) (err error) {
	err = parseFlags(args)
	if err != nil {
		return err
	}

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

	return nil
}

func parseFlags(args []string) error {
	fs := flag.NewFlagSet("gen-init", flag.ContinueOnError)
	fs.StringVar(&opt.configPath, "config", "", "Config file")
	fs.BoolVar(&opt.printOnly, "print", false, "Print data spec only")
	opt.spec.AddFlags(fs)

	if err := fs.Parse(args); err != nil {
		return err
	}

	if opt.spec.Database == "" {
		return errors.New("database is required")
	}

	if opt.spec.Retention == "" {
		return errors.New("retention policy is required")
	}

	return nil
}
