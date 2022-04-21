package init

//
import (
	"github.com/spf13/cobra"
	//"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/generate"
	//"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/server"
)

//
//// Command represents the program execution for "store query".
//type Options struct {
//	Stdin  io.Reader
//	Stdout io.Writer
//	Stderr io.Writer
//	server server.Interface
//
//	configPath string
//	printOnly  bool
//	spec       generate.StorageSpec
//}

//type ossServer struct {
//	logger   *zap.Logger
//	config   *run.Config
//	noClient bool
//	client   *meta.Client
//	mc       server.MetaClient
//}

//var opt = NewOptions()
//
//// NewCommand returns a new instance of Command.
//func NewOptions() *Options {
//	return &Options{
//		Stdin:  os.Stdin,
//		Stdout: os.Stdout,
//		Stderr: os.Stderr,
//		//server: server,
//	}
//}
//
func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "gen-init",
		Short: "creates database and retention policy metadata",

		Run: func(cmd *cobra.Command, args []string) {
			//if err := Run(opt, args); err != nil {
			//	fmt.Println(err)
			//}
		},
	}
	return c
}

//
//func Run(opt *Options, args []string) (err error) {
//	err = opt.parseFlags(args)
//	if err != nil {
//		return err
//	}
//
//	err = opt.server.Open(opt.configPath)
//	if err != nil {
//		return err
//	}
//
//	plan, err := opt.spec.Plan(opt.server)
//	if err != nil {
//		return err
//	}
//
//	plan.PrintPlan(opt.Stdout)
//
//	if !opt.printOnly {
//		return plan.InitMetadata(opt.server.MetaClient())
//	}
//
//	return nil
//}
////
//func (opt *Options) parseFlags(args []string) error {
//	fs := flag.NewFlagSet("gen-init", flag.ContinueOnError)
//	fs.StringVar(&opt.configPath, "config", "", "Config file")
//	fs.BoolVar(&opt.printOnly, "print", false, "Print data spec only")
//	//opt.spec.AddFlags(fs)
//
//	if err := fs.Parse(args); err != nil {
//		return err
//	}
//
//	if opt.spec.Database == "" {
//		return errors.New("database is required")
//	}
//
//	if opt.spec.Retention == "" {
//		return errors.New("retention policy is required")
//	}
//
//	return nil
//}
