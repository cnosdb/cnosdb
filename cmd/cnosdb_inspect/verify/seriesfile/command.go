package seriesfile

import (
	"fmt"
	"github.com/cnosdb/cnosdb/pkg/logger"
	"github.com/spf13/cobra"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"runtime"
)

type Options struct {
	Stderr io.Writer
	Stdout io.Writer

	c          int
	dir        string
	dbName     string
	seriesFile string
	verbose    bool
}

func NewOptions() *Options {
	return &Options{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

var opt = NewOptions()

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "verify-seriesfile",
		Short: "Verifies the integrity of Series files.",
		Run: func(cmd *cobra.Command, args []string) {
			config := logger.NewDefaultLogConfig()
			config.Level = "WARN"
			if opt.verbose {
				config.Level = "INFO"
			}
			vLogger, err := logger.NewLoggerWithConfigAndWriter(config, opt.Stdout)
			if err != nil {
				panic(err)
			}
			v := NewVerify(opt.c, vLogger)
			if opt.seriesFile != "" {
				valid, err := v.VerifySeriesFile(opt.seriesFile)
				if err != nil {
					panic(err)
				}
				if valid {
					fmt.Printf("verify-seriesfile completed, seriesfile is valid.")
				} else {
					fmt.Printf("verify-seriesfile completed, seriesfile is invalid.")
				}
			} else if opt.dbName != "" {
				valid, err := v.VerifySeriesFile(filepath.Join(opt.dir, opt.dbName, "_series"))
				if err != nil {
					panic(err)
				}
				if valid {
					fmt.Printf("verify-seriesfile completed, seriesfile is valid.")
				} else {
					fmt.Printf("verify-seriesfile completed, seriesfile is invalid.")
				}
			} else {
				invalidCnt := 0
				totCnt := 0
				dbs, err := ioutil.ReadDir(opt.dir)
				if err != nil {
					panic(err)
				}

				for _, db := range dbs {
					if !db.IsDir() {
						continue
					}
					totCnt++
					valid, err := v.VerifySeriesFile(filepath.Join(opt.dir, db.Name(), "_series"))
					if !valid {
						invalidCnt++
					}
					if err != nil {
						panic(err)
					}
				}
				fmt.Printf("verify-seriesfile completed, %d/%d invalid seriesfiles.", invalidCnt, totCnt)
			}

		},
	}
	var defaultDir string
	u, err := user.Current()
	if err == nil {
		defaultDir = path.Join(u.HomeDir, ".cnosdb", "data")
	} else if os.Getenv("HOME") != "" {
		defaultDir = path.Join(os.Getenv("HOME"), ".cnosdb", "data")
	} else {
		defaultDir = ""
	}
	c.PersistentFlags().IntVar(&opt.c, "c", runtime.GOMAXPROCS(0), "Specifies the number of concurrent workers to run for this command. Default is equal to the value of GOMAXPROCS. If performance is adversely impacted, you can set a lower value.")
	c.PersistentFlags().StringVar(&opt.dir, "dir", defaultDir, "Specifies the root data path. Defaults to '$HOME/data'.")
	c.PersistentFlags().StringVar(&opt.dbName, "db", "", "Restricts verifying series files to the specified database in the data directory.")
	c.PersistentFlags().StringVar(&opt.seriesFile, "series-file", "", "Path to a specific series file; overrides -db and -dir.")
	c.PersistentFlags().BoolVar(&opt.verbose, "v", false, "Enables verbose logging.")
	return c
}
