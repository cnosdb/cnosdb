package seriesfile

import (
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
			v := NewVerify()
			if opt.seriesFile != "" {
				_, err := v.VerifySeriesFile(opt.seriesFile)
				if err != nil {
					panic(err)
				}
				return
			}
			if opt.dbName != "" {
				_, err := v.VerifySeriesFile(filepath.Join(opt.dir, opt.dbName, "_series"))
				if err != nil {
					panic(err)
				}
				return
			}
			dbs, err := ioutil.ReadDir(opt.dir)
			if err != nil {
				panic(err)
			}

			for _, db := range dbs {
				if !db.IsDir() {
					continue
				}
				_, err := v.VerifySeriesFile(filepath.Join(opt.dir, db.Name(), "_series"))
				if err != nil {
					panic(err)
				}
			}
		},
	}
	var defaultDir string
	u, err := user.Current()
	if err == nil {
		defaultDir = path.Join(u.HomeDir, ".cnosdb")
	} else if os.Getenv("HOME") != "" {
		defaultDir = path.Join(os.Getenv("HOME"), ".cnosdb")
	} else {
		defaultDir = ""
	}
	c.PersistentFlags().IntVar(&opt.c, "c", runtime.GOMAXPROCS(0), "Specifies the number of concurrent workers to run for this command. Default is equal to the value of GOMAXPROCS. If performance is adversely impacted, you can set a lower value.")
	c.PersistentFlags().StringVar(&opt.dir, "dir", defaultDir, "Specifies the root data path. Defaults to $HOME/data")
	c.PersistentFlags().StringVar(&opt.dbName, "db", "", "Restricts verifying series files to the specified database in the data directory.")
	c.PersistentFlags().StringVar(&opt.seriesFile, "series-seriesFile", "", "Path to a specific series seriesFile; overrides -db and -dir.")
	c.PersistentFlags().BoolVar(&opt.verbose, "v", false, "The path to the storage root directory.")
	return c
}
