package verify

import (
	"fmt"
	"github.com/cnosdb/db/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
	"hash/crc32"
	"io"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"text/tabwriter"
	"time"
)

type Options struct {
	Stderr io.Writer
	Stdout io.Writer

	path string
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
		Use:   "verify",
		Short: "Verifies the integrity of TSM files.",
		Run: func(cmd *cobra.Command, args []string) {
			if err := verify(cmd, args); err != nil {
				fmt.Println(err)
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
	c.PersistentFlags().StringVar(&opt.path, "dir", defaultDir, "The path to the storage root directory.")
	return c
}

func verify(cmd *cobra.Command, args []string) error {
	start := time.Now()
	dataPath := filepath.Join(opt.path, "data")

	brokenBlocks := 0
	totalBlocks := 0

	// No need to do this in a loop
	ext := fmt.Sprintf(".%s", tsm1.TSMFileExtension)

	// Get all TSM files by walking through the data path
	files := []string{}
	err := filepath.Walk(dataPath, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ext {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	tw := tabwriter.NewWriter(opt.Stdout, 16, 8, 0, '\t', 0)

	// Verify the checksums of every block in every file
	for _, f := range files {
		file, err := os.OpenFile(f, os.O_RDONLY, 0600)
		if err != nil {
			return err
		}

		reader, err := tsm1.NewTSMReader(file)
		if err != nil {
			return err
		}

		blockItr := reader.BlockIterator()
		count := 0
		for blockItr.Next() {
			totalBlocks++
			key, _, _, _, checksum, buf, err := blockItr.Read()
			if err != nil {
				brokenBlocks++
				fmt.Fprintf(tw, "%s: could not get checksum for key %v block %d due to error: %q\n", f, key, count, err)
			} else if expected := crc32.ChecksumIEEE(buf); checksum != expected {
				brokenBlocks++
				fmt.Fprintf(tw, "%s: got %d but expected %d for key %v, block %d\n", f, checksum, expected, key, count)
			}
			count++
		}
		if brokenBlocks == 0 {
			fmt.Fprintf(tw, "%s: healthy\n", f)
		}
		reader.Close()
	}

	fmt.Fprintf(tw, "Broken Blocks: %d / %d, in %vs\n", brokenBlocks, totalBlocks, time.Since(start).Seconds())
	tw.Flush()
	return nil
}
