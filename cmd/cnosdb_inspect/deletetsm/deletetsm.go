// Package deletetsm bulk deletes a measurement from a raw tsm file.
package deletetsm

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/cnosdb/db/models"
	"github.com/cnosdb/db/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
)

// Command represents the program execution for "influxd deletetsm".
type Options struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	measurement string // measurement to delete
	sanitize    bool   // remove all keys with non-printable unicode
	// verbose     bool   // verbose logging
}

// NewOptions returns a new instance of Options.
func NewOptions() *Options {
	return &Options{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

var opt = NewOptions()

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "deletetsm",
		Short: "delete a measurement in a raw TSM file",
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, path := range args {
				log.Printf("processing: %s", path)
				if err := process(path); err != nil {
					return err
				}
			}
			return nil
		},
	}
	c.PersistentFlags().StringVar(&opt.measurement, "measurement", "", "Delete a measurement in a raw TSM file")
	c.PersistentFlags().BoolVar(&opt.sanitize, "sanitize", false, "Remove all tag and field keys containing non-printable Unicode characters in a raw TSM file")
	// c.PersistentFlags().BoolVar(&opt.verbose, "v", false, "Enable verbose logging")

	return c
}

func process(path string) error {
	// Open TSM reader.
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	defer input.Close()

	r, err := tsm1.NewTSMReader(input)
	if err != nil {
		return fmt.Errorf("unable to read %s: %s", path, err)
	}
	defer r.Close()

	// Remove previous temporary files.
	outputPath := path + ".rewriting.tmp"
	if err := os.RemoveAll(outputPath); err != nil {
		return err
	} else if err := os.RemoveAll(outputPath + ".idx.tmp"); err != nil {
		return err
	}

	// Create TSMWriter to temporary location.
	output, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer output.Close()

	w, err := tsm1.NewTSMWriter(output)
	if err != nil {
		return err
	}
	defer w.Close()

	// Iterate over the input blocks.
	itr := r.BlockIterator()
	for itr.Next() {
		// Read key & time range.
		key, minTime, maxTime, _, _, block, err := itr.Read()
		if err != nil {
			return err
		}

		// Skip block if this is the measurement and time range we are deleting.
		series, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		measurement, tags := models.ParseKey(series)
		if string(measurement) == opt.measurement || (opt.sanitize && !models.ValidKeyTokens(measurement, tags)) {
			log.Printf("deleting block: %s (%s-%s) sz=%d",
				key,
				time.Unix(0, minTime).UTC().Format(time.RFC3339Nano),
				time.Unix(0, maxTime).UTC().Format(time.RFC3339Nano),
				len(block),
			)
			continue
		}

		if err := w.WriteBlock(key, minTime, maxTime, block); err != nil {
			return err
		}
	}

	// Write index & close.
	if err := w.WriteIndex(); err != nil {
		return err
	} else if err := w.Close(); err != nil {
		return err
	}

	// Replace original file with new file.
	return os.Rename(outputPath, path)
}
