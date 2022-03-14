package dumptsmwal

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"

	"github.com/cnosdb/cnosdb/vend/db/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
)

type Options struct {
	Stderr io.Writer
	Stdout io.Writer

	showDuplicates bool
}

var opt = NewOptions()

func NewOptions() *Options {
	return &Options{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "dumptsmwal",
		Short: "Dumps all entries from one or more WAL (.wal) files only and excludes TSM (.tsm) files.",
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, path := range args {
				if err := process(path); err != nil {
					return err
				}
			}
			return nil
		},
	}
	c.PersistentFlags().BoolVar(&opt.showDuplicates, "show-duplicates", false, "balabala")

	return c
}

func process(path string) error {
	if filepath.Ext(path) != "."+tsm1.WALFileExtension {
		log.Printf("invalid wal filename, skipping %s", path)
		return nil
	}

	// Track the earliest timestamp for each key and a set of keys with out-of-order points.
	minTimestampByKey := make(map[string]int64)
	duplicateKeys := make(map[string]struct{})

	// Open WAL reader.
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	r := tsm1.NewWALSegmentReader(f)

	// Iterate over the WAL entries.
	for r.Next() {
		entry, err := r.Read()
		if err != nil {
			return fmt.Errorf("cannot read entry: %s", err)
		}

		switch entry := entry.(type) {
		case *tsm1.WriteWALEntry:
			if !opt.showDuplicates {
				fmt.Printf("[write] sz=%d\n", entry.MarshalSize())
			}

			keys := make([]string, 0, len(entry.Values))
			for k := range entry.Values {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				for _, v := range entry.Values[k] {
					t := v.UnixNano()

					// Check for duplicate/out of order keys.
					if min, ok := minTimestampByKey[k]; ok && t <= min {
						duplicateKeys[k] = struct{}{}
					}
					minTimestampByKey[k] = t

					// Skip printing if we are only showing duplicate keys.
					if opt.showDuplicates {
						continue
					}

					switch v := v.(type) {
					case tsm1.IntegerValue:
						fmt.Printf("%s %vi %d\n", k, v.Value(), t)
					case tsm1.UnsignedValue:
						fmt.Printf("%s %vu %d\n", k, v.Value(), t)
					case tsm1.FloatValue:
						fmt.Printf("%s %v %d\n", k, v.Value(), t)
					case tsm1.BooleanValue:
						fmt.Printf("%s %v %d\n", k, v.Value(), t)
					case tsm1.StringValue:
						fmt.Printf("%s %q %d\n", k, v.Value(), t)
					default:
						fmt.Printf("%s EMPTY\n", k)
					}
				}
			}

		case *tsm1.DeleteWALEntry:
			fmt.Printf("[delete] sz=%d\n", entry.MarshalSize())
			for _, k := range entry.Keys {
				fmt.Printf("%s\n", string(k))
			}

		case *tsm1.DeleteRangeWALEntry:
			fmt.Printf("[delete-range] min=%d max=%d sz=%d\n", entry.Min, entry.Max, entry.MarshalSize())
			for _, k := range entry.Keys {
				fmt.Printf("%s\n", string(k))
			}

		default:
			return fmt.Errorf("invalid wal entry: %#v", entry)
		}
	}

	// Print keys with duplicate or out-of-order points, if requested.
	if opt.showDuplicates {
		keys := make([]string, 0, len(duplicateKeys))
		for k := range duplicateKeys {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			fmt.Println(k)
		}
	}

	return nil
}
