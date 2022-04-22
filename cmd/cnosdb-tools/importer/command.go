package importer

import (
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"io"
	"os"
	"time"

	"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/internal/errlist"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/internal/format/binary"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-tools/server"
	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/vend/db/tsdb/engine/tsm1"
	"go.uber.org/zap"
)

// Options represents the program execution for "store query".
type Options struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdin  io.Reader
	Logger *zap.Logger
	server server.Interface

	configPath      string
	database        string
	retentionPolicy string
	replication     int
	duration        time.Duration
	shardDuration   time.Duration
	buildTSI        bool
	replace         bool
}

var opt = NewOption(server.NewSingleServer())

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "import",
		Short: "The import tool consumes binary data and write data directly to disk",

		RunE: func(cmd *cobra.Command, args []string) (err error) {

			if opt.database == "" {
				return errors.New("[ERR] database is required\n")
			}

			if opt.retentionPolicy == "" {
				return errors.New("[ERR] retention policy is required\n")
			}

			err = opt.server.Open(opt.configPath)
			if err != nil {
				return err
			}

			i := newImporter(opt.server, opt.database, opt.retentionPolicy, opt.replace, opt.buildTSI, opt.Logger)

			reader := binary.NewReader(opt.Stdin)
			_, err = reader.ReadHeader()
			if err != nil {
				return err
			}

			rp := &meta.RetentionPolicySpec{Name: opt.retentionPolicy, ShardGroupDuration: opt.shardDuration}
			if opt.duration >= time.Hour {
				rp.Duration = &opt.duration
			}
			if opt.replication > 0 {
				rp.ReplicaN = &opt.replication
			}
			err = i.CreateDatabase(rp)
			if err != nil {

				return err
			}

			var bh *binary.BucketHeader
			for bh, err = reader.NextBucket(); (bh != nil) && (err == nil); bh, err = reader.NextBucket() {
				err = importShard(reader, i, bh.Start, bh.End)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}

	c.PersistentFlags().StringVar(&opt.configPath, "config", "", "Config file")
	c.PersistentFlags().StringVar(&opt.database, "database", "", "Database name")
	c.PersistentFlags().StringVar(&opt.retentionPolicy, "rp", "", "Retention policy")
	c.PersistentFlags().IntVar(&opt.replication, "replication", 0, "Retention policy replication")
	c.PersistentFlags().DurationVar(&opt.duration, "duration", time.Hour*0, "Retention policy duration")
	c.PersistentFlags().DurationVar(&opt.shardDuration, "shard-duration", time.Hour*24*7, "Retention policy shard duration")
	c.PersistentFlags().BoolVar(&opt.buildTSI, "build-tsi", false, "Build the on disk TSI")
	c.PersistentFlags().BoolVar(&opt.replace, "replace", false, "Enables replacing an existing retention policy")

	return c
}

// NewOption returns a new instance of Options.
func NewOption(server server.Interface) *Options {
	return &Options{
		Stderr: os.Stderr,
		Stdin:  os.Stdin,
		server: server,
	}
}

func importShard(reader *binary.Reader, i *importer, start int64, end int64) error {
	err := i.StartShardGroup(start, end)
	if err != nil {
		return err
	}

	el := errlist.NewErrorList()
	var sh *binary.SeriesHeader
	var next bool
	for sh, err = reader.NextSeries(); (sh != nil) && (err == nil); sh, err = reader.NextSeries() {
		i.AddSeries(sh.SeriesKey)
		pr := reader.Points()
		seriesFieldKey := tsm1.SeriesFieldKeyBytes(string(sh.SeriesKey), string(sh.Field))

		for next, err = pr.Next(); next && (err == nil); next, err = pr.Next() {
			err = i.Write(seriesFieldKey, pr.Values())
			if err != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}

	el.Add(err)
	el.Add(i.CloseShardGroup())

	return el.Err()
}
