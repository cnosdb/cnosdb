package backup

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cnosdatabase/cnosdb/cmd/cnosdb/backup_util"
	"github.com/cnosdatabase/cnosdb/pkg/network"
	"github.com/cnosdatabase/cnosdb/server/snapshotter"
	gzip "github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
)

const (
	// Suffix is a suffix added to the backup while it's in-process.
	Suffix = ".pending"

	// Metafile is the base name given to the metastore backups.
	Metafile = "meta"

	// BackupFilePattern is the beginning of the pattern for a backup
	// file. They follow the scheme <database>.<ttl>.<shardID>.<increment>
	BackupFilePattern = "%s.%s.%05d"
)

var backup_examples = `  cnosdb backup --start 2021-10-10T12:12:00Z`

// options represents the program execution for "cnosdb backup".
type options struct {
	// The logger passed to the ticker during execution.
	StdoutLogger *log.Logger
	StderrLogger *log.Logger

	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	host       string
	path       string
	database   string
	timeToLive string
	shardID    string

	startArg string
	endArg   string
	start    time.Time
	end      time.Time

	portable         bool
	manifest         backup_util.Manifest
	portableFileBase string
	continueOnError  bool

	BackupFiles []string
}

var env = options{}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "backup [flags] PATH",
		Short:   "downloads a snapshot of a data node and saves it to disk",
		Long:    "Creates a backup copy of specified CnosDB database(s) and saves the files to PATH (directory where backups are saved).",
		Example: backup_examples,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableDescriptions: true,
			DisableNoDescFlag:   true,
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			env.BackupFiles = []string{}

			// for portable saving, if needed
			env.portableFileBase = time.Now().UTC().Format(backup_util.PortableFileNamePattern)

			var err error
			if env.startArg != "" {
				env.start, err = time.Parse(time.RFC3339, env.startArg)
				if err != nil {
					return err
				}
			}

			if env.endArg != "" {
				env.end, err = time.Parse(time.RFC3339, env.endArg)
				if err != nil {
					return err
				}

				// start should be < end
				if !env.start.Before(env.end) {
					return errors.New("start date must be before end date")
				}
			}

			// Ensure that only one arg is specified.
			if len(args) != 1 {
				return errors.New("Exactly one backup path is required.")
			}
			env.path = args[0]

			err = os.MkdirAll(env.path, 0700)

			return err
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// Set up logger.
			env.StdoutLogger = log.New(env.Stdout, "", log.LstdFlags)
			env.StderrLogger = log.New(env.Stderr, "", log.LstdFlags)

			var err error

			if env.shardID != "" {
				// always backup the metastore
				if err := env.backupMetastore(); err != nil {
					return err
				}
				err = env.backupShard(env.database, env.timeToLive, env.shardID)

			} else if env.timeToLive != "" {
				// always backup the metastore
				if err := env.backupMetastore(); err != nil {
					return err
				}
				err = env.backupTimeToLive()
			} else if env.database != "" {
				// always backup the metastore
				if err := env.backupMetastore(); err != nil {
					return err
				}
				err = env.backupDatabase()
			} else {
				// always backup the metastore
				if err := env.backupMetastore(); err != nil {
					return err
				}

				env.StdoutLogger.Println("No database, time to live or shard ID given. Full meta store backed up.")
				if env.portable {
					env.StdoutLogger.Println("Backing up all databases in portable format")
					if err := env.backupDatabase(); err != nil {
						env.StderrLogger.Printf("backup failed: %v", err)
						return err
					}

				}

			}

			if env.portable {
				filename := env.portableFileBase + ".manifest"
				if err := env.manifest.Save(filepath.Join(env.path, filename)); err != nil {
					env.StderrLogger.Printf("manifest save failed: %v", err)
					return err
				}
				env.BackupFiles = append(env.BackupFiles, filename)
			}

			if err != nil {
				env.StderrLogger.Printf("backup failed: %v", err)
				return err
			}
			env.StdoutLogger.Println("backup complete:")
			for _, v := range env.BackupFiles {
				env.StdoutLogger.Println("\t" + filepath.Join(env.path, v))
			}

			return nil
		},
	}

	c.Flags().StringVar(&env.host, "host", "localhost:8088", "CnosDB host to back up from. Optional. Defaults to 127.0.0.1:8088.")
	c.Flags().StringVar(&env.database, "db", "", "CnosDB database name to back up. Optional. If not specified, all databases are backed up.")
	c.Flags().StringVar(&env.timeToLive, "ttl", "", "Time-to-live to use for the backup. Optional. If not specified, all time-to-lives are used by default.")
	c.Flags().StringVar(&env.shardID, "shard", "", "The identifier of the shard to back up. Optional. If specified, '--ttl <ttl_name>' is required.")
	c.Flags().StringVar(&env.startArg, "start", "", "Include all points starting with specified timestamp (RFC3339 format).")
	c.Flags().StringVar(&env.endArg, "end", "", "Exclude all points after timestamp (RFC3339 format).")
	c.Flags().BoolVar(&env.continueOnError, "skip-errors", false, "Optional flag to continue backing up the remaining shards when the current shard fails to backup.")

	return c
}

func (cmd *options) backupShard(db, ttl, sid string) error {
	id, err := strconv.ParseUint(sid, 10, 64)
	if err != nil {
		return err
	}

	shardArchivePath, err := cmd.nextPath(filepath.Join(cmd.path, fmt.Sprintf(backup_util.BackupFilePattern, db, ttl, id)))
	if err != nil {
		return err
	}

	cmd.StdoutLogger.Printf("backing up db=%v ttl=%v shard=%v to %s with boundaries start=%s, end=%s",
		db, ttl, sid, shardArchivePath, cmd.start.Format(time.RFC3339), cmd.end.Format(time.RFC3339))

	req := &snapshotter.Request{
		Type:             snapshotter.RequestShardExport,
		BackupDatabase:   db,
		BackupTimeToLive: ttl,
		ShardID:          id,
		ExportStart:      cmd.start,
		ExportEnd:        cmd.end,
	}

	// TODO: verify shard backup data
	err = cmd.downloadAndVerify(req, shardArchivePath, nil)
	if err != nil {
		os.Remove(shardArchivePath)
		return err
	}
	if !cmd.portable {
		cmd.BackupFiles = append(cmd.BackupFiles, shardArchivePath)
	}

	if cmd.portable {
		f, err := os.Open(shardArchivePath)
		if err != nil {
			return err
		}
		defer f.Close()
		defer os.Remove(shardArchivePath)

		filePrefix := cmd.portableFileBase + ".s" + sid
		filename := filePrefix + ".tar.gz"
		out, err := os.OpenFile(filepath.Join(cmd.path, filename), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}

		zw := gzip.NewWriter(out)
		zw.Name = filePrefix + ".tar"

		cw := backup_util.CountingWriter{Writer: zw}

		_, err = io.Copy(&cw, f)
		if err != nil {
			if err := zw.Close(); err != nil {
				return err
			}

			if err := out.Close(); err != nil {
				return err
			}
			return err
		}

		shardid, err := strconv.ParseUint(sid, 10, 64)
		if err != nil {
			if err := zw.Close(); err != nil {
				return err
			}

			if err := out.Close(); err != nil {
				return err
			}
			return err
		}
		cmd.manifest.Files = append(cmd.manifest.Files, backup_util.Entry{
			Database:     db,
			Policy:       ttl,
			ShardID:      shardid,
			FileName:     filename,
			Size:         cw.Total,
			LastModified: 0,
		})

		if err := zw.Close(); err != nil {
			return err
		}

		if err := out.Close(); err != nil {
			return err
		}

		cmd.BackupFiles = append(cmd.BackupFiles, filename)
	}
	return nil

}

// backupDatabase will request the database information from the server and then backup
// every shard in every time to live in the database. Each shard will be written to a separate file.
func (cmd *options) backupDatabase() error {
	cmd.StdoutLogger.Printf("backing up db=%s", cmd.database)

	req := &snapshotter.Request{
		Type:           snapshotter.RequestDatabaseInfo,
		BackupDatabase: cmd.database,
	}

	response, err := cmd.requestInfo(req)
	if err != nil {
		return err
	}

	return cmd.backupResponsePaths(response)
}

// backupTimeToLive will request the time to live information from the server and then backup
// every shard in the time to live. Each shard will be written to a separate file.
func (cmd *options) backupTimeToLive() error {
	cmd.StdoutLogger.Printf("backing up ttl=%s with boundaries start=%s, end=%s",
		cmd.timeToLive, cmd.start.Format(time.RFC3339), cmd.end.Format(time.RFC3339))

	req := &snapshotter.Request{
		Type:             snapshotter.RequestTimeToLiveInfo,
		BackupDatabase:   cmd.database,
		BackupTimeToLive: cmd.timeToLive,
	}

	response, err := cmd.requestInfo(req)
	if err != nil {
		return err
	}

	return cmd.backupResponsePaths(response)
}

// backupResponsePaths will backup all shards identified by shard paths in the response struct
func (cmd *options) backupResponsePaths(response *snapshotter.Response) error {

	// loop through the returned paths and back up each shard
	for _, path := range response.Paths {
		db, ttl, id, err := backup_util.DBTimeToLiveAndShardFromPath(path)
		if err != nil {
			return err
		}

		err = cmd.backupShard(db, ttl, id)

		if err != nil && !cmd.continueOnError {
			cmd.StderrLogger.Printf("error (%s) when backing up db: %s, ttl %s, shard %s. continuing backup on remaining shards", err, db, ttl, id)
			return err
		}
	}

	return nil
}

// backupMetastore will backup the whole metastore on the host to the backup path
// if useDB is non-empty, it will backup metadata only for the named database.
func (cmd *options) backupMetastore() error {
	metastoreArchivePath, err := cmd.nextPath(filepath.Join(cmd.path, backup_util.Metafile))
	if err != nil {
		return err
	}

	cmd.StdoutLogger.Printf("backing up metastore to %s", metastoreArchivePath)

	req := &snapshotter.Request{
		Type: snapshotter.RequestMetastoreBackup,
	}

	err = cmd.downloadAndVerify(req, metastoreArchivePath, func(file string) error {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		defer f.Close()

		var magicByte [8]byte
		n, err := io.ReadFull(f, magicByte[:])
		if err != nil {
			return err
		}

		if n < 8 {
			return errors.New("Not enough bytes data to verify")
		}

		magic := binary.BigEndian.Uint64(magicByte[:])
		if magic != snapshotter.BackupMagicHeader {
			cmd.StderrLogger.Println("Invalid metadata blob, ensure the metadata service is running (default port 8088)")
			return errors.New("invalid metadata received")
		}

		return nil
	})

	if err != nil {
		return err
	}

	if !cmd.portable {
		cmd.BackupFiles = append(cmd.BackupFiles, metastoreArchivePath)
	}

	if cmd.portable {
		metaBytes, err := backup_util.GetMetaBytes(metastoreArchivePath)
		defer os.Remove(metastoreArchivePath)
		if err != nil {
			return err
		}
		filename := cmd.portableFileBase + ".meta"
		ep := backup_util.PortablePacker{Data: metaBytes, MaxNodeID: 0}
		protoBytes, err := ep.MarshalBinary()
		if err != nil {
			return err
		}
		if err := ioutil.WriteFile(filepath.Join(cmd.path, filename), protoBytes, 0644); err != nil {
			fmt.Fprintln(cmd.Stdout, "Error.")
			return err
		}

		cmd.manifest.Meta.FileName = filename
		cmd.manifest.Meta.Size = int64(len(metaBytes))
		cmd.BackupFiles = append(cmd.BackupFiles, filename)
	}

	return nil
}

// nextPath returns the next file to write to.
func (cmd *options) nextPath(path string) (string, error) {
	// Iterate through incremental files until one is available.
	for i := 0; ; i++ {
		s := fmt.Sprintf(path+".%02d", i)
		if _, err := os.Stat(s); os.IsNotExist(err) {
			return s, nil
		} else if err != nil {
			return "", err
		}
	}
}

// downloadAndVerify will download either the metastore or shard to a temp file and then
// rename it to a good backup file name after complete
func (cmd *options) downloadAndVerify(req *snapshotter.Request, path string, validator func(string) error) error {
	tmppath := path + backup_util.Suffix
	if err := cmd.download(req, tmppath); err != nil {
		return err
	}

	if validator != nil {
		if err := validator(tmppath); err != nil {
			if rmErr := os.Remove(tmppath); rmErr != nil {
				cmd.StderrLogger.Printf("Error cleaning up temporary file: %v", rmErr)
			}
			return err
		}
	}

	f, err := os.Stat(tmppath)
	if err != nil {
		return err
	}

	// There was nothing downloaded, don't create an empty backup file.
	if f.Size() == 0 {
		return os.Remove(tmppath)
	}

	// Rename temporary file to final path.
	if err := os.Rename(tmppath, path); err != nil {
		return fmt.Errorf("rename: %s", err)
	}

	return nil
}

// download downloads a snapshot of either the metastore or a shard from a host to a given path.
func (cmd *options) download(req *snapshotter.Request, path string) error {
	// Create local file to write to.
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("open temp file: %s", err)
	}
	defer f.Close()

	min := 2 * time.Second
	for i := 0; i < 10; i++ {
		if err = func() error {
			// Connect to snapshotter service.
			conn, err := network.Dial("tcp", cmd.host, snapshotter.MuxHeader)
			if err != nil {
				return err
			}
			defer conn.Close()

			_, err = conn.Write([]byte{byte(req.Type)})
			if err != nil {
				return err
			}

			// Write the request
			if err := json.NewEncoder(conn).Encode(req); err != nil {
				return fmt.Errorf("encode snapshot request: %s", err)
			}

			// Read snapshot from the connection
			if n, err := io.Copy(f, conn); err != nil || n == 0 {
				return fmt.Errorf("copy backup to file: err=%v, n=%d", err, n)
			}
			return nil
		}(); err == nil {
			break
		} else if err != nil {
			backoff := time.Duration(math.Pow(3.8, float64(i))) * time.Millisecond
			if backoff < min {
				backoff = min
			}
			cmd.StderrLogger.Printf("Download shard %v failed %s.  Waiting %v and retrying (%d)...\n", req.ShardID, err, backoff, i)
			time.Sleep(backoff)
		}
	}

	return err
}

// requestInfo will request the database or time to live information from the host
func (cmd *options) requestInfo(request *snapshotter.Request) (*snapshotter.Response, error) {
	// Connect to snapshotter service.
	var r snapshotter.Response
	conn, err := network.Dial("tcp", cmd.host, snapshotter.MuxHeader)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_, err = conn.Write([]byte{byte(request.Type)})
	if err != nil {
		return &r, err
	}

	// Write the request
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return nil, fmt.Errorf("encode snapshot request: %s", err)
	}

	// Read the response

	if err := json.NewDecoder(conn).Decode(&r); err != nil {
		return nil, err
	}

	return &r, nil
}

// printUsage prints the usage message to STDERR.
func (cmd *options) printUsage() {
	fmt.Fprintf(cmd.Stdout, `
Creates a backup copy of specified CnosDB database(s) and saves the files to PATH (directory where backups are saved).

Usage: cnosdb backup [options] PATH

  -portable
          Required to generate backup files in a portable format that can be restored to CnosDB OSS or CnosDB
          Enterprise. Use unless the legacy backup is required.
  -host <host:port>
          CnosDB OSS host to back up from. Optional. Defaults to 127.0.0.1:8088.
  -db <name>
          CnosDB OSS database name to back up. Optional. If not specified, all databases are backed up when
          using '-portable'.
  -ttl <name>
          Time to live to use for the backup. Optional. If not specified, all time to lives are used by
          default.
  -shard <id>
          The identifier of the shard to back up. Optional. If specified, '-ttl <ttl_name>' is required.
  -start <2015-12-24T08:12:23Z>
          Include all points starting with specified timestamp (RFC3339 format).
          Not compatible with '-since <timestamp>'.
  -end <2015-12-24T08:12:23Z>
          Exclude all points after timestamp (RFC3339 format).
          Not compatible with '-since <timestamp>'.
  -since <2015-12-24T08:12:23Z>
          Create an incremental backup of all points after the timestamp (RFC3339 format). Optional.
          Recommend using '-start <timestamp>' instead.
  -skip-errors
          Optional flag to continue backing up the remaining shards when the current shard fails to backup.
`)

}
