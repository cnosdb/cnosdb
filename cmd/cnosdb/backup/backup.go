package backup

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cnosdb/cnosdb/cmd/cnosdb/backup_util"
	errors2 "github.com/cnosdb/cnosdb/pkg/errors"
	"github.com/cnosdb/cnosdb/pkg/network"
	"github.com/cnosdb/cnosdb/server/snapshotter"
	gzip "github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var backupExamples = `  cnosdb backup --start 2021-10-10T12:12:00Z`

type options struct {
	StdoutLogger *log.Logger
	StderrLogger *log.Logger

	Stderr io.Writer
	Stdout io.Writer

	host            string
	path            string
	database        string
	retentionPolicy string
	shardID         string

	isBackup bool
	since    time.Time
	start    time.Time
	end      time.Time

	portable         bool
	manifest         backup_util.Manifest
	portableFileBase string
	continueOnError  bool

	BackupFiles []string

	sinceArg string
	startArg string
	endArg   string
}

var env = options{
	Stdout: os.Stdout,
	Stderr: os.Stderr,
}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "backup [flags] PATH",
		Short:   "downloads a snapshot of a data node and saves it to disk",
		Long:    "Creates a backup copy of specified CnosDB database(s) and saves the files to PATH (directory where backups are saved).",
		Example: backupExamples,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableDescriptions: true,
			DisableNoDescFlag:   true,
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			var err error
			env.BackupFiles = []string{}

			// for portable saving, if needed
			env.portableFileBase = time.Now().UTC().Format(backup_util.PortableFileNamePattern)

			// if startArg and endArg are unspecified, or if we are using -since then assume we are doing a full backup of the shards
			env.isBackup = (env.startArg == "" && env.endArg == "") || env.sinceArg != ""

			if env.sinceArg != "" {
				env.since, err = time.Parse(time.RFC3339, env.sinceArg)
				if err != nil {
					return err
				}
			}
			if env.startArg != "" {
				if env.isBackup {
					return errors.New("backup command uses one of -since or -start/-end")
				}
				env.start, err = time.Parse(time.RFC3339, env.startArg)
				if err != nil {
					return err
				}
			}

			if env.endArg != "" {
				if env.isBackup {
					return errors.New("backup command uses one of -since or -start/-end")
				}
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
				return errors.New("exactly one backup path is required")
			}
			env.path = args[0]

			err = os.MkdirAll(env.path, 0700)
			if err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var err error
			env.StdoutLogger = log.New(env.Stdout, "", log.LstdFlags)
			env.StderrLogger = log.New(env.Stderr, "", log.LstdFlags)
			if env.shardID != "" {
				// always backup the metastore
				if err := env.backupMetastore(); err != nil {
					return err
				}
				// Pass true for verifyLocation so we verify that db and rp are correct if given
				err = env.backupShard(env.database, env.retentionPolicy, env.shardID, true)

			} else if env.retentionPolicy != "" {
				// always backup the metastore
				if err := env.backupMetastore(); err != nil {
					return err
				}
				err = env.backupRetentionPolicy()
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

				env.StdoutLogger.Println("No database, retention policy or shard ID given. Full meta store backed up.")
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

	c.Flags().StringVar(&env.host, "host", "localhost:8088", "")
	c.Flags().StringVar(&env.database, "database", "", "")
	c.Flags().StringVar(&env.database, "db", "", "")
	c.Flags().StringVar(&env.retentionPolicy, "retention", "", "")
	c.Flags().StringVar(&env.retentionPolicy, "rp", "", "")
	c.Flags().StringVar(&env.shardID, "shard", "", "")
	c.Flags().StringVar(&env.sinceArg, "since", "", "")
	c.Flags().StringVar(&env.startArg, "start", "", "")
	c.Flags().StringVar(&env.endArg, "end", "", "")
	c.Flags().BoolVar(&env.portable, "portable", false, "")
	c.Flags().BoolVar(&env.continueOnError, "skip-errors", false, "")
	c.Flags().SetOutput(env.Stderr)

	return c
}

func (o *options) backupDatabase() error {
	o.StdoutLogger.Printf("backing up db=%s", o.database)

	req := &snapshotter.Request{
		Type:           snapshotter.RequestDatabaseInfo,
		BackupDatabase: o.database,
	}

	response, err := o.requestInfo(req)
	if err != nil {
		return err
	}

	return o.backupResponsePaths(response)
}

func (o *options) backupResponsePaths(response *snapshotter.Response) error {

	// loop through the returned paths and back up each shard
	for _, path := range response.Paths {
		db, rp, id, err := backup_util.DBRetentionAndShardFromPath(path)
		if err != nil {
			return err
		}

		// Don't need to verify db and rp, we know they're correct here
		err = o.backupShard(db, rp, id, false)

		if err != nil && !o.continueOnError {
			o.StderrLogger.Printf("error (%s) when backing up db: %s, rp %s, shard %s. continuing backup on remaining shards", err, db, rp, id)
			return err
		}
	}

	return nil
}

func (o *options) requestInfo(request *snapshotter.Request) (*snapshotter.Response, error) {
	// Connect to snapshotter service.
	var r snapshotter.Response
	conn, err := network.Dial("tcp", o.host, snapshotter.MuxHeader)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = conn.Close()
	}()
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

func (o *options) backupRetentionPolicy() error {
	if o.isBackup {
		o.StdoutLogger.Printf("backing up rp=%s since %s", o.retentionPolicy, o.since.Format(time.RFC3339))
	} else {
		o.StdoutLogger.Printf("backing up rp=%s with boundaries start=%s, end=%s",
			o.retentionPolicy, o.start.Format(time.RFC3339), o.end.Format(time.RFC3339))
	}

	req := &snapshotter.Request{
		Type:                  snapshotter.RequestRetentionPolicyInfo,
		BackupDatabase:        o.database,
		BackupRetentionPolicy: o.retentionPolicy,
	}

	response, err := o.requestInfo(req)
	if err != nil {
		return err
	}

	return o.backupResponsePaths(response)
}

func (o *options) download(req *snapshotter.Request, path string) (retErr error) {
	// Create local file to write to.
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("open temp file: %s", err)
	}
	defer errors2.Capture(&retErr, f.Close)()

	min := 2 * time.Second
	for i := 0; i < 10; i++ {
		if err = func() error {
			// Connect to snapshotter service.
			conn, err := network.Dial("tcp", o.host, snapshotter.MuxHeader)
			if err != nil {
				return err
			}
			defer errors2.Capture(&retErr, conn.Close)()

			_, err = conn.Write([]byte{byte(req.Type)})
			if err != nil {
				return err
			}

			// Write the request
			if err := json.NewEncoder(conn).Encode(req); err != nil {
				return fmt.Errorf("encode snapshot request: %s", err)
			}

			// Read snapshot from the connection
			n, err := io.Copy(f, conn)
			if err != nil {
				return fmt.Errorf("copy backup to file: err=%v, n=%d", err, n)
			}
			if n == 0 {
				// Unfortunately there is no out-of-band channel to actually return errors from the snapshot service, just
				// 'data' or 'no data'.
				return fmt.Errorf("copy backup to file: no data returned, check server logs for snapshot errors")
			}
			return nil
		}(); err == nil {
			break
		} else if err != nil {
			backoff := time.Duration(math.Pow(3.8, float64(i))) * time.Millisecond
			if backoff < min {
				backoff = min
			}
			o.StderrLogger.Printf("Download shard %v failed %s.  Waiting %v and retrying (%d)...\n", req.ShardID, err, backoff, i)
			time.Sleep(backoff)
		}
	}

	return err
}

func (o *options) downloadAndVerify(req *snapshotter.Request, path string, validator func(string) error) error {
	tmppath := path + backup_util.Suffix
	if err := o.download(req, tmppath); err != nil {
		return err
	}

	if validator != nil {
		if err := validator(tmppath); err != nil {
			if rmErr := os.Remove(tmppath); rmErr != nil {
				o.StderrLogger.Printf("Error cleaning up temporary file: %v", rmErr)
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

func (o *options) backupShard(db, rp, sid string, verifyLocation bool) (err error) {
	reqType := snapshotter.RequestShardBackup
	if !o.isBackup {
		reqType = snapshotter.RequestShardExport
	}

	shardId, err := strconv.ParseUint(sid, 10, 64)
	if err != nil {
		return err
	}

	// Get info about shard retention policy and database to fill-in missing db / rp
	if db == "" || rp == "" || verifyLocation {
		infoReq := &snapshotter.Request{
			Type:           snapshotter.RequestDatabaseInfo,
			BackupDatabase: db, // use db if we did happen to get it to limit result set
		}

		infoResponse, err := o.requestInfo(infoReq)
		if err != nil {
			return err
		}

		var shardFound bool
		for _, path := range infoResponse.Paths {
			checkDb, checkRp, checkSid, err := backup_util.DBRetentionAndShardFromPath(path)
			if err != nil {
				return fmt.Errorf("error while finding shard's db/rp: %w", err)
			}
			if sid == checkSid {
				// Found the shard, now fill-in / check db and rp
				if db == "" {
					db = checkDb
				} else if verifyLocation && db != checkDb {
					return fmt.Errorf("expected shard %d in database '%s', but found '%s'", shardId, db, checkDb)
				}

				if rp == "" {
					rp = checkRp
				} else if verifyLocation && rp != checkRp {
					return fmt.Errorf("expected shard %d with retention policy '%s', but found '%s'", shardId, rp, checkRp)
				}

				shardFound = true
				break
			}
		}
		if !shardFound {
			return fmt.Errorf("did not find shard %d", shardId)
		}
	}

	shardArchivePath, err := o.nextPath(filepath.Join(o.path, fmt.Sprintf(backup_util.BackupFilePattern, db, rp, shardId)))
	if err != nil {
		return err
	}

	if o.isBackup {
		o.StdoutLogger.Printf("backing up db=%v rp=%v shard=%v to %s since %s",
			db, rp, sid, shardArchivePath, o.since.Format(time.RFC3339))
	} else {
		o.StdoutLogger.Printf("backing up db=%v rp=%v shard=%v to %s with boundaries start=%s, end=%s",
			db, rp, sid, shardArchivePath, o.start.Format(time.RFC3339), o.end.Format(time.RFC3339))
	}
	req := &snapshotter.Request{
		Type:                  reqType,
		BackupDatabase:        db,
		BackupRetentionPolicy: rp,
		ShardID:               shardId,
		Since:                 o.since,
		ExportStart:           o.start,
		ExportEnd:             o.end,
	}

	// TODO: verify shard backup data
	err = o.downloadAndVerify(req, shardArchivePath, nil)
	if err != nil {
		_ = os.Remove(shardArchivePath)
		return err
	}
	if !o.portable {
		o.BackupFiles = append(o.BackupFiles, shardArchivePath)
	}

	if o.portable {
		var f, out *os.File
		f, err = os.Open(shardArchivePath)
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := f.Close(); err == nil {
				err = closeErr
			}
			if remErr := os.Remove(shardArchivePath); err == nil {
				err = remErr
			}
		}()

		filePrefix := o.portableFileBase + ".s" + sid
		filename := filePrefix + ".tar.gz"
		out, err = os.OpenFile(filepath.Join(o.path, filename), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		defer func() {
			if out != nil {
				if e := out.Close(); err == nil {
					err = e
				}
			}
		}()
		zw := gzip.NewWriter(out)
		defer func() {
			if zw != nil {
				if e := zw.Close(); err == nil {
					err = e
				}
			}
		}()
		zw.Name = filePrefix + ".tar"

		cw := backup_util.CountingWriter{Writer: zw}

		_, err = io.Copy(&cw, f)
		if err != nil {
			return err
		}

		o.manifest.Files = append(o.manifest.Files, backup_util.Entry{
			Database:     db,
			Policy:       rp,
			ShardID:      shardId,
			FileName:     filename,
			Size:         cw.Total,
			LastModified: 0,
		})

		o.BackupFiles = append(o.BackupFiles, filename)
	}
	return nil

}

func (o *options) nextPath(path string) (string, error) {
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

func (o *options) backupMetastore() (retErr error) {
	metastoreArchivePath, err := o.nextPath(filepath.Join(o.path, backup_util.Metafile))
	if err != nil {
		return err
	}

	o.StdoutLogger.Printf("backing up metastore to %s", metastoreArchivePath)

	req := &snapshotter.Request{
		Type: snapshotter.RequestMetastoreBackup,
	}

	err = o.downloadAndVerify(req, metastoreArchivePath, func(file string) (rErr error) {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		defer errors2.Capture(&rErr, f.Close)()

		var magicByte [8]byte
		n, err := io.ReadFull(f, magicByte[:])
		if err != nil {
			return err
		}

		if n < 8 {
			return errors.New("not enough bytes data to verify")
		}

		magic := binary.BigEndian.Uint64(magicByte[:])
		if magic != snapshotter.BackupMagicHeader {
			o.StderrLogger.Println("Invalid metadata blob, ensure the metadata service is running (default port 8088)")
			return errors.New("invalid metadata received")
		}

		return nil
	})

	if err != nil {
		return err
	}

	if !o.portable {
		o.BackupFiles = append(o.BackupFiles, metastoreArchivePath)
	}

	if o.portable {
		metaBytes, err := backup_util.GetMetaBytes(metastoreArchivePath)
		defer errors2.Capture(&retErr, func() error { return os.Remove(metastoreArchivePath) })()
		if err != nil {
			return err
		}
		filename := o.portableFileBase + ".meta"
		ep := backup_util.PortablePacker{Data: metaBytes, MaxNodeID: 0}
		protoBytes, err := ep.MarshalBinary()
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(o.path, filename), protoBytes, 0644); err != nil {
			_, _ = fmt.Fprintln(o.Stdout, "Error.")
			return err
		}

		o.manifest.Meta.FileName = filename
		o.manifest.Meta.Size = int64(len(metaBytes))
		o.BackupFiles = append(o.BackupFiles, filename)
	}

	return nil
}

/*
const (
	// Suffix is a suffix added to the backup while it's in-process.
	Suffix = ".pending"

	// Metafile is the base name given to the metastore backups.
	Metafile = "meta"

	// BackupFilePattern is the beginning of the pattern for a backup
	// file. They follow the scheme <database>.<retention>.<shardID>.<increment>
	BackupFilePattern = "%s.%s.%05d"
)


// options represents the program execution for "cnosdb backup".
type options struct {
	// The logger passed to the ticker during execution.
	StdoutLogger *log.Logger
	StderrLogger *log.Logger

	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	host            string
	path            string
	database        string
	retentionPolicy string
	shardID         string

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

var env = options{
	Stdout: os.Stdout,
	Stderr: os.Stderr,
}

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
				err = env.backupShard(env.database, env.retentionPolicy, env.shardID)

			} else if env.retentionPolicy != "" {
				// always backup the metastore
				if err := env.backupMetastore(); err != nil {
					return err
				}
				err = env.backupRetentionPolicy()
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

				env.StdoutLogger.Println("No database, retention policy or shard ID given. Full meta store backed up.")
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
	c.Flags().StringVar(&env.retentionPolicy, "rp", "", "Retention Policy to use for the backup. Optional. If not specified, all retention policies are used by default.")
	c.Flags().StringVar(&env.shardID, "shard", "", "The identifier of the shard to back up. Optional. If specified, '--rp <rp_name>' is required.")
	c.Flags().StringVar(&env.startArg, "start", "", "Include all points starting with specified timestamp (RFC3339 format).")
	c.Flags().StringVar(&env.endArg, "end", "", "Exclude all points after timestamp (RFC3339 format).")
	c.Flags().BoolVar(&env.continueOnError, "skip-errors", false, "Optional flag to continue backing up the remaining shards when the current shard fails to backup.")
	c.Flags().BoolVar(&env.portable, "portable", false, `Required to generate backup files in a portable format that can be restored to CnosDB OSS or CnosDB`)

	return c
}

func (cmd *options) backupShard(db, rp, sid string) (err error) {
	id, err := strconv.ParseUint(sid, 10, 64)
	if err != nil {
		return err
	}

	shardArchivePath, err := cmd.nextPath(filepath.Join(cmd.path, fmt.Sprintf(backup_util.BackupFilePattern, db, rp, id)))
	if err != nil {
		return err
	}

	cmd.StdoutLogger.Printf("backing up db=%v rp=%v shard=%v to %s with boundaries start=%s, end=%s",
		db, rp, sid, shardArchivePath, cmd.start.Format(time.RFC3339), cmd.end.Format(time.RFC3339))

	req := &snapshotter.Request{
		Type:                  snapshotter.RequestShardExport,
		BackupDatabase:        db,
		BackupRetentionPolicy: rp,
		ShardID:               id,
		ExportStart:           cmd.start,
		ExportEnd:             cmd.end,
	}

	err = cmd.downloadAndVerify(req, shardArchivePath, nil)
	if err != nil {
		_ = os.Remove(shardArchivePath)
		return err
	}
	if !cmd.portable {
		cmd.BackupFiles = append(cmd.BackupFiles, shardArchivePath)
	}

	if cmd.portable {
		var f, out *os.File
		f, err = os.Open(shardArchivePath)
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := f.Close(); err == nil {
				err = closeErr
			}
			if remErr := os.Remove(shardArchivePath); err == nil {
				err = remErr
			}
		}()

		filePrefix := cmd.portableFileBase + ".s" + sid
		filename := filePrefix + ".tar.gz"
		out, err := os.OpenFile(filepath.Join(cmd.path, filename), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		defer func() {
			if out != nil {
				if e := out.Close(); err == nil {
					err = e
				}
			}
		}()

		zw := gzip.NewWriter(out)
		defer func() {
			if zw != nil {
				if e := zw.Close(); err == nil {
					err = e
				}
			}
		}()
		zw.Name = filePrefix + ".tar"

		cw := backup_util.CountingWriter{Writer: zw}

		_, err = io.Copy(&cw, f)
		if err != nil {
			return err
		}

		var shardID uint64
		shardID, err = strconv.ParseUint(sid, 10, 64)
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
			Policy:       rp,
			ShardID:      shardID,
			FileName:     filename,
			Size:         cw.Total,
			LastModified: 0,
		})

		cmd.BackupFiles = append(cmd.BackupFiles, filename)
	}
	return nil

}

// backupDatabase will request the database information from the server and then backup
// every shard in every retention policy in the database. Each shard will be written to a separate file.
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

// backupRetentionPolicy will request the retention policy information from the server and then backup
// every shard in the retention policy. Each shard will be written to a separate file.
func (cmd *options) backupRetentionPolicy() error {
	cmd.StdoutLogger.Printf("backing up rp=%s with boundaries start=%s, end=%s",
		cmd.retentionPolicy, cmd.start.Format(time.RFC3339), cmd.end.Format(time.RFC3339))

	req := &snapshotter.Request{
		Type:                  snapshotter.RequestRetentionPolicyInfo,
		BackupDatabase:        cmd.database,
		BackupRetentionPolicy: cmd.retentionPolicy,
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
		db, rp, id, err := backup_util.DBRetentionAndShardFromPath(path)
		if err != nil {
			return err
		}

		err = cmd.backupShard(db, rp, id)

		if err != nil && !cmd.continueOnError {
			cmd.StderrLogger.Printf("error (%s) when backing up db: %s, rp %s, shard %s. continuing backup on remaining shards", err, db, rp, id)
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

// requestInfo will request the database or retention policy information from the host
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
  -rp <name>
          Retention policy to use for the backup. Optional. If not specified, all retention policies are used by
          default.
  -shard <id>
          The identifier of the shard to back up. Optional. If specified, '-rp <rp_name>' is required.
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
*/
