package restore

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cnosdb/cnosdb/cmd/cnosdb/backup_util"
	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/server/snapshotter"
	tarstream "github.com/cnosdb/cnosdb/vend/db/pkg/tar"

	gzip "github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
)

type options struct {
	// The logger passed to the ticker during execution.
	StdoutLogger *log.Logger
	StderrLogger *log.Logger

	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	host   string
	client *snapshotter.Client

	backupFilesPath     string
	metadir             string
	datadir             string
	destinationDatabase string
	sourceDatabase      string
	backupRetention     string
	restoreRetention    string
	shard               uint64
	portable            bool
	online              bool
	manifestMeta        *backup_util.MetaEntry
	manifestFiles       map[uint64]*backup_util.Entry

	// TODO: when the new meta stuff is done this should not be exported or be gone
	MetaConfig *meta.Config

	shardIDMap map[uint64]uint64
}

var env = options{
	Stdout: os.Stdout,
	Stderr: os.Stderr,
}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:     "restore [flags] PATH",
		Short:   "uses a snapshot of a data node to rebuild a cluster",
		Long:    "Uses backup copies from the specified PATH to restore databases or specific shards from CnosDB to an CnosDB instance.",
		Example: `  cnosdb restore`,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableDescriptions: true,
			DisableNoDescFlag:   true,
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			env.MetaConfig = meta.NewConfig()
			env.MetaConfig.Dir = env.metadir
			env.client = snapshotter.NewClient(env.host)

			if len(args) != 1 {
				return errors.New("exactly one backup path is required")
			}
			env.backupFilesPath = args[0]
			if env.backupFilesPath == "" {
				return fmt.Errorf("path with backup files required")
			}

			fi, err := os.Stat(env.backupFilesPath)
			if err != nil || !fi.IsDir() {
				return fmt.Errorf("backup path should be a valid directory: %s", env.backupFilesPath)
			}

			if env.destinationDatabase != "" && env.sourceDatabase == "" {
				return fmt.Errorf("must specify a database to be restored into new database %s", env.destinationDatabase)
			}

			if env.portable || env.online {
				// validate the arguments

				if env.metadir != "" {
					return fmt.Errorf("offline parameter metadir found, not compatible with -portable")
				}

				if env.datadir != "" {
					return fmt.Errorf("offline parameter datadir found, not compatible with -portable")
				}

				if env.restoreRetention == "" {
					env.restoreRetention = env.backupRetention
				}

				if env.portable {
					var err error

					env.manifestMeta, env.manifestFiles, err = backup_util.LoadIncremental(env.backupFilesPath)
					if err != nil {
						return fmt.Errorf("restore failed while processing manifest files: %s", err.Error())
					} else if env.manifestMeta == nil {
						// No manifest files found.
						return fmt.Errorf("No manifest files found in: %s\n", env.backupFilesPath)

					}
				}
			} else {
				// validate the arguments
				if env.destinationDatabase == "" {
					env.destinationDatabase = env.sourceDatabase
				}

				if env.metadir == "" && env.destinationDatabase == "" {
					return fmt.Errorf("-metadir or -newdb are required to restore")
				}

				if env.destinationDatabase != "" && env.datadir == "" {
					return fmt.Errorf("-datadir is required to restore")
				}

				if env.shard != 0 {
					if env.destinationDatabase == "" {
						return fmt.Errorf("-newdb is required to restore shard")
					}
					if env.backupRetention == "" {
						return fmt.Errorf("-retention is required to restore shard")
					}
				} else if env.backupRetention != "" && env.destinationDatabase == "" {
					return fmt.Errorf("-newdb is required to restore retention policy")
				}
			}

			env.StderrLogger = log.New(env.Stderr, "", log.LstdFlags)
			env.StdoutLogger = log.New(env.Stdout, "", log.LstdFlags)
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if env.portable {
				return env.runOnlinePortable()
			} else if env.online {
				return env.runOnlineLegacy()
			} else {
				return env.runOffline()
			}
		},
	}

	c.Flags().StringVar(&env.host, "host", "localhost:8088", "")
	c.Flags().StringVar(&env.metadir, "metadir", "", "")
	c.Flags().StringVar(&env.datadir, "datadir", "", "")
	c.Flags().StringVar(&env.sourceDatabase, "database", "", "")
	c.Flags().StringVar(&env.sourceDatabase, "db", "", "")
	c.Flags().StringVar(&env.destinationDatabase, "newdb", "", "")
	c.Flags().StringVar(&env.backupRetention, "retention", "", "")
	c.Flags().StringVar(&env.backupRetention, "rp", "", "")
	c.Flags().StringVar(&env.restoreRetention, "newrp", "", "")
	c.Flags().Uint64Var(&env.shard, "shard", 0, "")
	c.Flags().BoolVar(&env.online, "online", false, "")
	c.Flags().BoolVar(&env.portable, "portable", false, "")
	c.Flags().SetOutput(env.Stderr)
	return c
}

func (o *options) runOnlinePortable() error {
	err := o.updateMetaPortable()
	if err != nil {
		o.StderrLogger.Printf("error updating meta: %v", err)
		return err
	}
	err = o.uploadShardsPortable()
	if err != nil {
		o.StderrLogger.Printf("error updating shards: %v", err)
		return err
	}
	return nil
}

func (o *options) runOnlineLegacy() error {
	err := o.updateMetaLegacy()
	if err != nil {
		o.StderrLogger.Printf("error updating meta: %v", err)
		return err
	}
	err = o.uploadShardsLegacy()
	if err != nil {
		o.StderrLogger.Printf("error updating shards: %v", err)
		return err
	}
	return nil
}

func (o *options) runOffline() error {
	if o.metadir != "" {
		if err := o.unpackMeta(); err != nil {
			return err
		}
	}

	if o.shard != 0 {
		return o.unpackShard(o.shard)
	} else if o.restoreRetention != "" {
		return o.unpackRetention()
	} else if o.datadir != "" {
		return o.unpackDatabase()
	}
	return nil
}

func (o *options) updateMetaPortable() error {
	var metaBytes []byte
	fileName := filepath.Join(o.backupFilesPath, o.manifestMeta.FileName)

	fileBytes, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}

	var ep backup_util.PortablePacker
	_ = ep.UnmarshalBinary(fileBytes)

	metaBytes = ep.Data

	req := &snapshotter.Request{
		Type:                   snapshotter.RequestMetaStoreUpdate,
		BackupDatabase:         o.sourceDatabase,
		RestoreDatabase:        o.destinationDatabase,
		BackupRetentionPolicy:  o.backupRetention,
		RestoreRetentionPolicy: o.restoreRetention,
		UploadSize:             int64(len(metaBytes)),
	}

	shardIDMap, err := o.client.UpdateMeta(req, bytes.NewReader(metaBytes))
	o.shardIDMap = shardIDMap
	return err

}

func (o *options) uploadShardsPortable() error {
	for _, file := range o.manifestFiles {
		if o.sourceDatabase == "" || o.sourceDatabase == file.Database {
			if o.backupRetention == "" || o.backupRetention == file.Policy {
				if o.shard == 0 || o.shard == file.ShardID {
					oldID := file.ShardID
					// if newID not found then this shard's metadata was NOT imported
					// and should be skipped
					newID, ok := o.shardIDMap[oldID]
					if !ok {
						o.StdoutLogger.Printf("Meta info not found for shard %d on database %s. Skipping shard file %s", oldID, file.Database, file.FileName)
						continue
					}
					o.StdoutLogger.Printf("Restoring shard %d live from backup %s\n", file.ShardID, file.FileName)
					f, err := os.Open(filepath.Join(o.backupFilesPath, file.FileName))
					if err != nil {
						_ = f.Close()
						return err
					}
					gr, err := gzip.NewReader(f)
					if err != nil {
						_ = f.Close()
						return err
					}
					tr := tar.NewReader(gr)
					targetDB := o.destinationDatabase
					if targetDB == "" {
						targetDB = file.Database
					}

					if err := o.client.UploadShard(oldID, newID, targetDB, o.restoreRetention, tr); err != nil {
						_ = f.Close()
						return err
					}
					_ = f.Close()
				}
			}
		}
	}
	return nil
}

func (o *options) updateMetaLegacy() error {

	var metaBytes []byte

	// find the meta file
	metaFiles, err := filepath.Glob(filepath.Join(o.backupFilesPath, backup_util.Metafile+".*"))
	if err != nil {
		return err
	}

	if len(metaFiles) == 0 {
		return fmt.Errorf("no metastore backups in %s", o.backupFilesPath)
	}

	fileName := metaFiles[len(metaFiles)-1]
	o.StdoutLogger.Printf("Using metastore snapshot: %v\n", fileName)
	metaBytes, err = backup_util.GetMetaBytes(fileName)
	if err != nil {
		return err
	}

	req := &snapshotter.Request{
		Type:                   snapshotter.RequestMetaStoreUpdate,
		BackupDatabase:         o.sourceDatabase,
		RestoreDatabase:        o.destinationDatabase,
		BackupRetentionPolicy:  o.backupRetention,
		RestoreRetentionPolicy: o.restoreRetention,
		UploadSize:             int64(len(metaBytes)),
	}

	shardIDMap, err := o.client.UpdateMeta(req, bytes.NewReader(metaBytes))
	o.shardIDMap = shardIDMap
	return err
}

func (o *options) uploadShardsLegacy() error {
	// find the destinationDatabase backup files
	pat := fmt.Sprintf("%s.*", filepath.Join(o.backupFilesPath, o.sourceDatabase))
	o.StdoutLogger.Printf("Restoring live from backup %s\n", pat)
	backupFiles, err := filepath.Glob(pat)
	if err != nil {
		return err
	}
	if len(backupFiles) == 0 {
		return fmt.Errorf("no backup files in %s", o.backupFilesPath)
	}

	for _, fn := range backupFiles {
		parts := strings.Split(filepath.Base(fn), ".")

		if len(parts) != 4 {
			o.StderrLogger.Printf("Skipping mis-named backup file: %s", fn)
		}
		shardID, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return err
		}

		// if newID not found then this shard's metadata was NOT imported
		// and should be skipped
		newID, ok := o.shardIDMap[shardID]
		if !ok {
			o.StdoutLogger.Printf("Meta info not found for shard %d. Skipping shard file %s", shardID, fn)
			continue
		}
		f, err := os.Open(fn)
		if err != nil {
			return err
		}
		tr := tar.NewReader(f)
		if err := o.client.UploadShard(shardID, newID, o.destinationDatabase, o.restoreRetention, tr); err != nil {
			_ = f.Close()
			return err
		}
		_ = f.Close()
	}

	return nil
}

func (o *options) unpackMeta() error {
	// find the meta file
	metaFiles, err := filepath.Glob(filepath.Join(o.backupFilesPath, backup_util.Metafile+".*"))
	if err != nil {
		return err
	}

	if len(metaFiles) == 0 {
		return fmt.Errorf("no metastore backups in %s", o.backupFilesPath)
	}

	latest := metaFiles[len(metaFiles)-1]

	_, _ = fmt.Fprintf(o.Stdout, "Using metastore snapshot: %v\n", latest)
	// Read the metastore backup
	f, err := os.Open(latest)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, f); err != nil {
		return fmt.Errorf("copy: %s", err)
	}

	b := buf.Bytes()
	var i int

	// Make sure the file is actually a meta store backup file
	magic := binary.BigEndian.Uint64(b[:8])
	if magic != snapshotter.BackupMagicHeader {
		return fmt.Errorf("invalid metadata file")
	}
	i += 8

	// Size of the meta store bytes
	length := int(binary.BigEndian.Uint64(b[i : i+8]))
	i += 8
	metaBytes := b[i : i+length]
	i += length

	// Size of the node.json bytes
	length = int(binary.BigEndian.Uint64(b[i : i+8]))
	i += 8
	nodeBytes := b[i : i+length]

	// Unpack into metadata.
	var data meta.Data
	if err := data.UnmarshalBinary(metaBytes); err != nil {
		return fmt.Errorf("unmarshal: %s", err)
	}

	// Copy meta config and remove peers so it starts in single mode.
	c := o.MetaConfig
	c.Dir = o.metadir

	// Create the meta dir
	if err := os.MkdirAll(c.Dir, 0700); err != nil {
		return err
	}

	// Write node.json back to meta dir
	if err := os.WriteFile(filepath.Join(c.Dir, "node.json"), nodeBytes, 0655); err != nil {
		return err
	}

	client := meta.NewClient(c)
	if err := client.Open(); err != nil {
		return err
	}
	defer func() {
		_ = client.Close()
	}()

	// Force set the full metadata.
	if err := client.SetData(&data); err != nil {
		return fmt.Errorf("set data: %s", err)
	}

	// remove the raft.db file if it exists
	err = os.Remove(filepath.Join(o.metadir, "raft.db"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// remove the node.json file if it exists
	err = os.Remove(filepath.Join(o.metadir, "node.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return nil
}

func (o *options) unpackShard(shard uint64) error {
	shardID := strconv.FormatUint(shard, 10)
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(o.datadir, o.sourceDatabase, o.backupRetention, shardID)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("shard already present: %s", restorePath)
	}

	id, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		return err
	}

	// find the shard backup files
	pat := filepath.Join(o.backupFilesPath, fmt.Sprintf(backup_util.BackupFilePattern, o.sourceDatabase, o.backupRetention, id))
	return o.unpackFiles(pat + ".*")
}

func (o *options) unpackRetention() error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(o.datadir, o.sourceDatabase, o.backupRetention)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("retention already present: %s", restorePath)
	}

	// find the retention backup files
	pat := filepath.Join(o.backupFilesPath, o.sourceDatabase)
	return o.unpackFiles(fmt.Sprintf("%s.%s.*", pat, o.backupRetention))
}

func (o *options) unpackDatabase() error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(o.datadir, o.sourceDatabase)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("database already present: %s", restorePath)
	}

	// find the database backup files
	pat := filepath.Join(o.backupFilesPath, o.sourceDatabase)
	return o.unpackFiles(pat + ".*")
}

func (o *options) unpackFiles(pat string) error {
	o.StdoutLogger.Printf("Restoring offline from backup %s\n", pat)

	backupFiles, err := filepath.Glob(pat)
	if err != nil {
		return err
	}

	if len(backupFiles) == 0 {
		return fmt.Errorf("no backup files for %s in %s", pat, o.backupFilesPath)
	}

	for _, fn := range backupFiles {
		if err := o.unpackTar(fn); err != nil {
			return err
		}
	}

	return nil
}

func (o *options) unpackTar(tarFile string) error {
	f, err := os.Open(tarFile)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	// should get us ["db","rp", "00001", "00"]
	pathParts := strings.Split(filepath.Base(tarFile), ".")
	if len(pathParts) != 4 {
		return fmt.Errorf("backup tarfile name incorrect format")
	}

	shardPath := filepath.Join(o.datadir, pathParts[0], pathParts[1], strings.Trim(pathParts[2], "0"))
	_ = os.MkdirAll(shardPath, 0755)

	return tarstream.Restore(f, shardPath)
}
