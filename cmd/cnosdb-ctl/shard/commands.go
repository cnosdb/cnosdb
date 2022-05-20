package shard

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/cnosdb/cnosdb/pkg/network"
	"github.com/cnosdb/cnosdb/server/snapshotter"
	"github.com/spf13/cobra"
)

func GetCopyShardCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "copy-shard",
		Short:   "copy a shard from Host1 to Host2",
		Long:    "copy a shard from Host1 to Host2",
		Example: "  cnosdb-ctl copy-shard 127.0.0.1:8088 127.0.0.2:8088 1234",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 3 {
				return errors.New("Input parameters count not right, MUST be 3")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			srcAddr := args[0]
			destAddr := args[1]
			shardID, err := strconv.ParseUint(args[2], 10, 64)
			if err != nil {
				return err
			}

			request := &snapshotter.Request{
				Type:              snapshotter.RequestCopyShard,
				CopyShardDestHost: destAddr,
				ShardID:           shardID,
			}

			conn, err := network.Dial("tcp", srcAddr, snapshotter.MuxHeader)
			if err != nil {
				return err
			}
			defer conn.Close()

			_, err = conn.Write([]byte{byte(request.Type)})
			if err != nil {
				return err
			}

			// Write the request
			if err := json.NewEncoder(conn).Encode(request); err != nil {
				return fmt.Errorf("encode snapshot request: %s", err)
			}

			bytes, _ := ioutil.ReadAll(conn)

			fmt.Printf("%s\n", string(bytes))
			return nil
		},
	}
}

func GetRemoveShardCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "remove-shard",
		Short:   "remove a shard from host",
		Long:    "remove a shard from host",
		Example: "  cnosdb-ctl remove-shard 127.0.0.1:8888 1234",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return errors.New("Input parameters count not right, MUST be 2")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			shardID, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return err
			}

			request := &snapshotter.Request{
				Type:    snapshotter.RequestRemoveShard,
				ShardID: shardID,
			}

			conn, err := network.Dial("tcp", args[0], snapshotter.MuxHeader)
			if err != nil {
				return err
			}
			defer conn.Close()

			_, err = conn.Write([]byte{byte(request.Type)})
			if err != nil {
				return err
			}

			if err := json.NewEncoder(conn).Encode(request); err != nil {
				return fmt.Errorf("encode snapshot request: %s", err)
			}

			bytes, _ := ioutil.ReadAll(conn)

			fmt.Printf("%s\n", string(bytes))
			return nil
		},
	}
}
