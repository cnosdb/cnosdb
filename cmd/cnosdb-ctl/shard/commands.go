package shard

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/cnosdb/cnosdb/cmd/cnosdb-ctl/options"
	"github.com/cnosdb/cnosdb/meta"
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

func GetCopyShardStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "copy-shard-status",
		Short:   "show copy shard status",
		Long:    "show copy shard status",
		Example: "  cnosdb-ctl copy-shard-status --bind 127.0.0.1:8888 ",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},

		RunE: func(cmd *cobra.Command, args []string) error {
			nodes, err := getDataNodesInfo(options.Env.Bind)
			if err != nil {
				return err
			}

			fmt.Printf("Source \t Dest \t Database \t Policy \t ShardID \t Status\t StartedAt\n")
			for _, node := range nodes {
				request := &snapshotter.Request{
					Type: snapshotter.RequestCopyShardStatus,
				}

				conn, err := network.Dial("tcp", node.TCPHost, snapshotter.MuxHeader)
				if err != nil {
					continue
				}
				defer conn.Close()

				_, err = conn.Write([]byte{byte(request.Type)})
				if err != nil {
					continue
				}

				if err := json.NewEncoder(conn).Encode(request); err != nil {
					continue
				}

				rsp := snapshotter.CopyShardInfo{}
				if err := json.NewDecoder(conn).Decode(&rsp); err != nil {
					continue
				}

				fmt.Printf("%s \t %s \t %s \t %s \t %d \t %s \t %s\n",
					rsp.SrcHost, rsp.DestHost, rsp.Database, rsp.Retention,
					rsp.ShardID, rsp.Status, rsp.StartTime.String())
			}

			return nil
		},
	}
}

func GetKillCopyShardCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "kill-copy-shard",
		Short:   "kill copy shard",
		Long:    "kill copy shard",
		Example: "  cnosdb-ctl kill-copy-shard 127.0.0.1:8088 127.0.0.2:8088 1234",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 3 {
				return errors.New("Input parameters count not right, MUST be 3")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			shardID, err := strconv.ParseUint(args[2], 10, 64)
			if err != nil {
				return err
			}

			request := &snapshotter.Request{
				Type:              snapshotter.RequestKillCopyShard,
				ShardID:           shardID,
				CopyShardDestHost: args[1],
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

func getDataNodesInfo(metaAddr string) ([]meta.NodeInfo, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/datanodes", metaAddr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(string(b))
	}

	var nodes []meta.NodeInfo
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil, err
	}

	return nodes, err
}
