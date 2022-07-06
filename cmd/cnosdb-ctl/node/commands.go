package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/cnosdb/cnosdb/cmd/cnosdb-ctl/options"
	"github.com/cnosdb/cnosdb/meta"
	"github.com/cnosdb/cnosdb/pkg/network"
	"github.com/cnosdb/cnosdb/server"

	"github.com/spf13/cobra"
)

func GetShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show",
		Short:   "shows cluster nodes",
		Long:    `Shows all meta nodes and data nodes that are part of the cluster.`,
		Example: `  cnosdb-ctl --bind 127.0.0.1:8091 show`,
		PreRun: func(cmd *cobra.Command, args []string) {
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			peers, err := getMetaServers(options.Env.Bind)
			if err != nil {
				return err
			}

			if len(peers) == 0 {
				return ErrEmptyPeers
			}

			metaClient := meta.NewRemoteClient()
			metaClient.SetMetaServers(peers)
			if err := metaClient.Open(); err != nil {
				return err
			}
			defer metaClient.Close()

			metaNodes, err := metaClient.MetaNodes()
			if err != nil {
				return err
			}
			dataNodes, err := metaClient.DataNodes()
			if err != nil {
				return err
			}

			fmt.Fprint(cmd.OutOrStdout(), "Data Nodes:\n==========\n")
			for _, n := range dataNodes {
				fmt.Fprintln(cmd.OutOrStdout(), n.ID, "    ", n.TCPHost)
			}
			fmt.Fprintln(cmd.OutOrStdout(), "")

			fmt.Fprint(cmd.OutOrStdout(), "Meta Nodes:\n==========\n")
			for _, n := range metaNodes {
				fmt.Fprintln(cmd.OutOrStdout(), n.ID, "    ", n.Host)
			}
			fmt.Fprintln(cmd.OutOrStdout(), "")

			return nil
		},
	}
}

func GetAddMetaCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "add-meta",
		Short:   "adds a meta node to a cluster",
		Long:    "Adds a meta node to a cluster.",
		Example: "  cnosdb-ctl --bind 127.0.0.1:8091 add-meta localhost:8091",
		PreRun: func(cmd *cobra.Command, args []string) {
		},
		Run: func(cmd *cobra.Command, args []string) {
			remoteNodeAddr := args[0]
			err := addMetaServer(options.Env.Bind, remoteNodeAddr)
			if err != nil {
				fmt.Println(err)
			}
		},
	}
}

func GetRemoveMetaCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "remove-meta",
		Short:   "removes a data node from a cluster",
		Long:    "Removes a meta node from a cluster.",
		Example: "  cnosdb-ctl --bind 127.0.0.1:8091 remove-meta localhost:8091",
		PreRun: func(cmd *cobra.Command, args []string) {
		},
		Run: func(cmd *cobra.Command, args []string) {
			remoteNodeAddr := args[0]
			err := removeMetaServer(options.Env.Bind, remoteNodeAddr)
			if err != nil {
				fmt.Println(err)
			}
		},
	}
}

func GetAddDataCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "add-data",
		Short:   "adds a data node to a cluster",
		Long:    "Adds a data node to a cluster.",
		Example: "  cnosdb-ctl --bind 127.0.0.1:8091 add-data localhost:8088",
		PreRun: func(cmd *cobra.Command, args []string) {
		},
		Run: func(cmd *cobra.Command, args []string) {
			remoteNodeAddr := args[0]
			err := addDataServer(options.Env.Bind, remoteNodeAddr)
			if err != nil {
				fmt.Println(err)
			}
		},
	}
}

func GetRemoveDataCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "remove-data",
		Short:   "removes a data node from a cluster",
		Long:    "Removes a data node from a cluster.",
		Example: "  cnosdb-ctl --bind 127.0.0.1:8091 remove-data localhost:8088",
		PreRun: func(cmd *cobra.Command, args []string) {
		},
		Run: func(cmd *cobra.Command, args []string) {
			remoteNodeAddr := args[0]
			err := removeDataServer(options.Env.Bind, remoteNodeAddr)
			if err != nil {
				fmt.Println(err)
			}
		},
	}
}

func GetUpdateDataCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "update-data",
		Short:   "update old data node with o new node",
		Long:    "update-data",
		Example: "  cnosdb-ctl --bind 127.0.0.1:8091 update-data old-data-address new-data-address",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return errors.New("Input parameters count not right, MUST be 2")
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			err := updateDataNode(options.Env.Bind, args[0], args[1])
			if err != nil {
				fmt.Println(err)
			}
		},
	}
}

func GetReplaceDataCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "replace-data",
		Short:   "replace a data node address with a new node",
		Long:    "replace a data node address with a new node",
		Example: "  cnosdb-ctl --bind 127.0.0.1:8091 replace-data old-data-address new-data-address",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return errors.New("Input parameters count not right, MUST be 2")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			srcAddr := args[0]
			destAddr := args[1]
			request := &server.NodeRequest{
				Type:     server.RequestReplaceDataNode,
				NodeAddr: destAddr,
			}

			conn, err := network.Dial("tcp", srcAddr, server.NodeMuxHeader)
			if err != nil {
				return err
			}
			defer conn.Close()

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
