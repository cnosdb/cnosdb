package node

import (
	"fmt"

	"github.com/cnosdatabase/cnosdb/cmd/cnosdb-ctl/options"
	"github.com/cnosdatabase/cnosdb/meta"
	"github.com/spf13/cobra"
)

func GetShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show",
		Short:   "shows cluster nodes",
		Long:    `Shows all meta nodes and data nodes that are part of the cluster.`,
		Example: `examples`,
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
		Example: "  cnosdb-ctl add-meta localhost:8091",
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
		Example: "  cnosdb-ctl remove-meta localhost:8091",
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
		Example: "  cnosdb-ctl add-data localhost:8088",
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
		Example: "  cnosdb-ctl remove-data localhost:8088",
		PreRun: func(cmd *cobra.Command, args []string) {
		},
		Run: func(cmd *cobra.Command, args []string) {
			remoteNodeAddr := args[0]
			err := remoteDataServer(options.Env.Bind, remoteNodeAddr)
			if err != nil {
				fmt.Println(err)
			}
		},
	}
}
