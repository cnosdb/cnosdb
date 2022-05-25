package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/cnosdb/cnosdb/cmd/cnosdb-ctl/node"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-ctl/options"
	"github.com/cnosdb/cnosdb/cmd/cnosdb-ctl/shard"
	"github.com/cnosdb/cnosdb/meta"

	"github.com/spf13/cobra"
)

var (
	version string
)

func init() {
	if version == "" {
		version = "unknown"
	}
}

func main() {
	mainCmd := GetCommand()
	mainCmd.AddCommand(node.GetShowCommand())
	mainCmd.AddCommand(node.GetAddMetaCommand())
	mainCmd.AddCommand(node.GetRemoveMetaCommand())
	mainCmd.AddCommand(node.GetAddDataCommand())
	mainCmd.AddCommand(node.GetRemoveDataCommand())
	mainCmd.AddCommand(node.GetUpdateDataCommand())
	mainCmd.AddCommand(shard.GetCopyShardCommand())
	mainCmd.AddCommand(shard.GetRemoveShardCommand())
	mainCmd.AddCommand(printVersion())
	mainCmd.AddCommand(printMetaData())

	if err := mainCmd.Execute(); err != nil {
		fmt.Printf("Error : %+v\n", err)
	}
}

func printVersion() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Displays the CnosDB meta version",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableNoDescFlag:   true,
			DisableDescriptions: true,
		},
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("cnosdb-cli: v%s \n", version)
		},
	}
}

func GetCommand() *cobra.Command {
	c := &cobra.Command{
		Use:  "cnosdb-ctl",
		Long: "The 'cnosdb-ctl' command is used for managing CnosDB clusters.",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableDescriptions: true,
			DisableNoDescFlag:   true,
		},
	}

	c.PersistentFlags().StringVar(&options.Env.Bind, "bind", "127.0.0.1:8091", "")

	return c
}

func printMetaData() *cobra.Command {
	return &cobra.Command{
		Use:   "print-meta",
		Short: "Displays the CnosDB meta data",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd:   true,
			DisableNoDescFlag:   true,
			DisableDescriptions: true,
		},
		Run: func(cmd *cobra.Command, args []string) {
			url := fmt.Sprintf("http://%s/metajson", options.Env.Bind)
			rsp, err := http.Get(url)
			if err != nil {
				fmt.Printf("%s\n", err.Error())
				return
			}

			data := meta.Data{}
			if err := json.NewDecoder(rsp.Body).Decode(&data); err != nil {
				fmt.Printf("%s\n", err.Error())
				return
			}

			fmt.Printf("|--Cluster  Term: %d Index: %d ClusterID: %d MaxNodeID: %d MaxGroupID: %d MaxhardID: %d\n",
				data.Term, data.Index, data.ClusterID, data.MaxNodeID, data.MaxShardGroupID, data.MaxShardID)
			for _, node := range data.MetaNodes {
				fmt.Printf("|--Meta  ID: %d Host: %s TCPHost: %s\n", node.ID, node.Host, node.TCPHost)
			}

			for _, node := range data.DataNodes {
				fmt.Printf("|--Data  ID: %d Host: %s TCPHost: %s\n", node.ID, node.Host, node.TCPHost)
			}

			for _, db := range data.Databases {
				if db.Name == "_internal" {
					continue
				}

				fmt.Printf("|--DataBase  Name: %s DefaultRetentionPolicy: %s\n", db.Name, db.DefaultRetentionPolicy)
				for _, rp := range db.RetentionPolicies {
					fmt.Printf("|    |--RetentionPolicy  Name: %s Replica: %d Duration: %d ShardGroupDuration: %d\n",
						rp.Name, rp.ReplicaN, rp.Duration/time.Second, rp.ShardGroupDuration/time.Second)

					for _, sg := range rp.ShardGroups {
						fmt.Printf("|    |    |--ShardGroup  ID: %d Start: %s End: %s Delete: %s Truncate: %s\n", sg.ID,
							sg.StartTime.Format("2006-01-02 15:04:05"),
							sg.EndTime.Format("2006-01-02 15:04:05"),
							sg.DeletedAt.Format("2006-01-02 15:04:05"),
							sg.TruncatedAt.Format("2006-01-02 15:04:05"))
						for _, shard := range sg.Shards {
							fmt.Printf("|    |    |    |--Shard  ID: %d Owners: %v\n", shard.ID, shard.Owners)
						}
					}
				}
			}
		},
	}
}
