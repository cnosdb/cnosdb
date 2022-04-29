// Package help is the help subcommand of the cnosdb command.
package help

const usage = `
Tools for managing and querying CnosDB.

Usage: cnosdb-tools command [arguments]

The commands are:

    export               reshapes existing shards to a new shard duration
    compact-shard        fully compacts the specified shard
    gen-init             creates database and retention policy metadata 
    gen-exec             generates data
    help                 display this help message

Use "cnosdb-tools command -help" for more information about a command.
`