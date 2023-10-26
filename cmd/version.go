package cmd

import (
	"fmt"
	"os"

	"github.com/arkfra/gomysql2pg/internal/transfer"
	"github.com/urfave/cli/v3"
)

var versionCmd = &cli.Command{
	Name:        "version",
	Description: "Print the version number of gomysql2pg",
	Action: func(cmd *cli.Context) error {
		fmt.Println("\n\nyour version v" + transfer.Version)
		os.Exit(0)
		return nil
	},
}
