package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/urfave/cli/v3"
)

var ver = "0.2.3"

var versionCmd = &cli.Command{
	Name:        "version",
	Description: "Print the version number of gomysql2pg",
	Action: func(cmd *cli.Context) error {
		fmt.Println("\n\nyour version v" + ver)
		os.Exit(0)
		return nil
	},
}

func Info() {
	colorStr := color.New()
	colorStr.Add(color.FgHiGreen)
	colorStr.Printf("gomysql2pg\n")
	colorStr.Printf("Powered By: DBA Team Of Infrastructure Research Center \nRelease version v" + ver)
	time.Sleep(5 * 100 * time.Millisecond)
	fmt.Printf("\n")
}
