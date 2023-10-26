package main

import (
	"context"
	"fmt"
	"os"

	"github.com/arkfra/gomysql2pg/cmd"
)

func main() {
	rootCmd := cmd.NewRoot()
	if err := rootCmd.Run(context.Background(), os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
