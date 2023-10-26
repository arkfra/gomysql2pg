package cmd

import (
	"fmt"
	"strconv"
	"time"

	"github.com/arkfra/gomysql2pg/internal/transfer"
	"github.com/urfave/cli/v3"
)

func createTableCmd(tf *transfer.Transfer) *cli.Command {
	return &cli.Command{
		Name:        "createTable",
		Description: "Create meta table and no table data rows",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "tableOnly",
				Aliases: []string{"t"},
				Usage:   "only create table true",
				Value:   false,
			},
		},
		Action: func(cmd *cli.Context) error {
			start := time.Now()
			tableCount, failedCount, err := tf.CreateTable()
			log.Warn(err.Error())
			cost := time.Since(start)

			log.Info("Table structure synced from MySQL to PostgreSQL ,Source Table Total ", tableCount, " Failed Total ", strconv.Itoa(failedCount))
			fmt.Println("Table Create finish elapsed time ", cost)
			return nil
		},
	}
}
