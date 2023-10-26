package cmd

import (
	"fmt"
	"time"

	"github.com/arkfra/gomysql2pg/internal/transfer"
	"github.com/liushuochen/gotable"
	"github.com/urfave/cli/v3"
)

func compareDbCmd(tf *transfer.Transfer) *cli.Command {
	return &cli.Command{
		Name:        "compareDb",
		Description: "Compare entire source and target database table rows",
		Action: func(cmd *cli.Context) error {
			start := time.Now()
			dbRowsSlice, err := tf.CompareTable()
			log.Warn(err.Error())
			cost := time.Since(start)

			// 输出全库数量的表
			tableTotal, err := gotable.Create("Table", "SourceRows", "DestRows", "DestIsExist", "isOk")
			if err != nil {
				return err
			}
			// 输出比对信息失败的表
			tableFailed, err := gotable.Create("Table", "SourceRows", "DestRows", "DestIsExist", "isOk")
			if err != nil {
				return fmt.Errorf("create table failed: %s", err.Error())
			}
			for _, r := range dbRowsSlice {
				if r[4] == "NO" {
					_ = tableFailed.AddRow(r)
				}
				_ = tableTotal.AddRow(r)
			}

			fmt.Println("Table Compare Total Result")
			tableTotal.Align("Table", 1)
			tableTotal.Align("SourceRows", 1)
			tableTotal.Align("DestRows", 1)
			tableTotal.Align("isOk", 1)
			tableTotal.Align("DestIsExist", 1)
			fmt.Println(tableTotal)
			tableFailed.Align("Table", 1)
			tableFailed.Align("SourceRows", 1)
			tableFailed.Align("DestRows", 1)
			tableFailed.Align("isOk", 1)
			tableFailed.Align("DestIsExist", 1)
			fmt.Println("Table Compare Result (Only Not Ok Displayed)")
			fmt.Println(tableFailed)
			fmt.Println("Table Compare finish elapsed time ", cost)

			return nil
		},
	}
}
