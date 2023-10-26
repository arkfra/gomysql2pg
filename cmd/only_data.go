package cmd

import (
	"fmt"
	"strconv"
	"time"

	"github.com/arkfra/gomysql2pg/internal/transfer"
	"github.com/liushuochen/gotable"
	"github.com/urfave/cli/v3"
)

func onlyDataCmd(tf *transfer.Transfer) *cli.Command {
	return &cli.Command{
		Name:        "onlyData",
		Description: "only transfer table data rows",
		Action: func(cmd *cli.Context) error {
			migDataStart := time.Now()
			tf.SycnData()
			migDataEnd := time.Now()
			migCost := time.Since(migDataEnd)

			tableDataRet := []string{
				"TableData",
				migDataStart.Format("2006-01-02 15:04:05.000000"),
				migDataEnd.Format("2006-01-02 15:04:05.000000"),
				strconv.Itoa(tf.ErrDataCount()),
				migCost.String(),
			}
			// 输出配置文件信息
			tblConfig, err := gotable.Create("SourceDb", "DestDb", "MaxParallel", "PageSize")
			if err != nil {
				return fmt.Errorf("create tblConfig failed: %s", err.Error())
			}
			ymlConfig := []string{
				tf.SrcUri().Host + "-" + tf.SrcUri().Path,
				tf.DestUri().Host + "-" + tf.DestUri().Path,
				strconv.Itoa(tf.Cfg().MaxParallel),
				strconv.Itoa(tf.Cfg().PageSize),
			}
			tblConfig.AddRow(ymlConfig)
			fmt.Println(tblConfig)
			// 数据库对象迁移后信息
			table, err := gotable.Create("Object", "BeginTime", "EndTime", "DataErrorCount", "ElapsedTime")
			if err != nil {
				return fmt.Errorf("create table failed: %s", err.Error())
			}
			table.AddRow(tableDataRet)
			table.Align("Object", 1)
			table.Align("DataErrorCount", 1)
			table.Align("ElapsedTime", 1)
			fmt.Println(table)
			log.Info(fmt.Sprintf("All Table Data Finish Total Elapsed Time %s", migCost))
			return nil
		},
	}
}
