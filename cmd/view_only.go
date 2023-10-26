package cmd

import (
	"github.com/arkfra/gomysql2pg/internal/transfer"
	"github.com/urfave/cli/v3"
)

func viewOnlyCmd(tf *transfer.Transfer) *cli.Command {
	return &cli.Command{
		Name:        "viewOnly",
		Description: "Create view",
		Action: func(cmd *cli.Context) error {
			// 创建运行日志目录
			multiWriter, logDir, clean, err := transfer.CreateMultiWriter()
			if err != nil {
				return err
			}
			defer clean()
			log.SetOutput(multiWriter)
			// 实例初始化，调用接口中创建目标表的方法
			var db transfer.Database = &transfer.Table{Transfer: tf}
			db.ViewCreate(logDir)
			return nil
		},
	}

}
