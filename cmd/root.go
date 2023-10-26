package cmd

import (
	"fmt"
	"os"
	"path"

	"github.com/arkfra/gomysql2pg/internal/config"
	"github.com/arkfra/gomysql2pg/internal/transfer"
	"github.com/mitchellh/go-homedir"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
	"gopkg.in/yaml.v3"
)

var log = logrus.New()

// initConfig reads in config file and ENV variables if set.
func NewRoot() *cli.Command {
	// Find home directory.
	home, err := homedir.Dir()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var tf *transfer.Transfer

	// rootCmd represents the base command when called without any subcommands
	rootCmd := &cli.Command{
		Name: os.Args[0],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "config",
				Usage: "config file (default is ~/.config/gomysql2pg/default.yaml)",
				Value: path.Join(home, ".config", "gomysql2pg", "default.yaml"),
			},
			&cli.BoolFlag{
				Name:    "selFromYml",
				Aliases: []string{"s"},
				Usage:   "select from yml true",
			},
			&cli.BoolFlag{
				Name:    "tableOnly",
				Aliases: []string{"t"},
				Usage:   "only create table true",
			},
		},
		Before: func(ctx *cli.Context) (err error) {
			var (
				cfg  *config.Transfer
				data []byte
			)
			data, err = os.ReadFile(ctx.String("config"))
			if err != nil {
				return
			}
			if err = yaml.Unmarshal(data, &cfg); err != nil {
				return
			}

			if cfg.MaxParallel == 0 {
				cfg.MaxParallel = 20
			}

			tf, err = transfer.NewTransfer(cfg)
			if err != nil {
				panic(err)
			}

			tf.SetSelFromYml(ctx.Bool("selFromYml"))

			return nil
		},
		Action: func(cmd *cli.Context) error {
			// 获取配置文件中的数据库连接字符串
			tf.Mysql2pg()
			return nil
		},
	}

	rootCmd.Commands = append(rootCmd.Commands,
		compareDbCmd(tf),
		createTableCmd(tf), seqOnlyCmd(tf), idxOnlyCmd(tf),
		viewOnlyCmd(tf), onlyDataCmd(tf),
		versionCmd,
	)

	return rootCmd
}
