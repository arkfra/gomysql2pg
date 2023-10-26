package transfer

import (
	"fmt"
	"time"

	"github.com/fatih/color"
)

const Version = "0.2.3"

func Info() {
	colorStr := color.New()
	colorStr.Add(color.FgHiGreen)
	colorStr.Printf("gomysql2pg\n")
	colorStr.Printf("Powered By: DBA Team Of Infrastructure Research Center \nRelease version v" + Version)
	time.Sleep(5 * 100 * time.Millisecond)
	fmt.Printf("\n")
}
