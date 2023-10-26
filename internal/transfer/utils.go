package transfer

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func (tf Transfer) ErrDataCount() int {
	return tf.errDataCount
}

// 处理全局变量通道，responseChannel，在协程的这个通道里遍历获取到copy方法失败的计数
func (tf *Transfer) Response() {
	for rc := range responseChannel {
		fmt.Println("response:", rc)
		tf.errDataCount += 1
	}
}

// 创建运行日志目录
func CreateMultiWriter() (io.Writer, string, func(), error) {
	logDir, _ := filepath.Abs(createDateDir(""))
	f, err := os.OpenFile(logDir+"/"+"run.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, "", func() {}, err
	}
	// log信息重定向到平面文件
	return io.MultiWriter(os.Stdout, f), logDir, func() {
		if err := f.Close(); err != nil {
			log.Fatal(err) // 或设置到函数返回值中
		}
	}, nil
}

// createDateDir 根据当前日期来创建文件夹
func createDateDir(basePath string) string {
	folderName := "log/" + time.Now().Format("2006_01_02_15_04_05")
	folderPath := filepath.Join(basePath, folderName)
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		err := os.MkdirAll(folderPath, 0777) //级联创建目录
		if err != nil {
			fmt.Println("create directory log failed ", err)
		}
	}
	return folderPath
}
