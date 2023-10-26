package transfer

import (
	"fmt"
	"strconv"
)

func (tf *Transfer) CompareTable() ([][]string, error) {
	// 创建运行日志目录
	multiWriter, _, clean, err := CreateMultiWriter()
	if err != nil {
		return nil, err
	}
	defer clean()
	log.SetOutput(multiWriter)

	// 以下开始调用比对表行数的方法

	// 用于控制协程goroutine运行时候的并发数,例如3个一批，3个一批的goroutine并发运行
	ch := make(chan struct{}, tf.cfg.MaxParallel)

	var dbRowsSlice [][]string
	//遍历tableMap
	for _, table := range tf.TableMap() { //获取单个表名
		ch <- struct{}{}
		wg2.Add(1)
		go func(tableName string) {
			// 把每个单行切片追加到用于表格输出的切片里面
			dbRowsSlice = append(dbRowsSlice, tf.compareTable(tableName, ch))
		}(table.Name)
	}
	// 这里等待上面所有迁移数据的goroutine协程任务完成才会接着运行下面的主程序，如果这里不wait，上面还在迁移行数据的goroutine会被强制中断
	wg2.Wait()
	return dbRowsSlice, nil
}

func (tf *Transfer) compareTable(tableName string, ch chan struct{}) []string {
	var (
		srcRows  int      // 源表行数
		destRows int      // 目标行数
		ret      []string // 比对结果切片
	)
	isOk := "YES"        // 行数是否相同
	destIsExist := "YES" // 目标表是否存在
	defer wg2.Done()
	// 查询源库表行数
	srcSql := fmt.Sprintf("select count(*) from `%s`", tableName)
	err := tf.srcDb.QueryRow(srcSql).Scan(&srcRows)
	if err != nil {
		log.Error(err)
	}
	// 查询目标表行数
	destSql := fmt.Sprintf("select count(*) from \"%s\"", tableName)
	err = tf.destDb.QueryRow(destSql).Scan(&destRows)
	if err != nil {
		log.Error(err)
		isOk, destIsExist = "NO", "NO" // 查询失败就是目标表不存在
	}
	if srcRows != destRows {
		isOk = "NO"
	}
	// 单行比对结果的切片
	ret = append(ret, tableName, strconv.Itoa(srcRows), strconv.Itoa(destRows), destIsExist, isOk)
	<-ch
	return ret
}
