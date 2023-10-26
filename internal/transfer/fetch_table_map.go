package transfer

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/arkfra/gomysql2pg/internal/config"
)

var tableOnly bool

// 自动对表分析，然后生成每个表用来迁移查询源库SQL的集合(全表查询或者分页查询)
// 自动分析是否有排除的表名
// 最后返回map结构即 表:[查询SQL]
func (tf *Transfer) FetchTableMap() (tableMap []*config.Table) {
	var tableNumber int // 表总数
	var sqlStr string   // 查询源库获取要迁移的表名
	// 声明一个等待组
	var wg sync.WaitGroup
	// 使用互斥锁 sync.Mutex才能使用并发的goroutine
	mutex := &sync.Mutex{}
	log.Info("exclude table ", tf.cfg.Exclude)
	// 如果配置文件中exclude存在表名，使用not in排除掉这些表，否则获取到所有表名
	if tf.cfg.Exclude != nil {
		sqlStr = "select table_name from information_schema.tables where table_schema=database() and table_type='BASE TABLE' and table_name not in ("
		buffer := bytes.NewBufferString("")
		for index, tabName := range tf.cfg.Exclude {
			if index < len(tf.cfg.Exclude)-1 {
				buffer.WriteString("'" + tabName + "'" + ",")
			} else {
				buffer.WriteString("'" + tabName + "'" + ")")
			}
		}
		sqlStr += buffer.String()
	} else {
		sqlStr = "select table_name from information_schema.tables where table_schema=database() and table_type='BASE TABLE';" // 获取库里全表名称
	}
	// 查询下源库总共的表，获取到表名
	rows, err := tf.srcDb.Query(sqlStr)
	if err != nil {
		log.Error(fmt.Sprintf("Query "+sqlStr+" failed,\nerr:%v\n", err))
		return
	}
	defer rows.Close()
	var tableName string
	// 初始化外层的map，键值对，即 表名:[sql语句...]
	tableMap = make([]*config.Table, 0, 10)
	for rows.Next() {
		tableNumber++
		// 每一个任务开始时, 将等待组增加1
		wg.Add(1)
		var sqlFullList []string
		err = rows.Scan(&tableName)
		if err != nil {
			log.Error(err)
		}
		// 使用多个并发的goroutine调用函数获取该表用来执行的sql语句
		log.Info(time.Now().Format("2006-01-02 15:04:05.000000"), "ID[", tableNumber, "] ", "prepare ", tableName, " TableMap")
		go func(tableName string, sqlFullList []string) {
			// 使用defer, 表示函数完成时将等待组值减1
			defer wg.Done()
			// !tableOnly 即没有指定-t选项，生成全库的分页查询语句，否则就是指定了-t选项, sqlFullList 仅追加空字符串
			if !tableOnly {
				sqlFullList = tf.prepareSqlStr(tableName, tf.cfg.PageSize)
			} else {
				sqlFullList = append(sqlFullList, "")
			}

			tasks := make([]*config.Task, 0, len(sqlFullList))
			for i := 0; i < len(sqlFullList); i++ {
				tasks = append(tasks, &config.Task{Query: sqlFullList[i]})
			}

			mutex.Lock()
			// 追加到内层的切片，sql全表扫描语句或者分页查询语句，例如tableMap[test1]="select * from test1"
			tableMap = append(tableMap, &config.Table{Name: tableName, Tasks: tasks})
			mutex.Unlock()
		}(tableName, sqlFullList)
	}

	// 等待所有的任务完成
	wg.Wait()
	return tableMap
}
