package transfer

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/liushuochen/gotable"
)

func (tf *Transfer) Mysql2pg() error {
	// 自动侦测终端是否输入Ctrl+c,若按下,主动关闭数据库查询
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGTERM)
	go tf.exitHandle(exitChan)

	// 创建运行日志目录
	// 输出调用文件以及方法位置
	multiWriter, logDir, clean, err := CreateMultiWriter()
	if err != nil {
		return err
	}
	defer clean()
	log.SetOutput(multiWriter)
	start := time.Now()
	log.Info("running MySQL check connect")
	// 每页的分页记录数,仅全库迁移时有效
	log.Info("running Postgres check connect")
	// 实例初始化，调用接口中创建目标表的方法
	var db Database = new(Table)
	// 用于控制协程goroutine运行时候的并发数,例如3个一批，3个一批的goroutine并发运行
	ch := make(chan struct{}, tf.cfg.MaxParallel)
	startTbl := time.Now()
	for _, table := range tf.TableMap() { //获取单个表名
		ch <- struct{}{}
		wg2.Add(1)
		go db.TableCreate(logDir, table.Name, ch)
	}
	wg2.Wait()
	endTbl := time.Now()
	tableCost := time.Since(startTbl)
	// 创建表完毕
	log.Info("Table structure synced from MySQL to PostgreSQL ,Source Table Total ", tableCount, " Failed Total ", strconv.Itoa(failedCount))
	tabRet = append(tabRet, "Table", startTbl.Format("2006-01-02 15:04:05.000000"), endTbl.Format("2006-01-02 15:04:05.000000"), strconv.Itoa(failedCount), tableCost.String())
	fmt.Println("Table Create finish elapsed time ", tableCost)
	// 创建表之后，开始准备迁移表行数据
	// 同时执行goroutine的数量，这里是每个表查询语句切片集合的长度
	var goroutineSize int
	//遍历每个表需要执行的切片查询SQL，累计起来获得总的goroutine并发大小，即所有goroutine协程的数量
	for _, table := range tf.TableMap() {
		goroutineSize += len(table.Parts())
	}
	// 每个goroutine运行开始以及结束之后使用的通道，主要用于控制内层的goroutine任务与外层main线程的同步，即主线程需要等待子任务完成
	// ch := make(chan int, goroutineSize)  //v0.1.4及之前的版本通道使用的通道，配合下面for循环遍历行数据迁移失败的计数
	// 在协程里运行函数response，主要是从下面调用协程go runMigration的时候获取到里面迁移行数据失败的数量
	go tf.Response()
	//遍历tableMap，先遍历表，再遍历该表的sql切片集合
	migDataStart := time.Now()
	for _, table := range tf.TableMap() { //获取单个表名
		sqlFullSplit := table.Parts()
		colName, colType, tableNotExist := tf.PreMigData(table.Name, sqlFullSplit) //获取单表的列名，列字段类型
		if !tableNotExist {                                                        //目标表存在就执行数据迁移
			// 遍历该表的sql切片(多个分页查询或者全表查询sql)
			for index, sqlSplitSql := range sqlFullSplit {
				ch <- struct{}{} //在没有被接收的情况下，至多发送n个消息到通道则被阻塞，若缓存区满，则阻塞，这里相当于占位置排队
				wg.Add(1)        // 每运行一个goroutine等待组加1
				go tf.RunMigration(logDir, index, table.Name, sqlSplitSql, ch, colName, colType)
			}
		} else { //目标表不存在就往通道写1
			log.Info("table not exists ", table.Name)
		}
	}
	// 单独计算迁移表行数据的耗时
	migDataEnd := time.Now()
	// 这里等待上面所有迁移数据的goroutine协程任务完成才会接着运行下面的主程序，如果这里不wait，上面还在迁移行数据的goroutine会被强制中断
	wg.Wait()
	migCost := migDataEnd.Sub(migDataStart)
	// v0.1.4版本之前通过循环获取ch通道里写的int数据判断是否有迁移行数据失败的表，如果通道里发送的数据是2说明copy失败了
	//migDataFailed := 0
	// 这里是等待上面所有goroutine任务完成，才会执行for循环下面的动作
	//for i := 0; i < goroutineSize; i++ {
	//	migDataRet := <-ch
	//	log.Info("goroutine[", i, "]", " finish ", time.Now().Format("2006-01-02 15:04:05.000000"))
	//	if migDataRet == 2 {
	//		migDataFailed += 1
	//	}
	//}
	tableDataRet := []string{
		"TableData",
		migDataStart.Format("2006-01-02 15:04:05.000000"),
		migDataEnd.Format("2006-01-02 15:04:05.000000"),
		strconv.Itoa(tf.errDataCount),
		migCost.String(),
	}
	// 数据库对象的迁移结果
	var rowsAll = [][]string{{}}
	// 表结构创建以及数据迁移结果追加到切片,进行整合
	rowsAll = append(rowsAll, tabRet, tableDataRet)
	// 如果指定-s模式不创建下面对象
	if !tf.selFromYml {
		// 创建序列
		seqRet := db.SeqCreate(logDir)
		// 创建索引、约束
		idxRet := db.IdxCreate(logDir)
		// 创建外键
		fkRet := db.FKCreate(logDir)
		// 创建视图
		viewRet := db.ViewCreate(logDir)
		// 创建触发器
		triRet := db.TriggerCreate(logDir)
		// 以上对象迁移结果追加到切片,进行整合
		rowsAll = append(rowsAll, seqRet, idxRet, fkRet, viewRet, triRet)
	}
	// 输出配置文件信息
	fmt.Println("------------------------------------------------------------------------------------------------------------------------------")
	Info()
	tblConfig, err := gotable.Create("SourceDb", "DestDb", "MaxParallel", "PageSize", "ExcludeCount")
	if err != nil {
		fmt.Println("Create tblConfig failed: ", err.Error())
		return err
	}
	ymlConfig := []string{
		tf.srcUri.Host + "-" + tf.srcUri.Path,
		tf.destUri.Host + "-" + tf.destUri.Path,
		strconv.Itoa(tf.cfg.MaxParallel),
		strconv.Itoa(tf.cfg.PageSize),
		strconv.Itoa(len(tf.cfg.Exclude)),
	}
	tblConfig.AddRow(ymlConfig)
	fmt.Println(tblConfig)
	// 输出迁移摘要
	table, err := gotable.Create("Object", "BeginTime", "EndTime", "FailedTotal", "ElapsedTime")
	if err != nil {
		fmt.Println("Create table failed: ", err.Error())
		return err
	}
	for _, r := range rowsAll {
		_ = table.AddRow(r)
	}
	table.Align("Object", 1)
	table.Align("FailedTotal", 1)
	table.Align("ElapsedTime", 1)
	fmt.Println(table)
	// 总耗时
	cost := time.Since(start)
	log.Info(fmt.Sprintf("All complete totalTime %s The Report Dir %s", cost, logDir))
	return err
}
