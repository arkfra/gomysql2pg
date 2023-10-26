package transfer

func (tf *Transfer) SycnData() error {
	// 创建运行日志目录
	multiWriter, logDir, clean, err := CreateMultiWriter()
	if err != nil {
		return err
	}
	defer clean()
	log.SetOutput(multiWriter)
	// 创建表之后，开始准备迁移表行数据
	// 同时执行goroutine的数量，这里是每个表查询语句切片集合的长度
	var goroutineSize int
	//遍历每个表需要执行的切片查询SQL，累计起来获得总的goroutine并发大小
	for _, table := range tf.TableMap() {
		goroutineSize += len(table.Parts())
	}
	// 每个goroutine运行开始以及结束之后使用的通道，主要用于控制内层的goroutine任务与外层main线程的同步，即主线程需要等待子任务完成
	//ch := make(chan int, goroutineSize)  //v0.1.4及之前的版本通道使用的通道，配合下面for循环遍历行数据迁移失败的计数
	// 用于控制协程goroutine运行时候的并发数,例如3个一批，3个一批的goroutine并发运行
	ch := make(chan struct{}, tf.cfg.MaxParallel)
	// 在协程里运行函数response，主要是从下面调用协程go runMigration的时候获取到里面迁移行数据失败的数量
	go tf.Response()
	//遍历tableMap，先遍历表，再遍历该表的sql切片集合
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
			log.Info("table not exists")
		}
	}
	// v0.1.4版本及之前通过循环获取ch通道里写的int数据判断是否有迁移行数据失败的表，如果通道里发送的数据是2说明copy失败了
	// 这里是等待上面所有goroutine任务完成，才会执行for循环下面的动作
	//migDataFailed := 0
	//for i := 0; i < goroutineSize; i++ {
	//	migDataRet := <-ch
	//	log.Info("goroutine[", i, "]", " finish ", time.Now().Format("2006-01-02 15:04:05.000000"))
	//	if migDataRet == 2 {
	//		migDataFailed += 1
	//	}
	//}
	// 这里等待上面所有迁移数据的goroutine协程任务完成才会接着运行下面的主程序，如果这里不wait，上面还在迁移行数据的goroutine会被强制中断
	wg.Wait()
	return nil
}
