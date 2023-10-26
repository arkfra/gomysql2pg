package transfer

func (tf *Transfer) CreateTable() (int, int, error) {
	// 创建运行日志目录
	multiWriter, logDir, clean, err := CreateMultiWriter()
	if err != nil {
		return 0, 0, err
	}
	defer clean()
	log.SetOutput(multiWriter)
	// 实例初始化，调用接口中创建目标表的方法
	var db Database = &Table{Transfer: tf}
	// 用于控制协程goroutine运行时候的并发数,例如3个一批，3个一批的goroutine并发运行
	ch := make(chan struct{}, tf.cfg.MaxParallel)
	//遍历tableMap
	for _, table := range tf.TableMap() { //获取单个表名
		ch <- struct{}{}
		wg2.Add(1)
		go db.TableCreate(logDir, table.Name, ch)
	}
	// 这里等待上面所有迁移数据的goroutine协程任务完成才会接着运行下面的主程序，如果这里不wait，上面还在迁移行数据的goroutine会被强制中断
	wg2.Wait()

	return tableCount, failedCount, nil
}
