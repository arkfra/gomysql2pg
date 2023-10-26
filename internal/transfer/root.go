package transfer

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

var wg sync.WaitGroup
var wg2 sync.WaitGroup
var responseChannel = make(chan string, 1) // 设定为全局变量，用于在goroutine协程里接收copy行数据失败的计数

// 迁移数据前先清空目标表数据，并获取每个表查询语句的列名以及列字段类型,表如果不存在返回布尔值true
func (tf *Transfer) PreMigData(tableName string, sqlFullSplit []string) (dbCol []string, dbColType []string, tableNotExist bool) {
	var sqlCol string
	// 在写数据前，先清空下目标表数据
	truncateSql := fmt.Sprintf(`truncate table "%s"`, tableName)
	if _, err := tf.destDb.Exec(truncateSql); err != nil {
		log.Error("truncate ", tableName, " failed   ", err)
		tableNotExist = true
		return // 表不存在return布尔值
	}
	// 获取表的字段名以及类型
	// 如果指定了参数-s，就读取yml文件中配置的sql获取"自定义查询sql生成的列名"，否则按照select * 查全表获取
	if tf.selFromYml {
		sqlCol = "select * from (" + sqlFullSplit[0] + " )aa where 1=0;" // 在自定义sql外层套一个select * from (自定义sql) where 1=0
	} else {
		sqlCol = "select * from " + "`" + tableName + "`" + " where 1=0;"
	}
	rows, err := tf.srcDb.Query(sqlCol) //源库 SQL查询语句
	if err != nil {
		log.Error(fmt.Sprintf("Query "+sqlCol+" failed,\nerr:%v\n", err))
		return
	}
	defer rows.Close()
	//获取列名，这是字符串切片
	columns, err := rows.Columns()
	if err != nil {
		log.Fatal(err.Error())
	}
	//获取字段类型，看下是varchar等还是blob
	colType, err := rows.ColumnTypes()
	if err != nil {
		log.Fatal(err.Error())
	}
	// 循环遍历列名,把列名全部转为小写
	for i, value := range columns {
		dbCol = append(dbCol, strings.ToLower(value)) //由于CopyIn方法每个列都会使用双引号包围，这里把列名全部转为小写(pg库默认都是小写的列名)，这样即便加上双引号也能正确查询到列
		dbColType = append(dbColType, strings.ToUpper(colType[i].DatabaseTypeName()))
	}
	return dbCol, dbColType, tableNotExist
}

// 根据源sql查询语句，按行遍历使用copy方法迁移到目标数据库
func (tf *Transfer) RunMigration(logDir string, startPage int, tableName string, sqlStr string, ch chan struct{}, columns []string, colType []string) {
	defer wg.Done()
	log.Info(fmt.Sprintf("%v Taskid[%d] Processing TableData %v", time.Now().Format("2006-01-02 15:04:05.000000"), startPage, tableName))
	start := time.Now()
	// 直接查询,即查询全表或者分页查询(SELECT t.* FROM (SELECT id FROM test  ORDER BY id LIMIT ?, ?) temp LEFT JOIN test t ON temp.id = t.id;)
	sqlStr = "/* gomysql2pg */" + sqlStr
	// 查询源库的sql
	rows, err := tf.srcDb.Query(sqlStr) //传入参数之后执行
	if err != nil {
		log.Error(fmt.Sprintf("[exec  %v failed ] ", sqlStr), err)
		return
	}
	defer rows.Close()
	//fmt.Println(dbCol)  //输出查询语句里各个字段名称
	values := make([]sql.RawBytes, len(columns)) // 列的值切片,包含多个列,即单行数据的值
	scanArgs := make([]interface{}, len(values)) // 用来做scan的参数，将上面的列值value保存到scan
	for i := range values {                      // 这里也是取决于有几列，就循环多少次
		scanArgs[i] = &values[i] // 这里scanArgs是指向列值的指针,scanArgs里每个元素存放的都是地址
	}
	txn, err := tf.destDb.Begin() //开始一个事务
	if err != nil {
		log.Error(err)
	}
	stmt, err := txn.Prepare(pq.CopyIn(tableName, columns...)) //prepare里的方法CopyIn只是把copy语句拼接好并返回，并非直接执行copy
	if err != nil {
		log.Error("txn Prepare pq.CopyIn failed ", err)
		//ch <- 1 // 执行pg的copy异常就往通道写入1
		<-ch   // 通道向外发送
		return // 遇到CopyIn异常就直接return
	}
	var totalRow int                                   // 表总行数
	prepareValues := make([]interface{}, len(columns)) //用于给copy方法，一行数据的切片，里面各个元素是各个列字段值
	var value interface{}                              // 单个字段的列值
	for rows.Next() {                                  // 从游标里获取一行行数据
		totalRow++                   // 源表行数+1
		err = rows.Scan(scanArgs...) //scanArgs切片里的元素是指向values的指针，通过rows.Scan方法将获取游标结果集的各个列值复制到变量scanArgs各个切片元素(指针)指向的对象即values切片里，这里是一行完整的值
		//fmt.Println(scanArgs[0],scanArgs[1])
		if err != nil {
			log.Error("ScanArgs Failed ", err.Error())
		}
		// 以下for将单行的byte数据循环转换成string类型(大字段就是用byte类型，剩余非大字段类型获取的值再使用string函数转为字符串)
		for i, colValue := range values { //values是完整的一行所有列值，这里从values遍历，获取每一列的值并赋值到col变量，col是单列的列值
			//fmt.Println(i)
			if colValue == nil {
				value = nil //空值判断
			} else {
				if colType[i] == "BLOB" { //大字段类型就无需使用string函数转为字符串类型，即使用sql.RawBytes类型
					value = colValue
				} else if colType[i] == "GEOMETRY" { //gis类型的数据处理
					value = hex.EncodeToString(colValue)[8:] //golang把gis类型列数据转成16进制字符串后，会在开头多出来8个0，所以下面进行截取，从第9位开始获取数据
				} else if colType[i] == "BIT" {
					value = hex.EncodeToString(colValue)[1:] //mysql中获取bit类型转为16进制是00或者01,但是在pgsql中如果只需要1位类型为bit(1)，那么就从第1位后面开始截取数据
				} else {
					value = string(colValue) //非大字段类型,显式使用string函数强制转换为字符串文本，否则都是字节类型文本(即sql.RawBytes)
				}
			}
			// 检查varchar类型的数据中是否包含Unicode中的非法字符0
			//colNameStr := strings.ToLower(columns[i])
			//if colNameStr == "enable" {
			//	fmt.Println(colNameStr, colType[i], value)
			//}
			if colType[i] == "VARCHAR" || colType[i] == "TEXT" {
				// 由于在varchar类型下操作，这里直接断言成字符串类型
				newStr, _ := value.(string)
				// 将列值转换成Unicode码值，便于在码值中发现一些非法字符
				sliceRune := []rune(newStr)
				// 以下是通过遍历rune类型切片数据，找出列值中包含Unicode的非法字符0
				uniStr := 0    // 待查找的非合法的Unicode码值0
				found := false // 如果后面遍历切片找到非法值，值为true
				for _, val := range sliceRune {
					if val == rune(uniStr) {
						found = true
						break
					}
				}
				if found {
					//log.Warning("invalid is in sliceRune ", value, columns[i])
					LogError(logDir, "invalidTableData", "[Warning] invalid string found ! tableName:"+tableName+"     column value:["+newStr+"]      columnName:["+columns[i]+"]", err)
					// 直接批量替换，使用\x00去除掉列值中非法的Unicode码值0
					value = strings.Replace(string(sliceRune), "\x00", "", -1)
				}
			}
			prepareValues[i] = value //把第1列的列值追加到任意类型的切片里面，然后把第2列，第n列的值加到任意类型的切片里面,这里的切片即一行完整的数据
		}
		_, err = stmt.Exec(prepareValues...) //这里Exec只传入实参，即上面prepare的CopyIn所需的参数，这里理解为把stmt所有数据先存放到buffer里面
		if err != nil {
			log.Error("stmt.Exec(prepareValues...) failed ", tableName, " ", err) // 这里是按行来的，不建议在这里输出错误信息,建议如果遇到一行错误就直接return返回
			LogError(logDir, "errorTableData", StrVal(prepareValues), err)
			//ch <- 1
			// 通过外部的全局变量通道获取到迁移行数据失败的计数
			responseChannel <- fmt.Sprintf("data error %s", tableName)
			<-ch   // 通道向外发送数据
			return // 如果prepare异常就return
		}
	}
	err = rows.Close()
	if err != nil {
		return
	}
	_, err = stmt.Exec() //把所有的buffer进行flush，一次性写入数据
	if err != nil {
		log.Error("prepareValues Error PG Copy Failed: ", tableName, " ", err) //注意这里不能使用Fatal，否则会直接退出程序，也就没法遇到错误继续了
		// 在copy过程中异常的表，将异常信息输出到平面文件
		LogError(logDir, "errorTableData", StrVal(prepareValues), err)
		//ch <- 2
		// 通过外部的全局变量通道获取到迁移行数据失败的计数
		responseChannel <- fmt.Sprintf("data error %s", tableName)
		<-ch // 通道向外发送数据
	}
	err = stmt.Close() //关闭stmt
	if err != nil {
		log.Error(err)
	}
	err = txn.Commit() // 提交事务，这里注意Commit在上面Close之后
	if err != nil {
		err := txn.Rollback()
		if err != nil {
			return
		}
		log.Error("Commit failed ", err)
	}
	cost := time.Since(start) //计算时间差
	log.Info(fmt.Sprintf("%v Taskid[%d] table %v complete,processed %d rows,execTime %s", time.Now().Format("2006-01-02 15:04:05.000000"), startPage, tableName, totalRow, cost))
	//ch <- 0
	<-ch // 通道向外发送数据
}
