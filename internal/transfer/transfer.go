package transfer

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/arkfra/gomysql2pg/internal/config"
	"github.com/xo/dburl"
)

type Transfer struct {
	cfg        *config.Transfer
	selFromYml bool

	srcUri  *dburl.URL
	srcDb   *sql.DB
	destUri *dburl.URL
	destDb  *sql.DB

	// 表行数据迁移失败的计数
	errDataCount int
}

func NewTransfer(cfg *config.Transfer) (tf *Transfer, err error) {
	tf = &Transfer{
		cfg: cfg,
	}

	if uri, err2 := cfg.SrcUri(); err2 == nil {
		tf.srcUri = uri
	} else {
		err = errors.Join(err, err2)
		return
	}

	if uri, err2 := cfg.DestUri(); err2 == nil {
		tf.destUri = uri
	} else {
		err = errors.Join(err, err2)
		return
	}

	if err2 := tf.PrepareSrc(); err2 != nil {
		err = errors.Join(err, err2)
		return
	}

	if err2 := tf.PrepareDest(); err2 != nil {
		err = errors.Join(err, err2)
		return
	}

	return
}

func (tf *Transfer) SetSelFromYml(v bool) {
	tf.selFromYml = v
}

func (tf Transfer) Cfg() *config.Transfer {
	return tf.cfg
}

func (tf Transfer) SrcUri() *dburl.URL {
	return tf.srcUri
}

func (tf Transfer) DestUri() *dburl.URL {
	return tf.destUri
}

func (tf Transfer) TableMap() []*config.Table {
	// 获取源库的所有表
	if tf.selFromYml { // 如果用了-s选项，从配置文件中获取表名以及sql语句
		return tf.cfg.Tables
	} else { // 不指定-s选项，查询源库所有表名
		return tf.FetchTableMap()
	}
}

// 生成源库连接
func (tf *Transfer) PrepareSrc() (err error) {
	if tf.srcDb, err = sql.Open("mysql", tf.srcUri.DSN); err != nil {
		log.Fatal("please check MySQL yml file", err)
	}
	if err = tf.srcDb.Ping(); err != nil {
		log.Fatal("connect MySQL failed", err)
	}
	tf.srcDb.SetConnMaxLifetime(2 * time.Hour) // 一个连接被使用的最长时间，过一段时间之后会被强制回收
	tf.srcDb.SetMaxIdleConns(0)                // 最大空闲连接数，0为不限制
	tf.srcDb.SetMaxOpenConns(30)               // 设置连接池最大连接数
	log.Info("connect MySQL ", tf.srcUri.DSN, " success")

	return err
}

// 生成目标库连接
func (tf *Transfer) PrepareDest() (err error) {
	if tf.destDb, err = sql.Open("postgres", tf.destUri.DSN); err != nil {
		log.Fatal("please check Postgres yml file", err)
	}

	if err = tf.destDb.Ping(); err != nil {
		log.Fatal("connect Postgres failed ", err)
	}
	log.Info("connect Postgres ", tf.destUri.DSN, " success")

	return err
}

func LogError(logDir string, logName string, strContent string, errInfo error) {
	f, errFile := os.OpenFile(logDir+"/"+logName+".log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if errFile != nil {
		log.Fatal(errFile)
	}
	defer func() {
		if errFile := f.Close(); errFile != nil {
			log.Fatal(errFile) // 或设置到函数返回值中
		}
	}()
	// create new buffer
	buffer := bufio.NewWriter(f)
	_, errFile = buffer.WriteString(strContent + " -- ErrorInfo " + StrVal(errInfo) + "\n")
	if errFile != nil {
		log.Fatal(errFile)
	}
	// flush buffered data to the file
	if errFile := buffer.Flush(); errFile != nil {
		log.Fatal(errFile)
	}
}

func (tf *Transfer) cleanDBconn() {
	// 遍历正在执行gomysql2pg的客户端，使用kill query 命令kill所有查询id
	rows, err := tf.srcDb.Query("select id from information_schema.PROCESSLIST where info like '/* gomysql2pg%';")
	if err != nil {
		log.Error(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		err = rows.Scan(&id)
		if err != nil {
			log.Error("rows.Scan(&id) failed!", err)
		}
		tf.srcDb.Exec("kill query " + id)
		log.Info("kill thread id ", id)
	}
}

// 监控来自终端的信号，如果按下了ctrl+c，断开数据库查询以及退出程序
func (tf *Transfer) exitHandle(exitChan chan os.Signal) {
	sig := <-exitChan
	fmt.Println("receive system signal:", sig)
	tf.cleanDBconn() // 调用清理数据库连接的方法
	os.Exit(1)       // 如果ctrl+c 关不掉程序，使用os.Exit强行关掉
}

// StrVal
// 获取变量的字符串值，目前用于interface类型转成字符串类型
// 浮点型 3.0将会转换成字符串3, "3"
// 非数值或字符类型的变量将会被转换成JSON格式字符串
func StrVal(value interface{}) string {
	var key string
	if value == nil {
		return key
	}

	switch it := value.(type) {
	case float64:
		key = strconv.FormatFloat(it, 'f', -1, 64)
	case float32:
		key = strconv.FormatFloat(float64(it), 'f', -1, 64)
	case int:
		key = strconv.Itoa(it)
	case uint:
		key = strconv.Itoa(int(it))
	case int8:
		key = strconv.Itoa(int(it))
	case uint8:
		key = strconv.Itoa(int(it))
	case int16:
		key = strconv.Itoa(int(it))
	case uint16:
		key = strconv.Itoa(int(it))
	case int32:
		key = strconv.Itoa(int(it))
	case uint32:
		key = strconv.Itoa(int(it))
	case int64:
		key = strconv.FormatInt(it, 10)
	case uint64:
		key = strconv.FormatUint(it, 10)
	case string:
		key = value.(string)
	case []byte:
		key = string(value.([]byte))
	default:
		newValue, _ := json.Marshal(value)
		key = string(newValue)
	}

	return key
}
