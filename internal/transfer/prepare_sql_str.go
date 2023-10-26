package transfer

import (
	"bytes"
	"strconv"
	"strings"
)

// 根据表是否有主键，自动生成每个表查询sql，有主键就生成分页查询组成的切片，没主键就拼成全表查询sql，最后返回sql切片
func (tf *Transfer) prepareSqlStr(tableName string, pageSize int) (sqlList []string) {
	var scanColPk string   // 每个表主键列字段名称
	var colFullPk []string // 每个表所有主键列字段生成的切片
	var totalPageNum int   // 每个表的分页查询记录总数，即总共有多少页记录
	var sqlStr string      // 分页查询或者全表扫描sql
	//先获取下主键字段名称,可能是1个，或者2个以上组成的联合主键
	sql1 := "SELECT lower(COLUMN_NAME) FROM information_schema.key_column_usage t WHERE constraint_name='PRIMARY' AND table_schema=DATABASE() AND table_name=? order by ORDINAL_POSITION;"
	rows, err := tf.srcDb.Query(sql1, tableName)
	if err != nil {
		log.Fatal(sql1, " exec failed ", err)
	}
	defer rows.Close()
	// 获取主键集合，追加到切片里面
	for rows.Next() {
		err = rows.Scan(&scanColPk)
		if err != nil {
			log.Println(err)
		}
		colFullPk = append(colFullPk, scanColPk)
	}
	// 没有主键，就返回全表扫描的sql语句,即使这个表没有数据，迁移也不影响，测试通过
	if colFullPk == nil {
		sqlList = append(sqlList, "select * from "+"`"+tableName+"`")
		return sqlList
	}
	// 遍历主键集合，使用逗号隔开,生成主键列或者组合，以及join on的连接字段
	buffer1 := bytes.NewBufferString("")
	buffer2 := bytes.NewBufferString("")
	for i, col := range colFullPk {
		if i < len(colFullPk)-1 {
			buffer1.WriteString(col + ",")
			buffer2.WriteString("temp." + col + "=t." + col + " and ")
		} else {
			buffer1.WriteString(col)
			buffer2.WriteString("temp." + col + "=t." + col)
		}
	}
	// 如果有主键,根据当前表总数以及每页的页记录大小pageSize，自动计算需要多少页记录数，即总共循环多少次，如果表没有数据，后面判断下切片长度再做处理
	sql2 := "/* gomysql2pg */" + "select ceil(count(*)/" + strconv.Itoa(pageSize) + ") as total_page_num from " + "`" + tableName + "`"
	//以下是直接使用QueryRow
	err = tf.srcDb.QueryRow(sql2).Scan(&totalPageNum)
	if err != nil {
		log.Fatal(sql2, " exec failed ", err)
		return
	}
	// 以下生成分页查询语句
	for i := 0; i <= totalPageNum; i++ { // 使用小于等于，包含没有行数据的表
		sqlStr = "SELECT t.* FROM (SELECT " + buffer1.String() + " FROM " + "`" + tableName + "`" + " ORDER BY " + buffer1.String() + " LIMIT " + strconv.Itoa(i*pageSize) + "," + strconv.Itoa(pageSize) + ") temp LEFT JOIN " + "`" + tableName + "`" + " t ON " + buffer2.String() + ";"
		sqlList = append(sqlList, strings.ToLower(sqlStr))
	}
	return sqlList
}
