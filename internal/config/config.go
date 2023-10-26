package config

import (
	"github.com/xo/dburl"
)

type Table struct {
	Name  string
	Tasks []*Task
}

func (t Table) Parts() []string {
	parts := make([]string, 0, len(t.Tasks))
	for _, task := range t.Tasks {
		// TODO: generate query with Conditions
		if len(task.Query) > 0 {
			parts = append(parts, task.Query)
		}
	}
	return parts
}

type Task struct {
	Query      string   // sync with sql
	Conditions []string // sync with condition parts
}

type Transfer struct {
	Src          string // dsn
	Dest         string // dsn
	PageSize     int    // // 每页的分页记录数,仅全库迁移时有效
	MaxParallel  int
	CharInLength bool
	Distributed  bool
	Tables       []*Table
	Exclude      []string // 从配置文件中获取需要排除的表
}

func (tf *Transfer) SrcUri() (*dburl.URL, error) {
	return dburl.Parse(tf.Src)
}

func (tf *Transfer) DestUri() (*dburl.URL, error) {
	return dburl.Parse(tf.Dest)
}
