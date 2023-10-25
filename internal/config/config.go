package config

type Config struct {
	Src          string // dsn
	Dest         string // dsn
	PageSize     int
	MaxParallel  int
	CharInLength bool
	Distributed  bool
	Tables       []*Table
	Exclude      []string
}

type Table struct {
	Name  string
	Tasks []*Task
}

type Task struct {
	Query      string   // sync with sql
	Conditions []string // sync with condition parts
}
