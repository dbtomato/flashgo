package main

import (
	"flag"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
)

var testHost = flag.String("host", "10.16.4.125", "MySQL host")

type MyEventHandler struct {
	canal.DummyEventHandler
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	log.Infof("%s %v\n", e.Action, e.Rows)
	return nil
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

//func main() {
//	cfg := canal.NewDefaultConfig()
//	cfg.Addr = fmt.Sprintf("%s:3306", *testHost)
//	cfg.User = "percona1"
//	cfg.Password="ppercona1234"
//	cfg.HeartbeatPeriod = 200 * time.Millisecond
//	cfg.ReadTimeout = 300 * time.Millisecond
//	cfg.Dump.ExecutionPath = "mysqldump"
//	cfg.Dump.TableDB = "test1"
//	cfg.Dump.Tables = []string{"t3"}
//	//cfg.Dump.Where = "id>0"
//
//	//cfg.IncludeTableRegex = make([]string, 1)
//	//cfg.IncludeTableRegex[0] = ".*\\.canal_test"
//	//cfg.ExcludeTableRegex = make([]string, 2)
//	//cfg.ExcludeTableRegex[0] = "mysql\\..*"
//	//cfg.ExcludeTableRegex[1] = ".*\\..*_inner"
//
//	c, err := canal.NewCanal(cfg)
//	if err !=nil{
//		fmt.Println(err.Error())
//	}
//
//	c.SetEventHandler(&MyEventHandler{})
//	c.Run()
//
//}
