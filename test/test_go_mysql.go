package main

import (
	"context"
	"fmt"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"os"
	"reflect"
	"time"
)

func main() {

	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     "10.16.4.125",
		Port:     3306,
		User:     "percona1",
		Password: "ppercona1234",
	}
	syncer := replication.NewBinlogSyncer(cfg)
	binlogFile := "mysql-bin.000011"
	var binlogPos uint32 = 609657826
	//binlogFile := "mysql-bin.000009"
	//var binlogPos uint32 =4

	// Start sync with specified binlog file and position
	pos := mysql.Position{binlogFile, binlogPos}
	streamer, _ := syncer.StartSync(pos)

	// or you can start a gtid replication like
	// streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	// the mariadb GTID set likes this "0-1-100"

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ev, err := streamer.GetEvent(ctx)
		typeOfA := reflect.TypeOf(ev.Event).Elem()
		fmt.Println("打印当前日志名称和位置====", pos.Name, pos.Pos)

		eventType := typeOfA.Name()
		fmt.Println("反射得到的结果:-------------", eventType, "---", typeOfA.Kind())
		pos.Pos = ev.Header.LogPos
		//var event replication.EventType
		fmt.Println()
		// .Name()可以获取去这个类型的名称
		//fmt.Println("这个类型的名称是:-------------",ev.Header.EventType,ev.Header.LogPos,ev.Header.EventSize,ev.Header.Flags,ev.Header.Timestamp)

		//类型断言
		value, ok := ev.Event.(*replication.RowsEvent)
		if !ok {
			fmt.Println("It's not ok for type string")
			return
		}
		fmt.Println("The value is ", value)

		switch event := ev.Event.(type) {
		case *replication.RowsEvent:
			for _, rows := range event.Rows {
				fmt.Printf("--\n")
				for j, d := range rows {
					if _, ok := d.([]byte); ok {
						fmt.Printf("-------------%d:%q\n", j, d)
					} else {
						fmt.Printf("-------------%d:%#v\n", j, d)
					}
				}
			}
		case *replication.RotateEvent:
			pos.Name = string(event.NextLogName)
		}

		cancel()

		if err == context.DeadlineExceeded {
			// meet timeout
			continue
		}
		// Dump event
		ev.Dump(os.Stdout)

	}

	//	// or we can use a timeout context
	//	for {
	//		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	//		ev, err := s.GetEvent(ctx)
	//		cancel()
	//
	//		if err == context.DeadlineExceeded {
	//			// meet timeout
	//			continue
	//		}
	//
	//		ev.Dump(os.Stdout)
	//

	stime, err := time.Parse("2006-01-02 15:04:05", "1980-01-01 00:00:00")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(stime)

}
