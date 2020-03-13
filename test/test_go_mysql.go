package main

import (
	"context"
	"fmt"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"os"
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

	// Start sync with specified binlog file and position
	streamer, _ := syncer.StartSync(mysql.Position{binlogFile, binlogPos})

	// or you can start a gtid replication like
	// streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	// the mariadb GTID set likes this "0-1-100"

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ev, err := streamer.GetEvent(ctx)

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
