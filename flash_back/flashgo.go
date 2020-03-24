package main

import (
	"context"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"reflect"
	"runtime"
	"time"
)

var Version = "0.0.1"

var (
	user         = kingpin.Flag("user", "MySQL Username to log in as").Short('u').Default("root").String()
	host         = kingpin.Flag("host", "Host the MySQL database server located").Short('h').Default("127.0.0.1").String()
	port         = kingpin.Flag("port", "MySQL port to use").Short('P').Uint16()
	password     = kingpin.Flag("password", "MySQL Password to use").Short('p').Default("").String()
	charset      = kingpin.Flag("charset", "mysql charset").Default("utf8").String()
	startFile    = kingpin.Flag("start-file", "start binlog file name").Default("").String()
	startPos     = kingpin.Flag("start-position", "start binlog position ").Uint32()
	stopFile     = kingpin.Flag("stop-file", "end binlog file name").Default("").String()
	stopPos      = kingpin.Flag("stop-position", "end binlog position ").Uint32()
	startTime    = kingpin.Flag("start-datetime", "Start time. format %%Y-%%m-%%d %%H:%%M:%%S").Default("").String()
	stopTime     = kingpin.Flag("stop-datetime", "Stop Time. format %%Y-%%m-%%d %%H:%%M:%%S;").Default("").String()
	noPK         = kingpin.Flag("no-primary-key", "Generate insert sql without primary key if exists").Short('K').Bool()
	flashBack    = kingpin.Flag("flashback", "Flashback data to start_position of start_file").Short('B').Bool()
	stopNever    = kingpin.Flag("stop-never", "Continuously parse binlog. default: stop at the latest event when you start.").Bool()
	backInterval = kingpin.Flag("back-interval", "Sleep time between chunks of 1000 rollback sql. set it to 0 if do not need sleep").Default("").String()
	tables       = kingpin.Flag("tables", "tables you want to process").Short('t').Default("").String()
	databases    = kingpin.Flag("databases", "dbs you want to process").Short('d').Default("").String()
	onlyDML      = kingpin.Flag("only-dml", "only print dml, ignore ddl").Bool()
	sqlType      = kingpin.Flag("sql-type", "Sql type you want to process, support INSERT, UPDATE, DELETE").Default("DELETE,UPDATE,INSERT").String()
)

func processBinlog(dsn *Dsn, args *Args) {
	cfg := replication.BinlogSyncerConfig{
		ServerID: 10010,
		Flavor:   "mysql",
		Host:     dsn.host,
		Port:     dsn.port,
		User:     dsn.user,
		Password: dsn.password,
	}
	db, err := GetDBConnect(dsn.String())
	if err != nil {
		fmt.Print(err.Error())
	}
	flagLastEvent := false
	syncer := replication.NewBinlogSyncer(cfg)
	pos := mysql.Position{args.startFile, args.startPos}
	stream, _ := syncer.StartSync(pos)
	eStartPos, lastPos := args.startPos, args.startPos
	fmt.Println(eStartPos, lastPos)
	//创建文件存储数据
	fileNameHeader := fmt.Sprintf("%s.%d", dsn.host, dsn.port)
	tmpFile, err := createUniqueFile(fileNameHeader)
	fmt.Println("创建临时文件--------" + tmpFile)
	if err != nil {
		fmt.Print(err.Error())
	}
	fmt.Println("args.stopNever--", args.stopNever)
	fmt.Println(args.binlogArray)
	//对数据进行循环处理
	colsMap := make(map[string][]string)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		binlogEvent, err := stream.GetEvent(ctx)
		fmt.Println("----------------------------------------------------------------------")
		fmt.Println(binlogEvent.Header.EventType.String())
		pos.Pos = binlogEvent.Header.LogPos
		fmt.Println("pos.Pos--", pos.Pos)
		typeEvent := reflect.TypeOf(binlogEvent.Event).Elem()
		evenTime, err := time.Parse("2006-01-02 15:04:05", time.Unix(int64(binlogEvent.Header.Timestamp), 0).Format("2006-01-02 15:04:05"))
		//每个binlog头部和末尾会有RotateEvent event，我们取头部的RotateEvent来作为标记开始一个新的文件
		if evenTime.Before(args.startTime) {
			rotateEvent, ok := binlogEvent.Event.(*replication.RotateEvent)
			if ok {
				pos.Name = string(rotateEvent.NextLogName)
			}
		}

		if err != nil {
			fmt.Println(err.Error())
		}
		//确定当前的binlog file名称

		//完成任务
		if !(args.stopNever) {
			//暂时没有加异常处理
			if err != nil {
				fmt.Println(err.Error())
			}
			fmt.Println("event time is -----"+time.Unix(int64(binlogEvent.Header.Timestamp), 0).Format("2006-01-02 15:04:05"), "event binlog file is ------", pos.Name, "event binlog pos is ----- ", pos.Pos, binlogEvent.Header.LogPos)
			if (pos.Name == args.stopFile && pos.Pos == args.stopPos && pos.Pos != 0) || (pos.Name == args.eofFile && pos.Pos == args.eofPos && pos.Pos != 0) {
				fmt.Println(pos.Name, args.stopFile, pos.Pos, args.stopPos)
				fmt.Println(pos.Name, args.eofFile, pos.Pos, args.eofPos)
				fmt.Println("最后一个event是", pos.Name, pos.Pos)
				flagLastEvent = true
			} else if evenTime.Before(args.startTime) {
				fmt.Println("当前event时间小于starttime")
				if !((typeEvent.Name() == "RotateEvent") || (typeEvent.Name() == "FormatDescriptionEvent")) {
					lastPos = pos.Pos
				}
				//continue
			} else if (!IsContain(args.binlogArray, pos.Name)) || ((args.stopPos != 0) && (pos.Name == args.stopFile) && (pos.Pos > args.stopPos)) || (pos.Name == args.eofFile && pos.Pos > args.eofPos) || (evenTime.After(args.stopTime)) {
				fmt.Println("已经到达末尾，终止输出")
				break
			}
		}
		fmt.Println("已经是新的if啦")

		queryEvent, ok := binlogEvent.Event.(*replication.QueryEvent)
		if ok && string(queryEvent.Query) == "BEGIN" {
			eStartPos = lastPos
		}

		queryEvent, ok = binlogEvent.Event.(*replication.QueryEvent)
		if ok && !args.onlyDml {
			sql := "输出begin commit 等语句"
			fmt.Println("concat..." + sql)
			if sql != "" {
				fmt.Println(sql)
			}
		} else if IsDMLEvent(binlogEvent) && IsContain(args.sqlType, DMLEvenType(binlogEvent)) {
			event, ok := binlogEvent.Event.(*replication.RowsEvent)
			if !ok {
				fmt.Println("It's not ok for type RowsEvent")
				return
			}
			tableName := string(event.Table.Table)
			schemaName := string(event.Table.Schema)
			tableIdString := fmt.Sprintf("%v", event.TableID)
			tableNameKey := tableIdString + "." + schemaName + "." + tableName
			tableNamePrimaryKey := tableIdString + "." + schemaName + "." + tableName + ".primary"
			if colsMap[tableNameKey] == nil {
				fmt.Println("表ID不存在于colsMap，进入数据库查询")
				colsMap = GetColsInfo(db, tableIdString, schemaName, tableName, colsMap)
			}
			//fmt.Print("获取表字段名称为----")
			//fmt.Println(colsMap)
			//fmt.Println("rows event的库名和表名字是-----",tableName,schemaName)
			colsNames := colsMap[tableNameKey]
			colsNamesPrimary := colsMap[tableNamePrimaryKey]
			count := 0

			if binlogEvent.Header.EventType.String() == "UpdateRowsEventV2" {
				fmt.Println("打印UpdateRowsEventV2---拼接SQL")
				var updateBeforeValue []interface{}
				var updateAfterValue []interface{}
				for _, row := range event.Rows {
					if count%2 == 0 {
						updateBeforeValue = row
					} else {
						updateAfterValue = row
						sql, err := concatUpdateSqlFromBinlogEvent(args, db, binlogEvent, updateBeforeValue, updateAfterValue, args.noPK, args.flashBack, eStartPos, colsNames, colsNamesPrimary)
						//fmt.Println(sql)
						if err != nil {
							fmt.Println(err.Error())
						}
						if args.flashBack {
							fmt.Println(sql)
						} else {
							fmt.Println(sql)
						}
					}
					count++
				}

			} else {
				for _, row := range event.Rows {
					sql, err := concatSqlFromBinlogEvent(args, db, binlogEvent, row, args.noPK, args.flashBack, eStartPos, colsNames, colsNamesPrimary)
					//fmt.Println(sql)
					if err != nil {
						fmt.Println(err.Error())
					}
					if args.flashBack {
						fmt.Println(sql)
					} else {
						fmt.Println(sql)
					}
				}
			}

			fmt.Println("打印特定的DML语句")
		}

		if !(typeEvent.Name() == "RotateEvent" || typeEvent.Name() == "FormatDescriptionEvent") {
			lastPos = pos.Pos
		}

		if flagLastEvent {
			break
		}

		cancel()
		if err == context.DeadlineExceeded {
			continue
		}
		binlogEvent.Dump(os.Stdout)
	}

	if args.flashBack {
		fmt.Print("打印闪回sql语句")
	}

	fmt.Println("----------------------------------------------------------------------")

}

func main() {
	kingpin.Version(fmt.Sprintf("flashgo %s (built with %s)\n", Version, runtime.Version()))
	kingpin.Parse()
	mysqlStruct, _ := NewDsn(*user, *password, *host, *port, *charset)
	dsn_string := mysqlStruct.String()
	fmt.Println(dsn_string)
	fmt.Printf("%+v", mysqlStruct)
	fmt.Println()
	mysqlArgs, err := NewArgs(dsn_string, *startFile, *startPos, *stopFile, *stopPos, *startTime, *stopTime, *noPK, *flashBack, *stopNever, *backInterval, *onlyDML, *sqlType, *tables, *databases)

	if err != nil {
		fmt.Println(err.Error())
	}

	if mysqlArgs == nil {
		return
	}

	//fmt.Printf("打印参数列表-------%+v", mysqlArgs)
	//fmt.Println()
	//fmt.Println(mysqlArgs.startTime.Format("2006-01-02 15:04:05"))
	//fmt.Println(mysqlArgs.stopTime.Format("2006-01-02 15:04:05"))

	//执行解析binlog
	processBinlog(mysqlStruct, mysqlArgs)

}
