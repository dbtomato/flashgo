package main

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/alecthomas/kingpin.v2"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var Version = "0.0.1"

var (
	user     = kingpin.Flag("user", "MySQL Username to log in as").Short('u').Default("root").String()
	host     = kingpin.Flag("host", "Host the MySQL database server located").Short('h').Default("127.0.0.1").String()
	port     = kingpin.Flag("port", "MySQL port to use").Short('P').Default("3306").String()
	password = kingpin.Flag("password", "MySQL Password to use").Short('p').Default("").String()
	charset  = kingpin.Flag("charset", "mysql charset").Default("utf8").String()

	startFile    = kingpin.Flag("start-file", "start binlog file name").Default("").String()
	startPos     = kingpin.Flag("start-pos", "start binlog position ").Uint32()
	stopFile     = kingpin.Flag("stop-file", "end binlog file name").Default("").String()
	stopPos      = kingpin.Flag("stop-pos", "end binlog position ").Uint32()
	startTime    = kingpin.Flag("start-datetime", "Start time. format %%Y-%%m-%%d %%H:%%M:%%S").Default("").String()
	stopTime     = kingpin.Flag("stop-datetime", "Stop Time. format %%Y-%%m-%%d %%H:%%M:%%S;").Default("").String()
	noPK         = kingpin.Flag("no-primary-key", "Generate insert sql without primary key if exists").Short('K').Default("").String()
	flashBack    = kingpin.Flag("flashback", "Flashback data to start_position of start_file").Short('B').Default("").String()
	stopNever    = kingpin.Flag("stop-never", "Continuously parse binlog. default: stop at the latest event when you start.").Default("").String()
	backInterval = kingpin.Flag("back-interval", "Sleep time between chunks of 1000 rollback sql. set it to 0 if do not need sleep").Default("").String()
	tables       = kingpin.Flag("tables", "tables you want to process").Short('t').Default("").String()
	databases    = kingpin.Flag("databases", "dbs you want to process").Short('d').Default("").String()
	onlyDML      = kingpin.Flag("only-dml", "only print dml, ignore ddl").Default("false").String()
	sqlType      = kingpin.Flag("sql-type", "Sql type you want to process, support INSERT, UPDATE, DELETE").Default("").String()
)

//'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'
type Dsn struct {
	user     string
	password string
	host     string
	port     string
	charset  string
}

func (d Dsn) String() string {
	//return "user:" + d.user + "\npassword:" + d.password + "\nhost:" + d.host + "\nport:" + d.port + "\ncharset:" + d.charset
	dsn := fmt.Sprintf("%s:%s@%s(%s:%s)/%s", d.user, d.password, "tcp", d.host, d.port, "mysql")
	return dsn
}

type Args struct {
	startFile    string
	startPos     uint32
	stopFile     string
	stopPos      uint32
	startTime    time.Time
	stopTime     time.Time
	noPK         string
	flashBack    string
	stop_never   string
	backInterval string
	onlyDml      string
	sqlType      string
	tables       string
	databases    string
}

type master_status struct {
	masterFile        string
	masterPos         uint32
	Binlog_Do_DB      string
	Binlog_Ignore_DB  string
	Executed_Gtid_Set string
}

var binlog_array []string

func NewDsn(user string, password string, host string, port string, charset string) (*Dsn, error) {
	dsn := new(Dsn)
	dsn.user = user
	dsn.host = host
	dsn.port = port
	dsn.password = password
	dsn.charset = charset
	return dsn, nil

}

func NewArgs(dsn string, startFile string, startPos uint32, stopFile string, stopPos uint32, startTime string, stopTime string, noPK string, flashBack string, stopNever string, backInterval string, onlyDml string, sqlType string, tables string, databases string) (*Args, error) {
	args := new(Args)
	if startFile == "" {
		return nil, errors.New("Lack of parameter: start_file")
	}
	if flashBack != "" && stopNever != "" {
		return nil, errors.New("Only one of flashback or stop-never can be True")
	}
	if noPK != "" && stopNever != "" {
		return nil, errors.New("Only one of flashback or no_pk can be True")
	}
	if (startTime != "" && (!is_valid_datetime(startTime))) || (stopTime != "" && (!is_valid_datetime(stopTime))) {
		return nil, errors.New("Incorrect datetime argument")
	}
	if startPos == 0 {
		startPos = 4
	}

	if stopFile == "" {
		stopFile = startFile
	}

	if startTime != "" {
		stime, err := time.Parse("2006-01-02 15:04:05", startTime)
		if err != nil {
			fmt.Println("start_time时间格式化报错")
		}
		args.startTime = stime
	} else {
		stime, err := time.Parse("2006-01-02 15:04:05", "1980-01-01 00:00:00")
		if err != nil {
			return nil, errors.New("start_time时间格式化报错")
		}
		args.startTime = stime

	}

	if stopTime != "" {
		stime, err := time.Parse("2006-01-02 15:04:05", stopTime)
		if err != nil {
			fmt.Println("start_time时间格式化报错")
		}
		args.stopTime = stime
	} else {
		stime, err := time.Parse("2006-01-02 15:04:05", "2999-12-31 00:00:00")
		if err != nil {
			fmt.Println("start_time时间格式化报错")
		}
		args.stopTime = stime

	}

	//登陆数据库
	fmt.Println("---------连接数据库")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.New("数据库连接失败")
	}
	defer db.Close()
	//row := db.QueryRow("SHOW MASTER STATUS")
	row := db.QueryRow("show master status")

	ms := new(master_status)

	row.Scan(&ms.masterFile, &ms.masterPos, &ms.Binlog_Do_DB, &ms.Binlog_Ignore_DB, &ms.Executed_Gtid_Set)
	fmt.Println(ms.masterFile, ms.masterPos)

	//查询当前用户持有的binlog
	rows, err := db.Query("show master logs")
	defer rows.Close()
	if err != nil {
		return nil, errors.New("数据库连接失败")
	}

	columns, _ := rows.Columns()
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		//把sql.RawBytes类型的地址存进去了
		scanArgs[i] = &values[i]
	}
	var result []map[string]string
	for rows.Next() {
		res := make(map[string]string)
		rows.Scan(scanArgs...)
		for i, col := range values {
			res[columns[i]] = string(col)
		}
		result = append(result, res)
	}
	//判断用户输入binlog是否在show master logs里面
	flag := false
	for _, r := range result {
		//将所有的binlog名称追加到binlog_array数组中
		if r["Log_name"] == startFile {
			flag = true
		}

		file_num, err := strconv.Atoi(strings.Split(r["Log_name"], ".")[1])
		if err != nil {
			fmt.Println(err.Error())
		}
		start_file_num, err := strconv.Atoi(strings.Split(startFile, ".")[1])
		if err != nil {
			fmt.Println(err.Error())
		}
		stop_file_num, err := strconv.Atoi(strings.Split(stopFile, ".")[1])
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println(start_file_num, file_num, stop_file_num)
		if start_file_num <= file_num && file_num <= stop_file_num {
			binlog_array = append(binlog_array, r["Log_name"])
		}

	}

	if flag == false {
		return nil, errors.New("parameter error: start_file " + startFile + " not in mysql server")
	}

	fmt.Println(binlog_array)

	args.startFile = startFile
	args.startPos = startPos
	args.stopFile = stopFile
	args.stopPos = stopPos
	args.noPK = noPK
	args.flashBack = flashBack
	args.stop_never = stopNever
	args.backInterval = backInterval
	args.onlyDml = onlyDml
	args.sqlType = sqlType
	args.tables = tables
	args.databases = databases

	return args, nil

}

func main() {

	kingpin.Version(fmt.Sprintf("flashgo %s (built with %s)\n", Version, runtime.Version()))
	kingpin.Parse()
	mysql_struct, _ := NewDsn(*user, *password, *host, *port, *charset)
	dsn_string := mysql_struct.String()
	fmt.Println(dsn_string)
	fmt.Printf("%+v", mysql_struct)
	fmt.Println()
	mysql_args, err := NewArgs(dsn_string, *startFile, *startPos, *stopFile, *stopPos, *startTime, *stopTime, *noPK, *flashBack, *stopNever, *backInterval, *onlyDML, *sqlType, *tables, *databases)

	if err != nil {
		fmt.Println(err.Error())
	}

	if mysql_args == nil {
		return
	}

	fmt.Printf("%+v", mysql_args)
	fmt.Println()
	fmt.Println(mysql_args.startTime.Format("2006-01-02 15:04:05"))
	fmt.Println(mysql_args.stopTime.Format("2006-01-02 15:04:05"))

}
