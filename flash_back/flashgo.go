package flash_back

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	user     = kingpin.Flag("user", "start binlog file name").Default("root").String()
	host     = kingpin.Flag("host", "start binlog file name").Default("127.0.0.1").String()
	port     = kingpin.Flag("port", "start binlog file name").Default("3306").String()
	password = kingpin.Flag("password", "start binlog file name").Default("").String()
	charset  = kingpin.Flag("charset", "start binlog file name").Default("utf8").String()

	startFile    = kingpin.Flag("start_file", "start binlog file name").Default("").String()
	startPos     = kingpin.Flag("start_pos", "start binlog position ").Default("0").String()
	endFile      = kingpin.Flag("end_file", "end binlog file name").Default("0").String()
	endPos       = kingpin.Flag("end_pos", "end binlog position ").Default("0").String()
	startTime    = kingpin.Flag("start_time", "start datetime ").Default("0").String()
	endTime      = kingpin.Flag("start_time", "end datetime ").Default("0").String()
	onlySchemas  = kingpin.Flag("databases", "choose the database ").Default("0").String()
	onlyTables   = kingpin.Flag("tables", "choose the database ").Default("0").String()
	no_pk        = kingpin.Flag("no_pk", "have no primriry key ").Default("0").String()
	flashBack    = kingpin.Flag("flashback", "flash back the binlog to sql ").Default("0").String()
	stop_never   = kingpin.Flag("stop_never", "stop_never ").Default("0").String()
	backInterval = kingpin.Flag("back_interval", "stop_never ").Default("0").String()
	onlyDml      = kingpin.Flag("only_dml", "only dml operation analysis ").Default("0").String()
	sqlType      = kingpin.Flag("sql_type", "the sql type analysis ").Default("0").String()
)

//'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'
type Dsn struct {
	Host     string
	Port     string
	User     string
	Password string
	Charset  string
}

type Args struct {
	StartFile    string
	StartPos     uint32
	EndFile      string
	EndPos       uint32
	StartTime    string
	EndTime      string
	OnlySchemas  string
	OnlyTables   string
	NoPK         string
	FlashBack    string
	Stop_never   string
	BackInterval string
	OnlyDml      string
	SqlType      string
}

func NewDsn(host string, port string, user string, password string, charset string) *Dsn {
	dsn := new(Dsn)
	dsn.User = user
	dsn.Host = host
	dsn.Port = port
	dsn.Password = password
	dsn.Charset = charset
	return dsn

}

func NewArgs(StartFile string, StartPos uint32, EndFile string, EndPos uint32, StartTime string, EndTime string, OnlySchemas string, OnlyTables string, NoPK string, FlashBack string, StopNever string, BackInterval string, OnlyDml string, SqlType string) *Args {
	args := new(Args)
	if StartFile == "" {
		panic("Lack of parameter: start_file")
		return nil
	}
	if FlashBack != "" && StopNever != "" {
		panic("Only one of flashback or stop-never can be True")
		return nil
	}
	if NoPK != "" && StopNever != "" {
		panic("Only one of flashback or no_pk can be True")
		return nil
	}
	if (StartTime != "" && (!is_valid_datetime(StartTime))) || (EndTime != "" && (!is_valid_datetime(EndTime))) {
		panic("Incorrect datetime argument")
		return nil
	}

	args.StartFile = StartFile
	args.StartPos = StartPos
	args.EndFile = EndFile
	args.EndPos = EndPos
	args.StartTime = StartTime
	args.EndTime = EndTime
	args.OnlySchemas = OnlySchemas
	args.OnlyTables = OnlyTables
	args.NoPK = NoPK
	args.FlashBack = FlashBack
	args.Stop_never = StopNever
	args.BackInterval = BackInterval
	args.OnlyDml = OnlyDml
	args.SqlType = SqlType

	return args

}

func main() {

}
