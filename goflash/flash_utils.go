package main

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/replication"
	"os"
	"strconv"
	"strings"
	"time"
)

func isValidDatetime(dateStr string) bool {
	_, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		return false
	}
	return true
}

//判断文件是否存在
func fileExist(path string) bool {
	//使用Lstat比使用open效率更高
	_, err := os.Lstat(path)
	return !os.IsNotExist(err)
}

func createUniqueFile(filename string) (resultFileRes string, err error) {
	resultFile := filename
	//if we have to try more than 1000 times, something is seriously wrong
	for version := 0; fileExist(resultFile) && version < 1000; version++ {
		resultFile = filename + "." + strconv.Itoa(version)
		if version >= 1000 {
			return "", errors.New("file version >1000,create file failed")
		}
	}
	return resultFile, nil

}

func IsContain(items []string, item string) bool {
	for _, eachItem := range items {
		if eachItem == item {
			return true
		}
	}
	return false
}

func IsDMLEvent(binlogEvent *replication.BinlogEvent) bool {
	if binlogEvent.Header.EventType.String() == "WriteRowsEventV2" || binlogEvent.Header.EventType.String() == "DeleteRowsEventV2" || binlogEvent.Header.EventType.String() == "UpdateRowsEventV2" {
		return true
	} else {
		return false
	}

}

func DMLEvenType(e *replication.BinlogEvent) string {
	if e.Header.EventType.String() == "DeleteRowsEventV2" {
		return "DELETE"
	} else if e.Header.EventType.String() == "WriteRowsEventV2" {
		return "INSERT"
	} else if e.Header.EventType.String() == "UpdateRowsEventV2" {
		return "UPDATE"
	}
	return ""
}

func GetDBConnect(dsn string) (db1 *sql.DB, error error) {
	db, err := sql.Open("mysql", dsn)
	return db, err
}

func concatSqlFromBinlogEvent(args *Args, db *sql.DB, e *replication.BinlogEvent, row []interface{}, noPk bool, flashBack bool, eStartPos uint32, colsNames []string, colsNamesPrimary []string) (sql string, error error) {
	//sql = concat_sql_from_binlog_event(cursor=cursor, binlog_event=binlog_event, no_pk=self.no_pk, row=row, flashback=self.flashback, e_start_pos=e_start_pos)
	sql_concat := ""
	//如果同时打开了回滚和没有主键则报错
	if args.flashBack && args.noPK {
		return sql_concat, errors.New("only one of flashback or no_pk can be True")
	}

	if !(e.Header.EventType.String() == "DeleteRowsEventV2" || e.Header.EventType.String() == "WriteRowsEventV2" || e.Header.EventType.String() == "UpdateRowsEventV2" || e.Header.EventType.String() == "QueryEvent") {
		return sql_concat, errors.New("binlog_event must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent")
		return sql_concat, nil
	}

	if e.Header.EventType.String() == "DeleteRowsEventV2" || e.Header.EventType.String() == "WriteRowsEventV2" || e.Header.EventType.String() == "UpdateRowsEventV2" {
		//fmt.Println(row)
		sql_concat = generateSqlPattern(e, row, flashBack, *noPK, colsNames, colsNamesPrimary)
		sql_time, _ := time.Parse("2006-01-02 15:04:05", time.Unix(int64(e.Header.Timestamp), 0).Format("2006-01-02 15:04:05"))
		sql_concat = sql_concat + fmt.Sprintf(" # start %d end %d time %s", eStartPos, e.Header.LogPos, sql_time)
		//fmt.Println("generateSqlPattern返回值"+sql_concat)
		return sql_concat, nil

	} else if args.flashBack == false && e.Header.EventType.String() == "QueryEvent" {
		//fmt.Println("---------------------------这是一个QueryEvent")
		queryEvent, ok := e.Event.(*replication.QueryEvent)
		if ok && string(queryEvent.Query) != "BEGIN" && string(queryEvent.Query) != "COMMIT" {
			if queryEvent.Schema != nil || string(queryEvent.Schema) != "" {
				sql_concat = fmt.Sprintf("USE %s;\n", queryEvent.Schema)
			}
			sql_concat := sql_concat + fmt.Sprintf("%s;", string(queryEvent.Query))
			return sql_concat, nil
		}
	}
	return "", nil
}

func concatUpdateSqlFromBinlogEvent(args *Args, db *sql.DB, e *replication.BinlogEvent, beforeValue []interface{}, afterValue []interface{}, noPk bool, flashBack bool, eStartPos uint32, colsNames []string, colsNamesPrimary []string) (sql string, error error) {
	sql_concat := ""
	if args.flashBack && args.noPK {
		return sql_concat, errors.New("only one of flashback or no_pk can be True")
	}

	if !(e.Header.EventType.String() == "UpdateRowsEventV2") {
		return sql_concat, errors.New("binlog_event must be UpdateRowsEvent")
	}
	if e.Header.EventType.String() == "UpdateRowsEventV2" {
		sql_concat = generateUpdateSqlPattern(e, beforeValue, afterValue, flashBack, *noPK, colsNames, colsNamesPrimary)
		//fmt.Println("generateUpdateSqlPattern返回值"+sql_concat)
		return sql_concat, nil
	}
	return "", nil

}

func generateSqlPattern(e *replication.BinlogEvent, row []interface{}, flashBack bool, noPK bool, colsNames []string, colsNamesPrimary []string) string {
	event, ok := e.Event.(*replication.RowsEvent)
	if !ok {
		fmt.Println("It's not ok for type RowsEvent")
		return ""
	}
	if flashBack == true {
		if e.Header.EventType.String() == "WriteRowsEventV2" {
			tmpColsNames := compareDelItems(colsNames, row)
			tmpColWheres := strings.Join(tmpColsNames, " AND ")
			sql := fmt.Sprintf("DELETE FROM %s.%s WHERE %s limit 1;", event.Table.Schema, event.Table.Table, tmpColWheres)
			//fmt.Println(sql)
			return sql
		} else if e.Header.EventType.String() == "DeleteRowsEventV2" {
			var colsNamesDian []string
			for _, col := range colsNames {
				colsNamesDian = append(colsNamesDian, "`"+col+"`")
			}
			tmpColNames := strings.Join(colsNamesDian, ",")
			tmpColsValues := compareInsertItems(colsNames, row)
			tmpColValuesInsert := strings.Join(tmpColsValues, ",")
			sql := fmt.Sprintf("INSERT INTO `%s`.`%s`(%s) VALUES (%s);", event.Table.Schema, event.Table.Table, tmpColNames, tmpColValuesInsert)
			//fmt.Println(sql)
			return sql
		}
	} else {
		if e.Header.EventType.String() == "WriteRowsEventV2" {
			var colsNamesDian []string
			for _, col := range colsNames {
				colsNamesDian = append(colsNamesDian, "`"+col+"`")
			}
			//if noPK{
			//	if len(colsNamesPrimary)>0 || colsNamesPrimary !=nil{
			//		var noPkcolsNamesArray []string
			//		for _,col:= range(colsNames){
			//			for _,colPrimary:=range(colsNamesPrimary){
			//				if colPrimary!=col{
			//					noPkcolsNamesArray=append(noPkcolsNamesArray,colPrimary)
			//				}
			//
			//			}
			//		}
			//		colsNames=noPkcolsNamesArray
			//	}
			//}

			tmpColNames := strings.Join(colsNamesDian, ",")
			tmpColsValues := compareInsertItems(colsNames, row)
			tmpColValuesInsert := strings.Join(tmpColsValues, ",")
			//fmt.Println(tmpColsValues)
			sql := fmt.Sprintf("INSERT INTO `%s`.`%s`(%s) VALUES (%s);", event.Table.Schema, event.Table.Table, tmpColNames, tmpColValuesInsert)
			//fmt.Println(sql)
			return sql
		} else if e.Header.EventType.String() == "DeleteRowsEventV2" {
			tmpColsNames := compareDelItems(colsNames, row)
			tmpColWheres := strings.Join(tmpColsNames, " AND ")
			sql := fmt.Sprintf("delete from %s.%s where %s limit 1;", event.Table.Schema, event.Table.Table, tmpColWheres)
			//fmt.Println(sql)
			return sql
		}
	}
	return ""
}

func generateUpdateSqlPattern(e *replication.BinlogEvent, beforeValue []interface{}, afterValue []interface{}, flashBack bool, noPK bool, colsNames []string, colsNamesPrimary []string) string {
	event, ok := e.Event.(*replication.RowsEvent)
	if !ok {
		fmt.Println("It's not ok for type RowsEvent")
		return ""
	}
	if flashBack == true {
		if e.Header.EventType.String() == "UpdateRowsEventV2" {
			tmpBeforeValues := compareSetUpdateItems(colsNames, beforeValue)
			tmpAfterValues := compareWhereUpdateItems(colsNames, afterValue)
			tmpSet := strings.Join(tmpBeforeValues, " , ")
			tmpWhere := strings.Join(tmpAfterValues, " AND ")
			sql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;", event.Table.Schema, event.Table.Table, tmpSet, tmpWhere)
			//fmt.Println(sql)
			return sql
		}
	} else {
		if e.Header.EventType.String() == "UpdateRowsEventV2" {
			tmpBeforeValues := compareWhereUpdateItems(colsNames, beforeValue)
			tmpAfterValues := compareSetUpdateItems(colsNames, afterValue)
			tmpWhere := strings.Join(tmpBeforeValues, " , ")
			tmpSet := strings.Join(tmpAfterValues, " AND ")
			sql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;", event.Table.Schema, event.Table.Table, tmpSet, tmpWhere)
			//fmt.Println(sql)
			return sql
		}
	}
	return ""
}

func GetColsInfo(db *sql.DB, tableId string, tableSchema string, tableName string, colsMap map[string][]string) (colsMaps map[string][]string) {
	col_sql := fmt.Sprintf("SELECT COLUMN_NAME,COLUMN_KEY FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s';", tableSchema, tableName)
	//fmt.Println(col_sql)
	rows, err := db.Query(col_sql)
	if err != nil {
		fmt.Print(err.Error())
	}
	var colsArray []string
	var colsPrimary []string
	for rows.Next() {
		var colName string
		var colPrimary string
		rows.Scan(&colName, &colPrimary)
		colsArray = append(colsArray, colName)
		if colPrimary == "PRI" {
			colsPrimary = append(colsPrimary, colName)
		}
	}
	colsMap = make(map[string][]string)
	m_key := tableId + "." + tableSchema + "." + tableName
	m_key_primary := tableId + "." + tableSchema + "." + tableName + ".primary"
	colsMap[m_key] = colsArray
	colsMap[m_key_primary] = colsPrimary
	//fmt.Println(colsMap)
	return colsMap

}

func compareDelItems(colsNames []string, row []interface{}) []string {
	var colsWheres []string
	for num, col := range colsNames {
		if row[num] == nil {
			row[num] = "NULL"
		}
		//colsWhere:=fmt.Sprintf("%s=%v",col,row[num])
		if _, ok := row[num].([]byte); ok {
			colsWhere := fmt.Sprintf("`%s` = %q", col, row[num])
			colsWheres = append(colsWheres, colsWhere)
		} else {
			if row[num] == "NULL" {
				colsWhere := fmt.Sprintf("`%s` IS NULL ", col)
				colsWheres = append(colsWheres, colsWhere)
			} else {
				colsWhere := fmt.Sprintf("`%s` = %#v", col, row[num])
				colsWheres = append(colsWheres, colsWhere)
			}
		}
		//fmt.Println(colsWheres)
	}
	return colsWheres
}

func compareInsertItems(colsNames []string, row []interface{}) []string {
	var colsWheres []string

	for num, _ := range colsNames {
		if row[num] == nil {
			row[num] = "NULL"
		}
		//colsWhere:=fmt.Sprintf("%s=%v",col,row[num])
		if _, ok := row[num].([]byte); ok {
			colsWhere := fmt.Sprintf("%q", row[num])
			colsWheres = append(colsWheres, colsWhere)
		} else {
			if row[num] == "NULL" {
				colsWhere := fmt.Sprintf("%v", row[num])
				colsWheres = append(colsWheres, colsWhere)
			} else {
				colsWhere := fmt.Sprintf("%#v", row[num])
				colsWheres = append(colsWheres, colsWhere)
			}

		}
		//fmt.Println(colsWheres)
	}
	return colsWheres
}

func compareSetUpdateItems(colsNames []string, row []interface{}) []string {
	var colsWheres []string
	for num, col := range colsNames {
		if row[num] == nil {
			row[num] = "NULL"
		}
		if _, ok := row[num].([]byte); ok {
			colsWhere := fmt.Sprintf("`%s` = %q", col, row[num])
			colsWheres = append(colsWheres, colsWhere)
		} else {
			if row[num] == "NULL" {
				colsWhere := fmt.Sprintf("`%s` = %v", col, row[num])
				colsWheres = append(colsWheres, colsWhere)
			} else {
				colsWhere := fmt.Sprintf("`%s` =%#v", col, row[num])
				colsWheres = append(colsWheres, colsWhere)
			}
		}
		//fmt.Println(colsWheres)
	}
	return colsWheres
}

func compareWhereUpdateItems(colsNames []string, row []interface{}) []string {
	var colsWheres []string
	for num, col := range colsNames {
		if row[num] == nil {
			row[num] = "NULL"
		}
		if _, ok := row[num].([]byte); ok {
			colsWhere := fmt.Sprintf("`%s` =%q", col, row[num])
			colsWheres = append(colsWheres, colsWhere)
		} else {
			if row[num] == "NULL" {
				colsWhere := fmt.Sprintf("`%s` IS NULL ", col)
				colsWheres = append(colsWheres, colsWhere)
			} else {
				colsWhere := fmt.Sprintf("`%s` =%#v", col, row[num])
				colsWheres = append(colsWheres, colsWhere)
			}
		}
		//fmt.Println(colsWheres)
	}
	return colsWheres
}
