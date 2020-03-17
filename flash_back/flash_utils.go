package main

import (
	"errors"
	"github.com/siddontang/go-mysql/replication"
	"os"
	"strconv"
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

func concatSqlFromBinlogEvent(args Args) (sql string, error error) {
	//如果同时打开了回滚和没有主键则报错
	if args.flashBack && args.noPK {
		return "", errors.New("only one of flashback or no_pk can be True")
	}
	return "", nil
}
