#   goflash

## 简介
goflash是基于golang开发的MySQL binlog解析工具

## 工具功能
1. 正向解析binlog为SQL
2. 闪回binlog为SQL，对数据库操作进行回滚
3. 支持对binlog的库表进行过滤解析
4. 支持针对binlog file position进行过滤解
5. 支持对DML类型进行过滤

## 相对于binlog2sql的优势
1. 支持MySQL8.0
2. 速度更快3-5倍
3. 支持跨机器解析binlog为SQL
4. 对JSON格式的解析优化

## 如何使用
在goflash的bin目录下，已经存放了针对linux x86-64的编译版本，如果需要其他版本，亦可以搭建go的编译环境进行编译，所有依赖包放在govendor的目录下


## 使用命令
正向解析binlog为SQL
`./bin/goflash --user=root --password=pwd123 --port=3306 --host='10.16.26.87' --databases='test1' --tables='table_name' --sql-type="DELETE" --start-file="binlog.000826" --start-datetime="2020-04-01 13:00:00" --stop-datetime="2020-04-01 14:20:00"    > bin.log`

解析binlog为闪回SQL
`./bin/goflash --user=root --password=pwd123 --port=3306 --host='10.16.26.87' --databases='test1' --tables='table_name' --sql-type="DELETE" --start-file="binlog.000826" --start-datetime="2020-04-01 13:00:00" --stop-datetime="2020-04-01 14:20:00"  -B  > bin.log`

## 导入数据
goflash正向解析完成后的文件存放在当前目录下，以IP+PORT+ID命令文件
比如
10.16.26.87.3306
10.16.26.87.3306.0
10.16.26.87.3306.1

goflash进行闪回后的文件会带.bak请使用.bak文件进行闪回工作
10.16.26.87.3306
10.16.26.87.3306.bak
10.16.26.87.3306.0
10.16.26.87.3306.0.bak
10.16.26.87.3306.1
10.16.26.87.3306.1.bak