package main

//import (
//	"database/sql"
//	_ "github.com/go-sql-driver/mysql"
//)
//import "fmt"
//
//func main() {
//	dsn:="percona1:ppercona1234@tcp(10.16.4.125:3306)/test1"
//	db,err := sql.Open("mysql",dsn)
//	if err !=nil{
//
//	}
//	defer db.Close()
//	//row := DB.QueryRow("SHOW MASTER STATUS")
//	rows,err:= db.Query("select id ,`name` from t1")
//	defer rows.Close()
//
//	if err !=nil{
//		fmt.Println(err.Error())
//	}
//
//	id1 := 0
//	name1 := ""
//
//	for rows.Next() {
//		rows.Scan(&id1, &name1)
//		fmt.Println(id1, name1)
//	}
//
//
//}
