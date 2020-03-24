package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)
import "fmt"

func GetColsInfo(db *sql.DB, tableId string, tableSchema string, tableName string, colsMap map[string][]string) (colsMaps map[string][]string) {
	col_sql := fmt.Sprintf("SELECT COLUMN_NAME FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s';", tableSchema, tableName)
	fmt.Println(col_sql)
	rows, err := db.Query(col_sql)
	if err != nil {
		fmt.Print(err.Error())
	}
	var colsArray []string
	for rows.Next() {
		var colName string
		rows.Scan(&colName)
		colsArray = append(colsArray, colName)
	}
	colsMap = make(map[string][]string)
	m_key := tableId + "." + tableSchema + "." + tableName
	colsMap[m_key] = colsArray
	fmt.Println(colsMap)
	return colsMap

}

func main() {
	dsn := "percona1:ppercona1234@tcp(10.16.4.125:3306)/test1"
	db, err := sql.Open("mysql", dsn)
	if err != nil {

	}
	defer db.Close()
	//row := DB.QueryRow("SHOW MASTER STATUS")
	rows, err := db.Query("select id ,`name` from t1")
	defer rows.Close()

	if err != nil {
		fmt.Println(err.Error())
	}

	id1 := 0
	name1 := ""

	for rows.Next() {
		rows.Scan(&id1, &name1)
		fmt.Println(id1, name1)
	}

	colsMap := make(map[string][]string)
	colsMaps := GetColsInfo(db, "1", "test1", "t2", colsMap)
	fmt.Println(colsMaps)
}
