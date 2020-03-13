package main

import "time"
import "fmt"

func main() {
	stime2, err := time.Parse("2006-01-02 15:04:05", "1980-01-01 00:00:00")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(stime2)
}
