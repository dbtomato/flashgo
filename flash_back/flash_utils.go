package main

import "time"

func is_valid_datetime(dateStr string) bool {
	_, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err != nil {
		return false
	}
	return true
}
