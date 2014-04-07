package main

import (
	"strconv"
)

// Эквивалент map(int, array)
func StringArrayToIntArray(array []string) ([]int, error) {
	result := make([]int, len(array))
	for i, str := range array {
		val, err := strconv.ParseInt(str, 10, 32)
		if err != nil {
			return nil, err
		}
		result[i] = int(val)
	}
	return result, nil
}

func IntarrayToSet(array []int) map[int]bool {
	result := make(map[int]bool)
	for _, i := range(array) {
		result[i] = true
	}
	return result
}
