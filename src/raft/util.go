package raft

import (
	"fmt"

	"github.com/fatih/color"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	red := color.New(color.FgRed).SprintFunc()
	if Debug > 0 {
		msg:=fmt.Sprintf(format,a...)
		fmt.Print(red(msg))
	}
	return
}

func DPrintfR(format string, a ...interface{}) (n int, err error) {
	red := color.New(color.FgRed).SprintFunc()
	if Debug > 0 {
		msg:=fmt.Sprintf(format,a...)
		fmt.Print(red(msg))
	}
	return
}
func DPrintB(format string, a ...interface{}) (n int, err error) {
	red := color.New(color.FgBlue).SprintFunc()
	if Debug > 0 {
		msg:=fmt.Sprintf(format,a...)
		fmt.Print(red(msg))
	}
	return
}