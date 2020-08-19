package utils

import (
	"fmt"
	"os"
	"runtime"
	"strings"
)

type callerInfo struct {
	Caller    *runtime.Func
	PathSplit []string
	Line      int
}

func getFilePathSplit(iSkipLevel int) *callerInfo {
	fpcs := make([]uintptr, 1)

	// Skip 2 levels to get the direct caller
	// Skip 3 levels to get the caller's caller
	// etc.
	n := runtime.Callers(iSkipLevel, fpcs)
	if n == 0 {
		fmt.Println("MSG: NO CALLER")
	}

	caller := runtime.FuncForPC(fpcs[0] - 1)
	if caller == nil {
		fmt.Println("MSG CALLER WAS NIL")
	}

	// Print the file name and line number
	path, line := caller.FileLine(fpcs[0] - 1)

	callerInfo := &callerInfo{
		caller,
		strings.Split(path, string(os.PathSeparator)),
		line,
	}

	return callerInfo
}

/*
iBackTraceLevel:
	set to 1, returns just the caller's filepath (without the file name)
	set to n>1, returns the caller's filepath (without the file name) and every folder above recursively for n-1 times
*/
func GetCallerPaths(iBackTraceLevel int) []string {
	// 3 to return the caller of this function (2 to return this function itself)
	callerInfo := getFilePathSplit(3)

	callerFilePath := strings.Join(callerInfo.PathSplit[:len(callerInfo.PathSplit)-1], string(os.PathSeparator)) //-1 to exclude file name

	filePaths := []string{callerFilePath}
	for i := 1; i < iBackTraceLevel; i++ {
		fatherPathSplit := strings.Split(filePaths[len(filePaths)-1], string(os.PathSeparator))
		if fatherPathSplit[len(fatherPathSplit)-1] == "" { // we got to the system root folder
			break
		}
		filePaths = append(filePaths, strings.Join(fatherPathSplit[:len(fatherPathSplit)-1], string(os.PathSeparator)))
	}

	return filePaths
}
