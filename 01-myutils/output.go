package myutils

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// 将输出写入文件（终端太多 DEBUG）
func Output(msg string) {
	// 1. 获取调用者的文件路径
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		file = "unknown"
	}

	// 2. 取 go 文件名（不带路径和扩展名）
	base := filepath.Base(file)
	name := strings.TrimSuffix(base, filepath.Ext(base))

	// 3. 日志文件名
	logFile := "output_" + name

	// 4. 日志文件路径（和 go 文件同目录）
	logPath := filepath.Join(filepath.Dir(file), logFile)

	// 5. 写入日志
	f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	defer f.Close()
	f.WriteString(msg + " " + time.Now().Format("2006-01-02 15:04:05") + "\n")
}
