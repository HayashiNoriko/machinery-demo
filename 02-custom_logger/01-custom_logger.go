package main

import (
	"fmt"
	"os"

	"github.com/RichardKnop/machinery/v1/log"
)

type myLogger struct{}

func (l *myLogger) Print(v ...interface{}) {
	fmt.Printf("[mydebug]")
	fmt.Print(v...)
}

func (l *myLogger) Printf(s string, v ...interface{}) {
	fmt.Printf("[mydebug]")
	fmt.Printf(s, v...)
}

func (l *myLogger) Println(v ...interface{}) {
	fmt.Printf("[mydebug]")
	fmt.Println(v...)
}

func (l *myLogger) Fatal(v ...interface{}) {
	fmt.Printf("[myfatal]")
	fmt.Print(v...)
	os.Exit(1)
}

func (l *myLogger) Fatalf(s string, v ...interface{}) {
	fmt.Printf("[myfatal]")
	fmt.Printf(s, v...)
	os.Exit(1)
}

func (l *myLogger) Fatalln(v ...interface{}) {
	fmt.Printf("[myfatal]")
	fmt.Println(v...)
	os.Exit(1)
}

func (l *myLogger) Panic(v ...interface{}) {
	fmt.Printf("[mypanic]")
	fmt.Print(v...)
	os.Exit(1)
}

func (l *myLogger) Panicf(s string, v ...interface{}) {
	fmt.Printf("[mypanic]")
	fmt.Printf(s, v...)
	os.Exit(1)
}

func (l *myLogger) Panicln(v ...interface{}) {
	fmt.Printf("[mypanic]")
	fmt.Println(v...)
	os.Exit(1)
}

func main() {
	log.Set(&myLogger{})

	log.DEBUG.Println("debug-print log")
	log.DEBUG.Fatalln("debug-fatal log")
	log.DEBUG.Panicln("debug-panic log")

	log.INFO.Println("info log")
	log.WARNING.Println("warning log")
	log.ERROR.Println("error log")
	log.FATAL.Println("fatal log")
}
