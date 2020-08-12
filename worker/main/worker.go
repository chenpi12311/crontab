package main

import (
	"github.com/chenpi12311/crontab/worker"
	"flag"
	"fmt"
	"runtime"
	"time"
)

var (
	confFile string // 配置文件路径
)

// 解析命令行参数
func initArgs() {
	// worker -config ./worker.json
	// worker -h
	flag.StringVar(&confFile, "config", "./worker.json", "指定worker.json")
	flag.Parse()
}

// 初始化线程数量
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)

	// 初始化命令行参数
	initArgs()

	// 初始化线程
	initEnv()

	// 加载配置
	if err = worker.InitConfig(confFile); err != nil {
		goto ERR
	}

	// 加载JobManager
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}

	if err = worker.G_jobMgr.WatchJobs(); err != nil {
		goto ERR
	}

	fmt.Println("启动成功")
	for {
		time.Sleep(1 * time.Second)
	}
	// 正常退出
	return

ERR:
	fmt.Println(err)

}
