package worker

import (
	"github.com/chenpi12311/crontab/common"
)

// Scheduler 任务调度
type Scheduler struct {
	jobEventChan chan*common.JobEvent
}

var (
	G_scheduler *Scheduler
)

// InitScheduler 初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan: make(chan*common.JobEvent, 1000),
	}

	// 启动调度协程

	return
}
