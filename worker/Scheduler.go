package worker

import (
	"fmt"
	"time"
	"github.com/chenpi12311/crontab/common"
)

// Scheduler 任务调度
type Scheduler struct {
	// etcd任务事件队列
	jobEventChan chan*common.JobEvent
	// 任务调度计划表
	jobPlanTable map[string]*common.JobSchedulerPlan
	// 任务执行表
	jobExecutingTable map[string]*common.JobExecuteInfo
	// 任务执行结果队列
	jobExecuteResultChan chan*common.JobExecuteResult
}

var (
	G_scheduler *Scheduler
)

// 处理任务事件
func (g *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		err error
		jobSchedulerPlan *common.JobSchedulerPlan
		jobExisted bool
	)

	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE: // 保存任务事件
		if jobSchedulerPlan, err = common.BuildJobSchedulerPlan(jobEvent.Job); err != nil {
			return
		}
		g.jobPlanTable[jobEvent.Job.Name] = jobSchedulerPlan
	case common.JOB_EVENT_DEL: // 删除任务事件
		if jobSchedulerPlan, jobExisted = g.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(g.jobPlanTable, jobEvent.Job.Name)
		}
	}
}

// 处理任务执行结果
func (g *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	// 删除执行状态
	delete(g.jobExecutingTable, result.ExecuteInfo.Job.Name)

	fmt.Println("任务执行完成, 输出: ", string(result.Output), " 耗时: ", result.EndTime.Sub(result.StartTime))
}

// TryStartJob 尝试执行任务
func (g *Scheduler) TryStartJob(jobPlan *common.JobSchedulerPlan) {
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)
	// 调度和执行是两件事情
	
	// 执行的任务可能运行很久(比如1分钟调度60次，但是只能执行1次，防止并发)
	// 如果任务正在执行 跳过本次调度
	if jobExecuteInfo, jobExecuting = g.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		fmt.Println("任务正在执行，跳过任务: ", jobPlan.Job.Name)
		return
	}

	// 构建执行状态
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	// 保存执行状态
	g.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	// 执行任务
	G_executor.ExecuteJob(jobExecuteInfo)
	fmt.Println("执行任务: ", jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
}

// TryScheduler 重新计算任务调度状态
func (g *Scheduler) TryScheduler() (schedulerAfter time.Duration) {
	var (
		jobPlan *common.JobSchedulerPlan
		now time.Time
		nearTime *time.Time
	)

	// 如果任务表为空的话 睡眠1秒
	if len(g.jobPlanTable) == 0 {
		schedulerAfter = 1 * time.Second
		return
	}

	// 当前时间
	now = time.Now()

	// 1. 遍历所有任务
	for _, jobPlan = range g.jobPlanTable {
		// 2. 过期的任务立即执行
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			// 尝试执行任务
			g.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now) // 更新下次执行时间
		}

		// 统计最近一个要过期的任务事件
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}

	// 3. 统计最近的要过期的任务的时间(N秒后过期)
	schedulerAfter = (*nearTime).Sub(now)

	return
}

// 调度协程
func (g *Scheduler) scheduleLoop() {
	var (
		jobEvent *common.JobEvent
		schedulerAfter time.Duration
		schedulerTimer *time.Timer
		jobResult *common.JobExecuteResult
	)

	// 初始化一次
	schedulerAfter = g.TryScheduler()

	// 调度的延迟定时器
	schedulerTimer = time.NewTimer(schedulerAfter)

	// 获取任务事件
	for {
		select {
		case jobEvent = <-g.jobEventChan:
			// 对内存中维护的任务列表作增删改查
			g.handleJobEvent(jobEvent)
		case <-schedulerTimer.C:
			// 最近的任务到期了
		case jobResult = <-g.jobExecuteResultChan:
			// 监听任务执行结果
			g.handleJobResult(jobResult)
		}
		// 调度一次任务
		schedulerAfter = g.TryScheduler()
		schedulerTimer.Reset(schedulerAfter)
	}
}

// PushJobEvent 推送任务事件
func (g *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	g.jobEventChan <- jobEvent
}

// InitScheduler 初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan: make(chan*common.JobEvent, 1000),
		jobPlanTable: make(map[string]*common.JobSchedulerPlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobExecuteResultChan: make(chan*common.JobExecuteResult, 1000),
	}

	// 启动调度协程
	go G_scheduler.scheduleLoop()

	return
}

// PushJobResult 回传任务执行结果
func (g *Scheduler) PushJobResult(result *common.JobExecuteResult) {
	g.jobExecuteResultChan <- result
}