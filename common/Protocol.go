package common

import (
	"github.com/gorhill/cronexpr"
	"time"
	"strings"
	"encoding/json"
)

// Job 定时任务
type Job struct {
	Name     string `json:"name"`     // 任务名
	Command  string `json:"command"`  // shell命令
	CronExpr string `json:"cronExpr"` // 任务触发时间
}

// JobSchedulerPlan 任务调度计划
type JobSchedulerPlan struct {
	Job *Job // 要调度的任务信息
	Expr *cronexpr.Expression // 解析好的cronexpr表达式
	NextTime time.Time // 下次调度时间
}

// Response HTTP接口应答
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

// JobEvent 任务变化事件
type JobEvent struct {
	EventType int // SAVE, DELETE
	Job *Job
}

// BuildResponse 应答方法
func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	// 1. 定义一个response
	var (
		response Response
	)

	response.Errno = errno
	response.Msg = msg
	response.Data = data

	// 2. 序列化json
	resp, err = json.Marshal(response)

	return
}

// 反序列化Job
func UnpackJob(value []byte) (ret *Job, err error) {

	var (
		job *Job
	)

	job = &Job{}
	if err = json.Unmarshal(value, job); err != nil {
		return 
	}

	ret = job

	return
}

// 从etcd的key中提取任务名
func ExtractJobName(jobKey string) (name string) {
	name = strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
	return
}

// 任务变化事件：1）更新任务  2）删除任务
// BuildJobEvent 构造变化事件
func BuildJobEvent(eventType int, job *Job) (*JobEvent) {
	return &JobEvent{EventType: eventType, Job: job}
}

// BuildJobSchedulerPlan 构造任务执行计划
func BuildJobSchedulerPlan(job *Job) (jobSchedulerPlan *JobSchedulerPlan, err error) {
	var (
		expr *cronexpr.Expression
	)

	// 解析Job的cron表达式
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}

	// 生成 任务调度计划 对象
	jobSchedulerPlan = &JobSchedulerPlan{
		Job: job,
		Expr: expr,
		NextTime: expr.Next(time.Now()),
	}

	return
}