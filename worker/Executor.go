package worker

import (
	"os/exec"
	"time"

	"github.com/chenpi12311/crontab/common"
)

// Executor 执行器
type Executor struct {
}

var (
	// G_executor 执行器单例
	G_executor *Executor
)

// ExecuteJob 执行任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var (
			cmd       *exec.Cmd
			err       error
			output    []byte
			result    *common.JobExecuteResult
			startTime time.Time
			endTime   time.Time
			jobLock   *JobLock
		)

		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}

		// 初始化分布式锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		// 抢锁
		err = jobLock.TryLock()
		defer jobLock.UnLock()

		if err != nil { // 上锁失败
			result.Err = err
			result.EndTime = time.Now()
		} else {
			// 执行shell命令
			cmd = exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)

			startTime = time.Now()

			// 执行并捕获输出
			output, err = cmd.CombinedOutput()

			endTime = time.Now()

			// 更新任务执行结果
			result.Output = output
			result.StartTime = startTime
			result.EndTime = endTime
			result.Err = err
		}

		// 任务执行完成后 把执行的结果返回给Scheduler Scheduler会从executingTable中删掉执行记录
		G_scheduler.PushJobResult(result)
	}()
}

// InitExecutor 初始化执行器
func InitExecutor() (err error) {

	G_executor = &Executor{}

	return
}
