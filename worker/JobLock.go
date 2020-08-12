package worker

import (
	"context"
	"github.com/chenpi12311/crontab/common"
	"github.com/coreos/etcd/clientv3"
)

// JobLock 分布式锁(TXN事务)
type JobLock struct {
	kv clientv3.KV
	lease clientv3.Lease

	jobName string // 任务名
	cancelFunc context.CancelFunc
	leaseID clientv3.LeaseID
	isLocked bool
}

// InitJobLock 初始化一把锁
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		kv: kv,
		lease: lease,
		jobName: jobName,
	}
	return
}

// TryLock 尝试上锁
func (jobLock *JobLock) TryLock() (err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseID clientv3.LeaseID
		cancelCtx context.Context
		cancelFunc context.CancelFunc
		keepResponseChan <-chan*clientv3.LeaseKeepAliveResponse
		txn clientv3.Txn
		lockKey string
		txnResponse *clientv3.TxnResponse
	)
	// 1. 创建租约(5s)
	if leaseGrantResp, err = jobLock.lease.Grant(context.TODO(), 5); err != nil {
		return
	}

	leaseID = leaseGrantResp.ID

	// 创建cancelFunc
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())

	// 2. 自动续租
	if keepResponseChan, err = jobLock.lease.KeepAlive(cancelCtx, leaseID); err != nil {
		// 取消租约
		goto REVOKE
	}
	// 处理自动续租的时间队列
	go func() {
		var (
			keepResp *clientv3.LeaseKeepAliveResponse
		)

		for {
			select {
			case keepResp = <-keepResponseChan:
				if keepResp == nil {
					goto END
				}
			}
		}
		END:
	}()

	// 3. 创建事务txn
	lockKey = common.JOB_LOCK_DIR + jobLock.jobName
	txn = jobLock.kv.Txn(context.TODO())

	// 4. 事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseID))).
		Else(clientv3.OpGet(lockKey))

	if txnResponse, err = txn.Commit(); err != nil {
		goto REVOKE
	}

	// 5. 成功则返回，失败则释放租约
	if !txnResponse.Succeeded { // 锁被占用了
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto REVOKE
	}

	// 抢锁成功
	jobLock.cancelFunc = cancelFunc
	jobLock.leaseID = leaseID
	jobLock.isLocked = true
	return

REVOKE:
	// 取消自动续租
	cancelFunc()
	// 释放租约
	jobLock.lease.Revoke(context.TODO(), leaseID)
	return
}

// UnLock 释放锁
func (jobLock *JobLock) UnLock() {
	if jobLock.isLocked {
		jobLock.cancelFunc() // 取消程序自动续租的协程
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseID) // 释放租约, 关联的Key也会被删除
	}
}
