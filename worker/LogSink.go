package worker

import (
	"context"
	"time"

	"github.com/chenpi12311/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// LogSink mongodb 存储日志
type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	// 单例
	G_logSink *LogSink
)

// Append 追加日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default:
		// 队列满了就丢弃
	}

}

func (logSink *LogSink) saveLogs(logBatch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), logBatch.Logs)
}

func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch // 当前日志批次
		commitTimer  *time.Timer
		timeoutBatch *common.LogBatch // 过期日志批次
	)

	for {
		select {
		case log = <-logSink.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				// 让这个批次超时自动提交(给1s的时间)
				commitTimer = time.AfterFunc(
					time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond,
					func(logBatch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- logBatch
						}
					}(logBatch),
				)
			}

			// 把新日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				logSink.saveLogs(logBatch)
				// 清空logBatch
				logBatch = nil
				// 取消定时器
				commitTimer.Stop()
			}
		case timeoutBatch = <-logSink.autoCommitChan: // 过期的批次
			// 判断过期批次是否仍旧是当前的批次
			if timeoutBatch != logBatch {
				continue // 跳过已经被提交的批次
			}
			// 把批次写入到mongo中
			logSink.saveLogs(timeoutBatch)
			// 清空logBatch
			logBatch = nil
		}
	}
}

// InitLogSink 初始化
func InitLogSink() (err error) {
	var (
		client     *mongo.Client
		database   *mongo.Database
		collection *mongo.Collection
	)

	if client, err = mongo.NewClient(
		options.Client().ApplyURI(G_config.MongodbUri),
		options.Client().SetConnectTimeout(time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond),
	); err != nil {
		return
	}

	if err = client.Connect(context.TODO()); err != nil {
		return
	}

	database = client.Database(G_config.MongodbDatabase)
	collection = database.Collection(G_config.MongodbCollection)

	G_logSink = &LogSink{
		client:         client,
		logCollection:  collection,
		logChan:        make(chan *common.JobLog, 2000),
		autoCommitChan: make(chan *common.LogBatch, 1),
	}

	// 启动一个MongoDB处理协程
	go G_logSink.writeLoop()

	return
}
