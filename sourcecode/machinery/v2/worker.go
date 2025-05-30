package machinery

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/opentracing/opentracing-go"

	"github.com/RichardKnop/machinery/v2/backends/amqp"
	"github.com/RichardKnop/machinery/v2/brokers/errs"
	"github.com/RichardKnop/machinery/v2/log"
	"github.com/RichardKnop/machinery/v2/retry"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/RichardKnop/machinery/v2/tracing"
)

// Worker represents a single worker process
type Worker struct {
	server            *Server
	ConsumerTag       string
	Concurrency       int
	Queue             string
	errorHandler      func(err error)
	preTaskHandler    func(*tasks.Signature)
	postTaskHandler   func(*tasks.Signature)
	preConsumeHandler func(*Worker) bool
}

var (
	// ErrWorkerQuitGracefully is return when worker quit gracefully
	ErrWorkerQuitGracefully = errors.New("Worker quit gracefully")
	// ErrWorkerQuitGracefully is return when worker quit abruptly
	ErrWorkerQuitAbruptly = errors.New("Worker quit abruptly")
)

// 同步（阻塞）启动，内部帮我们创建一个 errorsChan，然后调用 LaunchAsync，阻塞等待 errorsChan 消息，有消息（错误）才返回
// Launch starts a new worker process. The worker subscribes
// to the default queue and processes incoming registered tasks
func (worker *Worker) Launch() error {
	errorsChan := make(chan error)

	worker.LaunchAsync(errorsChan)

	return <-errorsChan
}

// 异步启动，不阻塞，需要自己传入 errorsChan
// LaunchAsync is a non blocking version of Launch
func (worker *Worker) LaunchAsync(errorsChan chan<- error) {
	cnf := worker.server.GetConfig()
	broker := worker.server.GetBroker()

	// 1.输出一些配置信息，省略
	// Log some useful information about worker configuration

	// 2. 启动消费任务的 goroutine
	var signalWG sync.WaitGroup
	// Goroutine to start broker consumption and handle retries when broker connection dies
	go func() {
		// 2.1. 循环调用 broker 的 StartConsuming 方法，启动任务消费
		for {
			retry, err := broker.StartConsuming(worker.ConsumerTag, worker.Concurrency, worker)

			if retry {
				// 2.2. 如果 retry 为 true，说明消费过程中发生了可重试的错误，会调用 errorHandler 或打印日志，然后继续重试
				if worker.errorHandler != nil {
					worker.errorHandler(err)
				} else {
					log.WARNING.Printf("Broker failed with error: %s", err)
				}
			} else {
				// 2.3. 如果 retry 为 false，说明遇到不可恢复的错误或收到退出信号，等待信号处理完成后，将错误写入 errorsChan 并退出 goroutine
				signalWG.Wait()
				errorsChan <- err // stop the goroutine
				return
			}
		}
	}()

	// 3. 信号监听与优雅/强制退出
	if !cnf.NoUnixSignals {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		var signalsReceived uint

		// Goroutine Handle SIGINT and SIGTERM signals
		go func() {
			for s := range sig {
				log.WARNING.Printf("Signal received: %v", s)
				signalsReceived++

				if signalsReceived < 2 {
					// 第一次收到信号时，优雅退出
					// After first Ctrl+C start quitting the worker gracefully
					log.WARNING.Print("Waiting for running tasks to finish before shutting down")
					signalWG.Add(1)
					go func() {
						// 这里会小小地阻塞一下，等待当前任务完成
						// 详细解释可以看 Broker 的相关笔记
						worker.Quit()
						errorsChan <- ErrWorkerQuitGracefully
						signalWG.Done()
					}()
				} else {
					// 第二次收到信号时，强制退出
					// Abort the program when user hits Ctrl+C second time in a row
					errorsChan <- ErrWorkerQuitAbruptly
				}
			}
		}()
	}
}

// 返回 Queue 字段。因为 worker 最终其实对外作为 iface.TaskProcessor 接口使用的，所以提供这个方法。（Broker 子类对外作为 iface.Broker 接口使用）
// CustomQueue returns Custom Queue of the running worker process
func (worker *Worker) CustomQueue() string {
	return worker.Queue
}

// Quit tears down the running worker process
func (worker *Worker) Quit() {
	worker.server.GetBroker().StopConsuming()
}

// Process handles received tasks and triggers success/error callbacks
func (worker *Worker) Process(signature *tasks.Signature) error {
	// 1. 校验任务是否已注册
	// If the task is not registered with this worker, do not continue
	// but only return nil as we do not want to restart the worker process
	if !worker.server.IsTaskRegistered(signature.Name) {
		return nil
	}

	// 2.获取任务处理函数
	taskFunc, err := worker.server.GetRegisteredTask(signature.Name)
	if err != nil {
		return nil
	}

	// 3. 设置任务状态为 RECEIVED
	// Update task state to RECEIVED
	if err = worker.server.GetBackend().SetStateReceived(signature); err != nil {
		return fmt.Errorf("Set state to 'received' for task %s returned error: %s", signature.UUID, err)
	}

	// 4. 构造任务对象
	// Prepare task for processing
	task, err := tasks.NewWithSignature(taskFunc, signature)
	// if this failed, it means the task is malformed, probably has invalid
	// signature, go directly to task failed without checking whether to retry
	// 如果构造失败，调用 taskFailed 记录失败
	if err != nil {
		worker.taskFailed(signature, err)
		return err
	}

	// 5. 分布式追踪（tracing）支持
	// try to extract trace span from headers and add it to the function context
	// so it can be used inside the function if it has context.Context as the first
	// argument. Start a new span if it isn't found.
	// 从任务 header 中提取 trace 信息，构建 trace span，便于链路追踪
	taskSpan := tracing.StartSpanFromHeaders(signature.Headers, signature.Name)
	tracing.AnnotateSpanWithSignatureInfo(taskSpan, signature)
	task.Context = opentracing.ContextWithSpan(task.Context, taskSpan)

	// 6. 设置任务状态为 STARTED
	// Update task state to STARTED
	if err = worker.server.GetBackend().SetStateStarted(signature); err != nil {
		return fmt.Errorf("Set state to 'started' for task %s returned error: %s", signature.UUID, err)
	}

	// 7. 执行任务前置钩子
	//Run handler before the task is called
	if worker.preTaskHandler != nil {
		worker.preTaskHandler(signature)
	}

	// 8. defer 执行任务后置钩子
	//Defer run handler for the end of the task
	if worker.postTaskHandler != nil {
		defer worker.postTaskHandler(signature)
	}

	// 9. 调用任务函数
	// Call the task
	// 9.1 如果 err==nil，没有返回错误，那么执行成功，调用 taskSucceeded
	// 9.2 如果 err!=nil，那么分情况讨论，依次判断：
	//   如果 err 是 tasks.ErrRetryTaskLater，调用 retryTaskIn 延迟重试
	//   如果有剩余重试次数，调用 taskRetry 立即重试
	//   以上都不是，调用 taskFailed
	results, err := task.Call()
	if err != nil {
		// If a tasks.ErrRetryTaskLater was returned from the task,
		// retry the task after specified duration
		retriableErr, ok := interface{}(err).(tasks.ErrRetryTaskLater)
		if ok {
			return worker.retryTaskIn(signature, retriableErr.RetryIn())
		}

		// Otherwise, execute default retry logic based on signature.RetryCount
		// and signature.RetryTimeout values
		if signature.RetryCount > 0 {
			return worker.taskRetry(signature)
		}

		return worker.taskFailed(signature, err)
	}

	return worker.taskSucceeded(signature, results)
}

// 任务失败且有重试次数时，更新状态为 RETRY，递减重试次数，增加重试超时时间（Fibonacci 退避），设置新的 ETA，并重新投递到队列
// retryTask decrements RetryCount counter and republishes the task to the queue
func (worker *Worker) taskRetry(signature *tasks.Signature) error {
	// Update task state to RETRY
	if err := worker.server.GetBackend().SetStateRetry(signature); err != nil {
		return fmt.Errorf("Set state to 'retry' for task %s returned error: %s", signature.UUID, err)
	}

	// Decrement the retry counter, when it reaches 0, we won't retry again
	signature.RetryCount--

	// Increase retry timeout
	signature.RetryTimeout = retry.FibonacciNext(signature.RetryTimeout)

	// Delay task by signature.RetryTimeout seconds
	eta := time.Now().UTC().Add(time.Second * time.Duration(signature.RetryTimeout))
	signature.ETA = &eta

	log.WARNING.Printf("Task %s failed. Going to retry in %d seconds.", signature.UUID, signature.RetryTimeout)

	// Send the task back to the queue
	_, err := worker.server.SendTask(signature)
	return err
}

// 任务失败且返回 ErrRetryTaskLater 时，更新状态为 RETRY，设置新的 ETA（当前时间+retryIn），并重新投递到队列
// taskRetryIn republishes the task to the queue with ETA of now + retryIn.Seconds()
func (worker *Worker) retryTaskIn(signature *tasks.Signature, retryIn time.Duration) error {
	// Update task state to RETRY
	if err := worker.server.GetBackend().SetStateRetry(signature); err != nil {
		return fmt.Errorf("Set state to 'retry' for task %s returned error: %s", signature.UUID, err)
	}

	// Delay task by retryIn duration
	eta := time.Now().UTC().Add(retryIn)
	signature.ETA = &eta

	log.WARNING.Printf("Task %s failed. Going to retry in %.0f seconds.", signature.UUID, retryIn.Seconds())

	// Send the task back to the queue
	_, err := worker.server.SendTask(signature)
	return err
}

// 任务成功时，更新状态为 SUCCESS，记录日志，触发成功回调（OnSuccess），如果是 group 任务还会检查 group 是否全部完成，必要时触发 chord 回调
// taskSucceeded updates the task state and triggers success callbacks or a
// chord callback if this was the last task of a group with a chord callback
func (worker *Worker) taskSucceeded(signature *tasks.Signature, taskResults []*tasks.TaskResult) error {
	// Update task state to SUCCESS
	if err := worker.server.GetBackend().SetStateSuccess(signature, taskResults); err != nil {
		return fmt.Errorf("Set state to 'success' for task %s returned error: %s", signature.UUID, err)
	}

	// Log human readable results of the processed task
	var debugResults = "[]"
	results, err := tasks.ReflectTaskResults(taskResults)
	if err != nil {
		log.WARNING.Print(err)
	} else {
		debugResults = tasks.HumanReadableResults(results)
	}
	log.DEBUG.Printf("Processed task %s. Results = %s", signature.UUID, debugResults)

	// Trigger success callbacks

	for _, successTask := range signature.OnSuccess {
		if signature.Immutable == false {
			// Pass results of the task to success callbacks
			for _, taskResult := range taskResults {
				successTask.Args = append(successTask.Args, tasks.Arg{
					Type:  taskResult.Type,
					Value: taskResult.Value,
				})
			}
		}

		worker.server.SendTask(successTask)
	}

	// If the task was not part of a group, just return
	if signature.GroupUUID == "" {
		return nil
	}

	// There is no chord callback, just return
	if signature.ChordCallback == nil {
		return nil
	}

	// Check if all task in the group has completed
	groupCompleted, err := worker.server.GetBackend().GroupCompleted(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		return fmt.Errorf("Completed check for group %s returned error: %s", signature.GroupUUID, err)
	}

	// If the group has not yet completed, just return
	if !groupCompleted {
		return nil
	}

	// Defer purging of group meta queue if we are using AMQP backend
	if worker.hasAMQPBackend() {
		defer worker.server.GetBackend().PurgeGroupMeta(signature.GroupUUID)
	}

	// Trigger chord callback
	shouldTrigger, err := worker.server.GetBackend().TriggerChord(signature.GroupUUID)
	if err != nil {
		return fmt.Errorf("Triggering chord for group %s returned error: %s", signature.GroupUUID, err)
	}

	// Chord has already been triggered
	if !shouldTrigger {
		return nil
	}

	// Get task states
	taskStates, err := worker.server.GetBackend().GroupTaskStates(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		log.ERROR.Printf(
			"Failed to get tasks states for group:[%s]. Task count:[%d]. The chord may not be triggered. Error:[%s]",
			signature.GroupUUID,
			signature.GroupTaskCount,
			err,
		)
		return nil
	}

	// Append group tasks' return values to chord task if it's not immutable
	for _, taskState := range taskStates {
		if !taskState.IsSuccess() {
			return nil
		}

		if signature.ChordCallback.Immutable == false {
			// Pass results of the task to the chord callback
			for _, taskResult := range taskState.Results {
				signature.ChordCallback.Args = append(signature.ChordCallback.Args, tasks.Arg{
					Type:  taskResult.Type,
					Value: taskResult.Value,
				})
			}
		}
	}

	// Send the chord task
	_, err = worker.server.SendTask(signature.ChordCallback)
	if err != nil {
		return err
	}

	return nil
}

// 任务失败时，更新状态为 FAILURE，调用错误处理器（errorHandler），触发错误回调（OnError），并根据配置决定是否阻止任务删除
// taskFailed updates the task state and triggers error callbacks
func (worker *Worker) taskFailed(signature *tasks.Signature, taskErr error) error {
	// Update task state to FAILURE
	if err := worker.server.GetBackend().SetStateFailure(signature, taskErr.Error()); err != nil {
		return fmt.Errorf("Set state to 'failure' for task %s returned error: %s", signature.UUID, err)
	}

	if worker.errorHandler != nil {
		worker.errorHandler(taskErr)
	} else {
		log.ERROR.Printf("Failed processing task %s. Error = %v", signature.UUID, taskErr)
	}

	// Trigger error callbacks
	for _, errorTask := range signature.OnError {
		// Pass error as a first argument to error callbacks
		args := append([]tasks.Arg{{
			Type:  "string",
			Value: taskErr.Error(),
		}}, errorTask.Args...)
		errorTask.Args = args
		worker.server.SendTask(errorTask)
	}

	if signature.StopTaskDeletionOnError {
		return errs.ErrStopTaskDeletion
	}

	return nil
}

// 判断所属的 server 的 backend 是不是 AMQP
// Returns true if the worker uses AMQP backend
func (worker *Worker) hasAMQPBackend() bool {
	_, ok := worker.server.GetBackend().(*amqp.Backend)
	return ok
}

// SetErrorHandler sets a custom error handler for task errors
// A default behavior is just to log the error after all the retry attempts fail
func (worker *Worker) SetErrorHandler(handler func(err error)) {
	worker.errorHandler = handler
}

// SetPreTaskHandler sets a custom handler func before a job is started
func (worker *Worker) SetPreTaskHandler(handler func(*tasks.Signature)) {
	worker.preTaskHandler = handler
}

// SetPostTaskHandler sets a custom handler for the end of a job
func (worker *Worker) SetPostTaskHandler(handler func(*tasks.Signature)) {
	worker.postTaskHandler = handler
}

// SetPreConsumeHandler sets a custom handler for the end of a job
func (worker *Worker) SetPreConsumeHandler(handler func(*Worker) bool) {
	worker.preConsumeHandler = handler
}

// GetServer returns server
func (worker *Worker) GetServer() *Server {
	return worker.server
}

// 在 worker 启动消费前调用，返回 false 可阻止 worker 启动消费任务
func (worker *Worker) PreConsumeHandler() bool {
	// preConsumeHandler 是 worker 的一个钩子函数
	if worker.preConsumeHandler == nil {
		return true
	}

	return worker.preConsumeHandler(worker)
}

// 对 URL 进行脱敏，只保留协议（scheme）和主机（host）部分
func RedactURL(urlString string) string {
	u, err := url.Parse(urlString)
	if err != nil {
		return urlString
	}
	return fmt.Sprintf("%s://%s", u.Scheme, u.Host)
}
