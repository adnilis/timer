package timer

import (
	"context"
	"sync"
	"time"
)

// TimerActor 定义定时器 Actor 的接口。
// 它提供了调度和管理带有回调函数的定时器的方法。
type TimerActor interface {
	// Add 调度一个在指定延迟后执行的一次性定时任务。
	// 参数：
	//   - delay: 执行任务前等待的时间
	//   - fn: 要执行的回调函数
	//   - async: 是否异步执行回调（可选，默认为 false）
	// 返回可用于删除定时器的定时器 ID。
	Add(delay time.Duration, fn func(), async ...bool) uint64

	// AddSchedule 根据固定日期调度器调度一个重复执行的任务。
	// 参数：
	//   - schedule: 调度配置
	//   - fn: 要执行的回调函数
	//   - async: 是否异步执行回调（可选，默认为 false）
	// 返回可用于删除定时器的定时器 ID。
	AddSchedule(schedule *FixedDateSchedule, fn func(), async ...bool) uint64

	// AddCronSchedule 根据 cron 表达式调度一个重复执行的任务。
	// 参数：
	//   - expr: cron 表达式字符串（支持 5 字段和 6 字段格式）
	//   - fn: 要执行的回调函数
	//   - async: 是否异步执行回调（可选，默认为 false）
	// 返回可用于删除定时器的定时器 ID。
	//
	// 支持的 cron 表达式格式：
	//   - 5 字段：分 时 日 月 周
	//   - 6 字段：秒 分 时 日 月 周
	//   - 描述符：@yearly、@monthly、@weekly、@daily、@hourly
	//
	// 示例：
	//   - "0 * * * *" - 每小时
	//   - "*/15 * * * *" - 每 15 分钟
	//   - "0 9 * * 1-5" - 每个工作日上午 9 点
	//   - "0 0 1 * *" - 每月 1 号午夜
	//   - "@daily" - 每天午夜一次
	//   - "*/30 * * * * *" - 每 30 秒（6 字段格式）
	//
	// 如果 cron 表达式无效，返回 0 并输出错误日志。
	AddCronSchedule(expr string, fn func(), async ...bool) uint64

	// AddScheduleOnce 根据固定日期调度器调度一个执行一次的任务。
	// 参数：
	//   - schedule: 调度配置
	//   - fn: 要执行的回调函数
	//   - async: 是否异步执行回调（可选，默认为 false）
	// 返回可用于删除定时器的定时器 ID。
	AddScheduleOnce(schedule *FixedDateSchedule, fn func(), async ...bool) uint64

	// Once 调度一个在指定延迟后执行的一次性定时任务。
	// 这是 Add 的别名，具有单次执行的语义。
	// 参数：
	//   - delay: 执行任务前等待的时间
	//   - fn: 要执行的回调函数
	//   - async: 是否异步执行回调（可选，默认为 false）
	// 返回可用于删除定时器的定时器 ID。
	Once(delay time.Duration, fn func(), async ...bool) uint64

	// Remove 停止并删除具有指定 ID 的定时器。
	// 如果定时器已经触发或不存在，此操作无效。
	Remove(id uint64)
}

// DefaultTimerActor 是 TimerActor 接口的默认实现。
// 它内部使用 TimeWheel 来管理定时器，并维护一个 id 到 Timer 的映射以支持 Remove 操作。
type DefaultTimerActor struct {
	tw     *TimeWheel
	timers map[uint64]*Timer
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

// NewDefaultTimerActor 创建一个新的默认定时器 Actor。
//
// 参数：
//   - tick: 时间轮的刻度间隔（tick interval）
//   - wheelSize: 时间轮的大小（轮盘上 bucket 的数量）
//
// 返回一个初始化好的 DefaultTimerActor 实例。
//
// 使用示例：
//
//	actor := NewDefaultTimerActor(10*time.Millisecond, 60)
//	actor.Start(context.Background())
//	defer actor.Stop()
//
//	id := actor.Add(1*time.Second, func() {
//	    fmt.Println("任务执行！")
//	})
//
//	// 如果需要取消任务
//	actor.Remove(id)
func NewDefaultTimerActor(tick time.Duration, wheelSize int64) *DefaultTimerActor {
	ctx, cancel := context.WithCancel(context.Background())
	return &DefaultTimerActor{
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start 启动默认定时器 Actor。
//
// 参数：
//   - tick: 时间轮的刻度间隔
//   - wheelSize: 时间轮的大小
//
// 此方法会创建并启动内部的 TimeWheel。如果在调用 Start 之前
// 调用了其他方法（Add、Once 等），会自动使用默认参数启动 TimeWheel。
func (a *DefaultTimerActor) Start(tick time.Duration, wheelSize int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.tw != nil {
		return nil // 已经启动
	}

	tw, err := NewTimeWheel(tick, wheelSize)
	if err != nil {
		return err
	}

	a.tw = tw
	a.timers = make(map[uint64]*Timer)
	a.tw.Start(a.ctx)

	return nil
}

// Stop 停止默认定时器 Actor。
//
// 此方法会停止内部的 TimeWheel，并清理所有资源。
// 已经触发的任务不会被取消。
func (a *DefaultTimerActor) Stop() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.tw == nil {
		return
	}

	a.tw.Stop()
	a.tw = nil
	a.timers = nil
	a.cancel()
}

// Add 调度一个在指定延迟后执行的一次性定时任务。
// 参数：
//   - delay: 执行任务前等待的时间
//   - fn: 要执行的回调函数
//   - async: 是否异步执行回调（可选，默认为 false）
//
// 返回可用于删除定时器的定时器 ID。
func (a *DefaultTimerActor) Add(delay time.Duration, fn func(), async ...bool) uint64 {
	a.ensureStarted()

	id := NextId()
	timer := a.tw.AfterFunc(id, delay, fn, async...)

	a.mu.Lock()
	a.timers[id] = timer
	a.mu.Unlock()

	return id
}

// Once 调度一个在指定延迟后执行的一次性定时任务。
// 这是 Add 的别名，语义相同。
// 参数：
//   - delay: 执行任务前等待的时间
//   - fn: 要执行的回调函数
//   - async: 是否异步执行回调（可选，默认为 false）
//
// 返回可用于删除定时器的定时器 ID。
func (a *DefaultTimerActor) Once(delay time.Duration, fn func(), async ...bool) uint64 {
	return a.Add(delay, fn, async...)
}

// AddSchedule 根据固定日期调度器调度一个重复执行的任务。
// 参数：
//   - schedule: 调度配置
//   - fn: 要执行的回调函数
//   - async: 是否异步执行回调（可选，默认为 false）
//
// 返回可用于删除定时器的定时器 ID。
func (a *DefaultTimerActor) AddSchedule(schedule *FixedDateSchedule, fn func(), async ...bool) uint64 {
	a.ensureStarted()

	id := NextId()
	timer := a.tw.ScheduleFunc(id, schedule, fn, async...)

	a.mu.Lock()
	a.timers[id] = timer
	a.mu.Unlock()

	return id
}

// AddCronSchedule 根据cron 表达式调度一个重复执行的任务。
// 参数：
//   - expr: cron 表达式字符串
//   - fn: 要执行的回调函数
//   - async: 是否异步执行回调（可选，默认为 false）
//
// 返回可用于删除定时器的定时器 ID。
// 如果 cron 表达式无效，返回 0。
func (a *DefaultTimerActor) AddCronSchedule(expr string, fn func(), async ...bool) uint64 {
	schedule, err := NewCronSchedule(expr)
	if err != nil {
		// 无效的 cron 表达式，返回 0 表示失败
		return 0
	}

	a.ensureStarted()

	id := NextId()
	timer := a.tw.ScheduleFunc(id, schedule, fn, async...)

	a.mu.Lock()
	a.timers[id] = timer
	a.mu.Unlock()

	return id
}

// AddScheduleOnce 根据固定日期调度器调度一个执行一次的任务。
// 参数：
//   - schedule: 调度配置
//   - fn: 要执行的回调函数
//   - async: 是否异步执行回调（可选，默认为 false）
//
// 返回可用于删除定时器的定时器 ID。
func (a *DefaultTimerActor) AddScheduleOnce(schedule *FixedDateSchedule, fn func(), async ...bool) uint64 {
	a.ensureStarted()

	id := NextId()
	timer := a.tw.ScheduleFunc(id, schedule, fn, async...)

	a.mu.Lock()
	a.timers[id] = timer
	a.mu.Unlock()

	return id
}

// Remove 停止并删除具有指定 ID 的定时器。
// 如果定时器已经触发或不存在，此操作无效。
func (a *DefaultTimerActor) Remove(id uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.tw == nil || a.timers == nil {
		return
	}

	if timer, ok := a.timers[id]; ok {
		timer.Stop()
		delete(a.timers, id)
	}
}

// ensureStarted 确保 TimeWheel 已经启动。
// 如果尚未启动，则使用默认参数（10ms tick, 60 wheel size）启动。
func (a *DefaultTimerActor) ensureStarted() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.tw == nil {
		// 确保 context 已经初始化（如果已取消，则创建新的）
		if a.ctx == nil {
			a.ctx, a.cancel = context.WithCancel(context.Background())
		} else {
			// 检查 context 是否已被取消
			select {
			case <-a.ctx.Done():
				// context 已被取消，创建新的
				a.ctx, a.cancel = context.WithCancel(context.Background())
			default:
				// context 仍然有效
			}
		}

		tw, _ := NewTimeWheel(10*time.Millisecond, 60) // 默认参数
		a.tw = tw
		a.timers = make(map[uint64]*Timer)
		a.tw.Start(a.ctx)
	}
}

var (
	defaultActor   TimerActor
	defaultActorMu sync.RWMutex
)

// getDefaultActor 返回当前的默认定时器 Actor。
//
// 如果尚未设置（或设置为 nil），会自动创建一个新的 DefaultTimerActor。
// 这样可以确保在没有显式调用 StartActor 的情况下，包级函数也能正常工作。
func getDefaultActor() TimerActor {
	defaultActorMu.RLock()
	actor := defaultActor
	defaultActorMu.RUnlock()

	if actor == nil {
		defaultActorMu.Lock()
		// 双重检查锁定
		if defaultActor == nil {
			// 自动创建并启动默认 actor（确保 ctx 已初始化）
			ctx, cancel := context.WithCancel(context.Background())
			actor := &DefaultTimerActor{
				ctx:    ctx,
				cancel: cancel,
			}
			actor.Start(10*time.Millisecond, 60) // 使用默认参数启动
			defaultActor = actor
		}
		actor = defaultActor
		defaultActorMu.Unlock()
	}

	return actor
}

// StartActor 设置包级函数的默认定时器 Actor。
// 必须在使用 Add、Once、AddSchedule 或 Remove 函数之前调用此函数。
//
// 如果传入 nil，则会创建一个默认的 DefaultTimerActor 并启动它。
//
// 示例：
//
//	actor := NewDefaultTimerActor(10*time.Millisecond, 60)
//	actor.Start(10*time.Millisecond, 60)
//	StartActor(actor)
func StartActor(actor TimerActor) {
	defaultActorMu.Lock()
	defer defaultActorMu.Unlock()

	if actor == nil {
		// 如果传入 nil，创建一个默认的 DefaultTimerActor（带有初始化的 ctx）
		ctx, cancel := context.WithCancel(context.Background())
		defaultActor = &DefaultTimerActor{
			ctx:    ctx,
			cancel: cancel,
		}
		// 启动默认 actor
		defaultActor.(*DefaultTimerActor).Start(10*time.Millisecond, 60)
		return
	}
	defaultActor = actor
}

// GetDefaultActor 返回当前的默认定时器 Actor。
//
// 如果尚未设置（或设置为 nil），会自动创建一个新的 DefaultTimerActor。
// 这样可以确保在没有显式调用 StartActor 的情况下，包级函数也能正常工作。
func GetDefaultActor() TimerActor {
	return getDefaultActor()
}

// Add 调度一个在指定延迟后执行的一次性定时任务。
// 这是使用默认 Actor 的便捷函数。
// 如果尚未设置默认 Actor，会自动创建一个 DefaultTimerActor。
//
// 参数：
//   - delay: 执行任务前等待的时间
//   - fn: 要执行的回调函数
//   - async: 是否异步执行回调（可选，默认为 false）
//
// 返回可用于删除定时器的定时器 ID。
//
// 示例：
//
//	id := Add(1*time.Second, func() {
//	    fmt.Println("1秒后执行")
//	})
func Add(delay time.Duration, fn func(), async ...bool) uint64 {
	return GetDefaultActor().Add(delay, fn, async...)
}

// AddSchedule 根据固定日期调度器调度一个重复执行的任务。
// 这是使用默认 Actor 的便捷函数。
// 如果尚未设置默认 Actor，会自动创建一个 DefaultTimerActor。
//
// 参数：
//   - schedule: 调度配置
//   - fn: 要执行的回调函数
//   - async: 是否异步执行回调（可选，默认为 false）
//
// 返回可用于删除定时器的定时器 ID。
//
// 示例：
//
//	schedule, _ := NewCronSchedule("0 * * * *")
//	id := AddSchedule(schedule, func() {
//	    fmt.Println("每小时执行")
//	})
func AddSchedule(schedule *FixedDateSchedule, fn func(), async ...bool) uint64 {
	return GetDefaultActor().AddSchedule(schedule, fn, async...)
}

// AddCronSchedule 根据cron 表达式调度一个重复执行的任务。
// 这是使用默认 Actor 的便捷函数。
// 如果尚未设置默认 Actor，会自动创建一个 DefaultTimerActor。
//
// 参数：
//   - expr: cron 表达式字符串
//   - fn: 要执行的回调函数
//   - async: 是否异步执行回调（可选，默认为 false）
//
// 返回可用于删除定时器的定时器 ID。
// 如果 cron 表达式无效，返回 0。
//
// 使用示例：
//
//	id := timer.AddCronSchedule("0 */5 * * * *", func() {
//	    fmt.Println("每 5 秒执行一次")
//	})
//
//	// 每 15 分钟执行一次
//	timer.AddCronSchedule("*/15 * * * *", func() {
//	    fmt.Println("每 15 分钟检查")
//	})
//
//	// 每天上午 9 点执行
//	timer.AddCronSchedule("0 9 * * *", func() {
//	    fmt.Println("早上好！")
//	})
func AddCronSchedule(expr string, fn func(), async ...bool) uint64 {
	return GetDefaultActor().AddCronSchedule(expr, fn, async...)
}

// AddScheduleOnce 根据固定日期调度器调度一个执行一次的任务。
// 这是使用默认 Actor 的便捷函数。
// 如果尚未设置默认 Actor，会自动创建一个 DefaultTimerActor。
//
// 参数：
//   - schedule: 调度配置
//   - fn: 要执行的回调函数
//   - async: 是否异步执行回调（可选，默认为 false）
//
// 返回可用于删除定时器的定时器 ID。
func AddScheduleOnce(schedule *FixedDateSchedule, fn func(), async ...bool) uint64 {
	return GetDefaultActor().AddScheduleOnce(schedule, fn, async...)
}

// Once 调度一个在指定延迟后执行的一次性定时任务。
// 这是使用默认 Actor 的便捷函数。
// 如果尚未设置默认 Actor，会自动创建一个 DefaultTimerActor。
//
// 参数：
//   - delay: 执行任务前等待的时间
//   - fn: 要执行的回调函数
//   - async: 是否异步执行回调（可选，默认为 false）
//
// 返回可用于删除定时器的定时器 ID。
//
// 示例：
//
//	id := Once(5*time.Second, func() {
//	    fmt.Println("5秒后执行一次")
//	})
func Once(delay time.Duration, fn func(), async ...bool) uint64 {
	return GetDefaultActor().Add(delay, fn, async...)
}

// Remove 停止并删除具有指定 ID 的定时器。
// 这是使用默认 Actor 的便捷函数。
// 如果尚未设置默认 Actor，会自动创建一个 DefaultTimerActor。
//
// 如果定时器已经触发或不存在，此操作无效。
//
// 示例：
//
//	id := Add(10*time.Second, func() {
//	    fmt.Println("不会执行")
//	})
//	Remove(id) // 取消定时器
func Remove(id uint64) {
	GetDefaultActor().Remove(id)
}
