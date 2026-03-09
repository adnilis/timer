package timer

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/adnilis/logger"
)

// TimeWheel 是分层时间轮（Hierarchical Timing Wheels）的实现。
//
// 分层时间轮是一种高效的时间轮算法，通过多级时间轮来处理不同时间范围的定时任务。
// 每一级时间轮的 tick 间隔是下一级的 wheelSize 倍，这样可以覆盖更大的时间范围。
//
// 工作原理：
//   - 时间轮是一个环形数组，每个槽位（bucket）存储在该时间点过期的定时器
//   - 指针按固定间隔（tick）移动，处理过期槽位中的定时器
//   - 当定时器超出当前时间轮范围时，放入上层时间轮（overflowWheel）
//   - 当上层时间轮的定时器到期时，会重新插入到下层时间轮
//
// 时间复杂度：
//   - 添加定时器：O(1)
//   - 删除定时器：O(1)
//   - 触发定时器：O(1) 平均
//
// 空间复杂度：O(n)，n 为定时器数量
type TimeWheel struct {
	tick          int64            // 时间轮的刻度间隔（毫秒），指针每次移动的时间单位
	wheelSize     int64            // 时间轮的大小（槽位数），即环形数组的长度
	interval      int64            // 时间轮的总时间跨度（毫秒），等于 tick * wheelSize
	currentTime   int64            // 当前时间（毫秒），时间轮指针指向的时间点
	buckets       []*bucket        // 桶列表，每个桶存储在该时间点过期的定时器
	queue         *DelayQueue      // 延迟队列，用于按过期时间顺序处理桶
	overflowWheel unsafe.Pointer   // 上层时间轮指针，类型为 *TimeWheel，用于处理超出当前时间轮范围的定时器
	exitC         chan struct{}    // 退出信号通道，用于停止时间轮的运行
	waitGroup     waitGroupWrapper // 等待组包装器，用于管理后台 goroutine 的生命周期
}

var (
	ErrInvalidTick      = errors.New("tick must be greater than or equal to 1ms") // tick 必须大于等于 1 毫秒
	ErrInvalidWheelSize = errors.New("wheelSize must be greater than 0")          // wheelSize 必须大于 0
)

// NewTimeWheel 创建一个具有指定 tick 和 wheelSize 的 TimeWheel 实例。
//
// 参数：
//   - tick: 时间轮的刻度间隔，决定了时间轮的精度。例如 10ms 表示每 10 毫秒移动一次指针
//   - wheelSize: 时间轮的大小（槽位数），决定了时间轮的时间跨度。例如 60 表示有 60 个槽位
//
// 返回：
//   - *TimeWheel: 创建的时间轮实例
//   - error: 如果参数无效则返回错误
//
// 示例：
//
//	tw, err := NewTimeWheel(10*time.Millisecond, 60)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	// 创建了一个精度为 10ms、跨度为 600ms 的时间轮
func NewTimeWheel(tick time.Duration, wheelSize int64) (*TimeWheel, error) {
	// 将 tick 转换为毫秒
	tickMs := int64(tick / time.Millisecond)
	if tickMs <= 0 {
		return nil, ErrInvalidTick
	}

	// 验证 wheelSize 必须大于 0
	if wheelSize <= 0 {
		return nil, ErrInvalidWheelSize
	}

	// 使用当前 UTC 时间作为起始时间
	startMs := TimeToMS(time.Now().UTC())

	// 调用内部构造函数创建时间轮
	return newTimingWheel(
		tickMs,
		wheelSize,
		startMs,
		NewDelayQueue(int(wheelSize)),
	), nil
}

// newTimingWheel 是内部辅助函数，真正创建 TimeWheel 实例。
//
// 参数：
//   - tickMs: 时间轮的刻度间隔（毫秒）
//   - wheelSize: 时间轮的大小（槽位数）
//   - startMs: 起始时间（毫秒）
//   - queue: 延迟队列实例，用于管理桶的过期顺序
//
// 返回：
//   - *TimeWheel: 创建的时间轮实例
func newTimingWheel(tickMs int64, wheelSize int64, startMs int64, queue *DelayQueue) *TimeWheel {
	// 创建指定数量的桶
	buckets := make([]*bucket, wheelSize)
	for i := range buckets {
		buckets[i] = newBucket()
	}

	return &TimeWheel{
		tick:        tickMs,
		wheelSize:   wheelSize,
		currentTime: truncate(startMs, tickMs), // 将起始时间截断到 tick 的整数倍
		interval:    tickMs * wheelSize,        // 计算时间轮的总时间跨度
		buckets:     buckets,
		queue:       queue,
		exitC:       make(chan struct{}),
	}
}

// add 将定时器 t 插入到当前时间轮中。
//
// 参数：
//   - t: 要添加的定时器
//
// 返回：
//   - bool: 如果定时器已过期返回 false，否则返回 true
//
// 工作流程：
//  1. 检查定时器是否已过期（在当前时间 + tick 之前）
//  2. 如果在当前时间轮范围内，计算对应的桶并添加
//  3. 如果超出当前时间轮范围，创建或使用上层时间轮
//
// 注意：此方法是线程安全的，使用原子操作读取 currentTime
func (tw *TimeWheel) add(t *Timer) bool {
	// 原子加载当前时间
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if t.expiration < currentTime+tw.tick {
		// 定时器已过期
		return false
	}

	if t.expiration < currentTime+tw.interval {
		// 定时器在当前时间轮范围内，放入对应的桶
		virtualID := t.expiration / tw.tick
		b := tw.buckets[virtualID%tw.wheelSize]
		b.Add(t)

		// 设置桶的过期时间
		if b.SetExpiration(virtualID * tw.tick) {
			// 桶需要入队，因为它是一个已过期的桶
			// 只有当桶的过期时间发生变化时才需要入队
			// 即时间轮前进后，该桶被重新使用并设置了新的过期时间
			// 在同一个时间轮周期内，后续设置过期时间的调用会传入相同的值
			// 因此返回 false，相同过期的桶不会被多次入队
			tw.queue.Offer(b, b.Expiration())
		}
		return true
	} else {
		// 定时器超出当前时间轮范围，放入上层时间轮
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel == nil {
			// 使用 CAS 操作创建上层时间轮
			atomic.CompareAndSwapPointer(
				&tw.overflowWheel,
				nil,
				unsafe.Pointer(newTimingWheel(
					tw.interval,  // 上层时间轮的 tick 等于当前时间轮的 interval
					tw.wheelSize, // 上层时间轮的 wheelSize 与当前相同
					currentTime,  // 使用当前时间作为起始时间
					tw.queue,     // 共享同一个延迟队列
				)),
			)
			overflowWheel = atomic.LoadPointer(&tw.overflowWheel)
		}

		// 递归调用上层时间轮的 add 方法
		return (*TimeWheel)(overflowWheel).add(t)
	}
}

// addOrRun 将定时器 t 插入到当前时间轮中，如果定时器已过期则直接执行其任务。
//
// 参数：
//   - t: 要添加或执行的定时器
//
// 工作流程：
//  1. 尝试将定时器添加到时间轮
//  2. 如果添加失败（定时器已过期），则执行定时器的任务
//  3. 根据 isAsync 标志决定是否在独立的 goroutine 中执行任务
//
// 注意：
//   - 与标准库 time.AfterFunc 类似，异步任务总是在独立的 goroutine 中执行
//   - 同步任务在当前 goroutine 中执行，可能阻塞调用者
func (tw *TimeWheel) addOrRun(t *Timer) {
	if !tw.add(t) {
		// 定时器已过期，执行任务
		// 与标准库 time.AfterFunc (https://golang.org/pkg/time/#AfterFunc) 类似，
		// 总是在独立的 goroutine 中执行定时器的任务
		if t.isAsync {
			go t.task()
		} else {
			t.task()
		}
	}
}

// advanceClock 推进时间轮的时钟到指定的过期时间。
//
// 参数：
//   - expiration: 要推进到的过期时间（毫秒）
//
// 工作流程：
//  1. 检查是否需要推进时钟（至少推进一个 tick）
//  2. 将过期时间截断到 tick 的整数倍
//  3. 原子更新当前时间
//  4. 如果存在上层时间轮，也推进其时钟
//
// 注意：
//   - 此方法是线程安全的，使用原子操作更新 currentTime
//   - 时钟只能向前推进，不能后退
//   - 上层时间轮的时钟也会同步推进，保持时间一致性
func (tw *TimeWheel) advanceClock(expiration int64) {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if expiration >= currentTime+tw.tick {
		// 将过期时间截断到 tick 的整数倍
		currentTime = truncate(expiration, tw.tick)
		// 原子更新当前时间
		atomic.StoreInt64(&tw.currentTime, currentTime)

		// 如果存在上层时间轮，也推进其时钟
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel != nil {
			(*TimeWheel)(overflowWheel).advanceClock(currentTime)
		}
	}
}

// Start 启动当前时间轮。
//
// 参数：
//   - ctx: 上下文，用于控制时间轮的生命周期
//
// 工作流程：
//  1. 启动延迟队列的轮询 goroutine，定期检查过期的桶
//  2. 启动事件处理 goroutine，处理过期的桶并执行定时器任务
//
// 注意：
//   - 此方法会启动两个后台 goroutine
//   - 调用 Stop() 可以停止时间轮
//   - 上下文取消也会导致时间轮停止
func (tw *TimeWheel) Start(ctx context.Context) {
	// 启动延迟队列的轮询 goroutine
	tw.waitGroup.Wrap(func() {
		tw.queue.Poll(tw.exitC, func() int64 {
			return TimeToMS(time.Now().UTC())
		})
	})

	// 启动事件处理 goroutine
	tw.waitGroup.Wrap(func() {
		for {
			select {
			case elem := <-tw.queue.C:
				// 处理过期的桶
				b := elem.(*bucket)
				tw.advanceClock(b.Expiration())
				b.Flush(tw.addOrRun)
			case <-tw.exitC:
				// 收到退出信号
				return
			case <-ctx.Done():
				// 上下文被取消
				return
			}
		}
	})
}

// Stop 停止当前时间轮。
//
// 工作流程：
//  1. 关闭退出信号通道
//  2. 等待所有后台 goroutine 退出
//
// 注意：
//   - 如果有定时器的任务正在独立的 goroutine 中运行，Stop 不会等待任务完成
//   - 如果调用者需要知道任务是否完成，必须与任务显式协调
//   - Stop 是阻塞的，会等待所有后台 goroutine 退出后才返回
//   - 多次调用 Stop 会导致 panic（因为 exitC 会被关闭多次）
func (tw *TimeWheel) Stop() {
	close(tw.exitC)
	tw.waitGroup.Wait()
}

// AfterFunc 等待指定的持续时间后，在独立的 goroutine 中调用函数 f。
//
// 参数：
//   - id: 定时器的唯一标识符
//   - d: 等待的持续时间
//   - f: 要执行的函数
//   - async: 可选参数，指定是否异步执行（默认为 true）
//
// 返回：
//   - *Timer: 定时器对象，可以使用其 Stop 方法取消调用
//
// 示例：
//
//	timer := tw.AfterFunc(1, 5*time.Second, func() {
//	    fmt.Println("5秒后执行")
//	})
//	// 取消定时器
//	timer.Stop()
func (tw *TimeWheel) AfterFunc(id uint64, d time.Duration, f func(), async ...bool) *Timer {
	t := &Timer{
		id:         id,
		expiration: TimeToMS(time.Now().UTC().Add(d)), // 计算过期时间
		task:       f,
		isAsync:    getAsyncValue(async...),
	}
	tw.addOrRun(t)

	return t
}

// AddEveryFunc 每隔指定的持续时间调用一次函数 f。
//
// 参数：
//   - id: 定时器的唯一标识符
//   - d: 调用间隔
//   - f: 要执行的函数
//   - async: 可选参数，指定是否异步执行（默认为 true）
//
// 返回：
//   - *Timer: 定时器对象，可以使用其 Stop 方法停止周期性调用
//
// 示例：
//
//	timer := tw.AddEveryFunc(1, 1*time.Second, func() {
//	    fmt.Println("每秒执行一次")
//	})
//	// 5秒后停止
//	time.Sleep(5 * time.Second)
//	timer.Stop()
func (tw *TimeWheel) AddEveryFunc(id uint64, d time.Duration, f func(), async ...bool) *Timer {
	return tw.ScheduleFunc(id, &EverySchedule{Interval: d}, f, async...)
}

// BuildAfterFunc 创建一个在指定持续时间后执行的定时器，自动生成 ID。
//
// 参数：
//   - d: 等待的持续时间
//   - f: 要执行的函数
//
// 返回：
//   - *Timer: 定时器对象
//
// 注意：
//   - 此方法会自动生成唯一的定时器 ID
//   - 默认在独立的 goroutine 中执行函数
func (tw *TimeWheel) BuildAfterFunc(d time.Duration, f func()) *Timer {
	id := NextId()
	return tw.AfterFunc(id, d, f)
}

// BuildEveryFunc 创建一个每隔指定持续时间执行的定时器，自动生成 ID。
//
// 参数：
//   - d: 调用间隔
//   - f: 要执行的函数
//   - async: 可选参数，指定是否异步执行（默认为 true）
//
// 返回：
//   - *Timer: 定时器对象
//
// 注意：
//   - 此方法会自动生成唯一的定时器 ID
//   - 默认在独立的 goroutine 中执行函数
func (tw *TimeWheel) BuildEveryFunc(d time.Duration, f func(), async ...bool) *Timer {
	id := NextId()
	return tw.AddEveryFunc(id, d, f, async...)
}

// ScheduleFunc 根据调度器 s 的执行计划调用函数 f（在独立的 goroutine 中）。
//
// 参数：
//   - id: 定时器的唯一标识符
//   - s: 调度器接口，用于计算下一次执行时间
//   - f: 要执行的函数
//   - async: 可选参数，指定是否异步执行（默认为 true）
//
// 返回：
//   - *Timer: 定时器对象，可以使用其 Stop 方法取消调用
//
// 工作原理：
//  1. 初始时调用 s.Next() 获取第一次执行时间
//  2. 如果执行时间非零，创建定时器
//  3. 每次执行 f 之前，调用 s.Next() 获取下一次执行时间
//  4. 如果下一次执行时间非零，重新调度定时器
//
// 注意事项：
//   - 如果调用者想要中途终止执行计划，必须停止定时器并确保定时器实际已停止
//   - 在当前实现中，定时器过期和重新启动之间存在一个时间间隙
//   - 确保定时器停止的等待时间很短，因为间隙非常小
//   - 使用 UTC 时间进行调度，避免时区问题
//   - 函数 f 中的 panic 会被捕获并记录，不会导致程序崩溃
//
// 示例：
//
//	// 使用 Cron 表达式调度
//	timer := tw.ScheduleFunc(1, &CronSchedule{Cron: "0 */5 * * * *"}, func() {
//	    fmt.Println("每5分钟执行一次")
//	})
//
//	// 使用自定义调度器
//	type MyScheduler struct{}
//	func (s *MyScheduler) Next(t time.Time) time.Time {
//	    return t.Add(1 * time.Hour)
//	}
//	timer := tw.ScheduleFunc(2, &MyScheduler{}, func() {
//	    fmt.Println("每小时执行一次")
//	})
func (tw *TimeWheel) ScheduleFunc(id uint64, s Scheduler, f func(), async ...bool) *Timer {
	// 使用 UTC 时间进行调度，避免时区问题
	expiration := s.Next(time.Now().UTC())
	if expiration.IsZero() {
		// 没有调度时间，返回 nil
		return nil
	}

	t := &Timer{
		id:         id,
		expiration: TimeToMS(expiration),
		isAsync:    getAsyncValue(async...),
	}

	// 定义定时器的任务
	t.task = func() {
		// 如果可能，调度任务在下一个时间执行
		nextExpiration := s.Next(MSToTime(t.expiration))
		if !nextExpiration.IsZero() {
			t.expiration = TimeToMS(nextExpiration)
			tw.addOrRun(t)
		}

		// 捕获 panic 并记录，防止程序崩溃
		defer func() {
			logger.Panic(recover())
		}()

		// 执行用户函数
		f()
	}

	tw.addOrRun(t)
	return t
}

// NextId 生成下一个唯一的定时器 ID。
//
// 返回：
//   - uint64: 唯一的定时器 ID
//
// 注意：
//   - 此方法调用全局的 NextId() 函数
//   - ID 是原子递增的，保证全局唯一性
func (tw *TimeWheel) NextId() uint64 {
	return NextId()
}

// Remove 删除具有指定 id 的定时器。
//
// 参数：
//   - id: 要删除的定时器的唯一标识符
//
// 返回：
//   - *Timer: 如果定时器存在且成功停止，返回 Timer 对象；否则返回 nil
//
// 注意：
//   - 此方法需要调用者维护一个 id 到 Timer 的映射关系
//   - TimeWheel 内部没有存储所有活跃定时器的映射
//   - 如果需要在 Remove 后获取 Timer 对象，建议调用方自行维护映射
//   - 当前实现返回 nil，调用者需要自行维护映射并调用 Timer.Stop()
//
// 使用示例：
//
//	// 维护定时器映射
//	timers := make(map[uint64]*Timer)
//	timerID := tw.AfterFunc(NextId(), delay, task)
//	timers[timerID] = timer
//	// ...
//	// 删除定时器
//	if timer, ok := timers[id]; ok {
//		timer.Stop()
//		delete(timers, id)
//	}
func (tw *TimeWheel) Remove(id uint64) *Timer {
	// TimeWheel 内部没有维护 id 到 Timer 的映射，
	// 因此无法根据 id 直接找到对应的 Timer。
	// 调用者需要自行维护 id 到 Timer 的映射，并调用 Timer.Stop() 方法。
	// 这是一个空实现，返回 nil 表示无法直接移除。
	return nil
}

// getAsyncValue 获取异步执行标志的值。
//
// 参数：
//   - asyncTask: 可变参数，包含异步执行标志
//
// 返回：
//   - bool: 如果提供了参数则返回第一个参数的值，否则返回 false
//
// 注意：
//   - 这是一个辅助函数，用于处理可选的 async 参数
//   - 默认情况下（不提供参数），返回 false 表示同步执行
func getAsyncValue(asyncTask ...bool) bool {
	if len(asyncTask) > 0 {
		return asyncTask[0]
	}
	return false
}
