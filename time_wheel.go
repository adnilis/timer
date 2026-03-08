package timer

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/adnilis/logger"
)

// TimeWheel is an implementation of Hierarchical Timing Wheels.
type TimeWheel struct {
	tick          int64            // in milliseconds
	wheelSize     int64            // wheel size
	interval      int64            // in milliseconds
	currentTime   int64            // in milliseconds
	buckets       []*bucket        // bucket list
	queue         *DelayQueue      // delay queue
	overflowWheel unsafe.Pointer   // type: *TimeWheel The higher-level overflow wheel.
	exitC         chan struct{}    // exit chan
	waitGroup     waitGroupWrapper // wait group
}

var (
	ErrInvalidTick      = errors.New("tick must be greater than or equal to 1ms")
	ErrInvalidWheelSize = errors.New("wheelSize must be greater than 0")
)

// NewTimeWheel creates an instance of TimeWheel with the given tick and wheelSize.
// Returns an error if the parameters are invalid.
func NewTimeWheel(tick time.Duration, wheelSize int64) (*TimeWheel, error) {
	tickMs := int64(tick / time.Millisecond)
	if tickMs <= 0 {
		return nil, ErrInvalidTick
	}

	if wheelSize <= 0 {
		return nil, ErrInvalidWheelSize
	}

	startMs := TimeToMS(time.Now().UTC())

	return newTimingWheel(
		tickMs,
		wheelSize,
		startMs,
		NewDelayQueue(int(wheelSize)),
	), nil
}

// newTimingWheel is an internal helper function that really creates an instance of TimeWheel.
func newTimingWheel(tickMs int64, wheelSize int64, startMs int64, queue *DelayQueue) *TimeWheel {
	buckets := make([]*bucket, wheelSize)
	for i := range buckets {
		buckets[i] = newBucket()
	}

	return &TimeWheel{
		tick:        tickMs,
		wheelSize:   wheelSize,
		currentTime: truncate(startMs, tickMs),
		interval:    tickMs * wheelSize,
		buckets:     buckets,
		queue:       queue,
		exitC:       make(chan struct{}),
	}
}

// add inserts the timer t into the current timing wheel.
func (tw *TimeWheel) add(t *Timer) bool {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if t.expiration < currentTime+tw.tick {
		// Already expired
		return false
	}

	if t.expiration < currentTime+tw.interval {
		// Put it into its own bucket
		virtualID := t.expiration / tw.tick
		b := tw.buckets[virtualID%tw.wheelSize]
		b.Add(t)

		// Set the bucket expiration time
		if b.SetExpiration(virtualID * tw.tick) {
			// The bucket needs to be enqueued since it was an expired bucket.
			// We only need to enqueue the bucket when its expiration time has changed,
			// i.e. the wheel has advanced and this bucket get reused with a new expiration.
			// Any further calls to set the expiration within the same wheel cycle will
			// pass in the same value and hence return false, thus the bucket with the
			// same expiration will not be enqueued multiple times.
			tw.queue.Offer(b, b.Expiration())
		}
		return true
	} else {
		// Out of the interval. Put it into the overflow wheel
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel == nil {
			atomic.CompareAndSwapPointer(
				&tw.overflowWheel,
				nil,
				unsafe.Pointer(newTimingWheel(
					tw.interval,
					tw.wheelSize,
					currentTime,
					tw.queue,
				)),
			)
			overflowWheel = atomic.LoadPointer(&tw.overflowWheel)
		}

		return (*TimeWheel)(overflowWheel).add(t)
	}
}

// addOrRun inserts the timer t into the current timing wheel, or run the
// timer's task if it has already expired.
func (tw *TimeWheel) addOrRun(t *Timer) {
	if !tw.add(t) {
		// Already expired
		// Like the standard time.AfterFunc (https://golang.org/pkg/time/#AfterFunc),
		// always execute the timer's task in its own goroutine.
		if t.isAsync {
			go t.task()
		} else {
			t.task()
		}
	}
}

func (tw *TimeWheel) advanceClock(expiration int64) {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if expiration >= currentTime+tw.tick {
		currentTime = truncate(expiration, tw.tick)
		atomic.StoreInt64(&tw.currentTime, currentTime)

		// Try to advance the clock of the overflow wheel if present
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel != nil {
			(*TimeWheel)(overflowWheel).advanceClock(currentTime)
		}
	}
}

// Start starts the current timing wheel.
func (tw *TimeWheel) Start(ctx context.Context) {
	tw.waitGroup.Wrap(func() {
		tw.queue.Poll(tw.exitC, func() int64 {
			return TimeToMS(time.Now().UTC())
		})
	})

	tw.waitGroup.Wrap(func() {
		for {
			select {
			case elem := <-tw.queue.C:
				b := elem.(*bucket)
				tw.advanceClock(b.Expiration())
				b.Flush(tw.addOrRun)
			case <-tw.exitC:
				return
			case <-ctx.Done():
				return
			}
		}
	})
}

// Stop stops the current timing wheel.
//
// If there is any timer's task being running in its own goroutine, Stop does
// not wait for the task to complete before returning. If the caller needs to
// know whether the task is completed, it must coordinate with the task explicitly.
func (tw *TimeWheel) Stop() {
	close(tw.exitC)
	tw.waitGroup.Wait()
}

// AfterFunc waits for the duration to elapse and then calls f in its own goroutine.
// It returns a Timer that can be used to cancel the call using its Stop method.
func (tw *TimeWheel) AfterFunc(id uint64, d time.Duration, f func(), async ...bool) *Timer {
	t := &Timer{
		id:         id,
		expiration: TimeToMS(time.Now().UTC().Add(d)),
		task:       f,
		isAsync:    getAsyncValue(async...),
	}
	tw.addOrRun(t)

	return t
}

func (tw *TimeWheel) AddEveryFunc(id uint64, d time.Duration, f func(), async ...bool) *Timer {
	return tw.ScheduleFunc(id, &EverySchedule{Interval: d}, f, async...)
}

func (tw *TimeWheel) BuildAfterFunc(d time.Duration, f func()) *Timer {
	id := NextId()
	return tw.AfterFunc(id, d, f)
}

func (tw *TimeWheel) BuildEveryFunc(d time.Duration, f func(), async ...bool) *Timer {
	id := NextId()
	return tw.AddEveryFunc(id, d, f, async...)
}

// ScheduleFunc calls f (in its own goroutine) according to the execution
// plan scheduled by s. It returns a Timer that can be used to cancel the
// call using its Stop method.
//
// If the caller want to terminate the execution plan halfway, it must
// stop the timer and ensure that the timer is stopped actually, since in
// the current implementation, there is a gap between the expiring and the
// restarting of the timer. The wait time for ensuring is short since the
// gap is very small.
//
// Internally, ScheduleFunc will ask the first execution time (by calling
// s.Next()) initially, and create a timer if the execution time is non-zero.
// Afterwards, it will ask the next execution time each time f is about to
// be executed, and f will be called at the next execution time if the time
// is non-zero.
func (tw *TimeWheel) ScheduleFunc(id uint64, s Scheduler, f func(), async ...bool) *Timer {
	// 使用 UTC 时间进行调度
	expiration := s.Next(time.Now().UTC())
	if expiration.IsZero() {
		// No time is scheduled, return nil.
		return nil
	}

	t := &Timer{
		id:         id,
		expiration: TimeToMS(expiration),
		isAsync:    getAsyncValue(async...),
	}

	t.task = func() {
		// Schedule the task to execute at the next time if possible.
		nextExpiration := s.Next(MSToTime(t.expiration))
		if !nextExpiration.IsZero() {
			t.expiration = TimeToMS(nextExpiration)
			tw.addOrRun(t)
		}

		defer func() {
			logger.Panic(recover())
		}()

		f()
	}

	tw.addOrRun(t)
	return t
}

func (tw *TimeWheel) NextId() uint64 {
	return NextId()
}

// Remove 删除具有指定 id 的定时器。
// 如果定时器存在且成功停止，返回 *Timer 对象；否则返回 nil。
//
// 注意：此方法需要调用者维护一个 id 到 Timer 的映射关系，
// 因为 TimeWheel 内部没有存储所有活跃定时器的映射。
// 如果需要在 Remove 后获取 Timer 对象，建议调用方自行维护映射。
func (tw *TimeWheel) Remove(id uint64) *Timer {
	// TimeWheel 内部没有维护 id 到 Timer 的映射，
	// 因此无法根据 id 直接找到对应的 Timer。
	// 调用者需要自行维护 id 到 Timer 的映射，并调用 Timer.Stop() 方法。
	// 这是一个空实现，返回 nil 表示无法直接移除。
	// 使用示例：
	//
	//	timers := make(map[uint64]*Timer)
	//	timerID := tw.AfterFunc(NextId(), delay, task)
	//	timers[timerID] = timer
	//	// ...
	//	if timer, ok := timers[id]; ok {
	//		timer.Stop()
	//		delete(timers, id)
	//	}
	return nil
}

func getAsyncValue(asyncTask ...bool) bool {
	if len(asyncTask) > 0 {
		return asyncTask[0]
	}
	return false
}
