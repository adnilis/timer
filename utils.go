package timer

import (
	"sync"
	"sync/atomic"
	"time"
)

// truncate 返回将 x 向零舍入到 m 的倍数的结果。
// 如果 m <= 0，Truncate 返回 x 不变。
//
// 此函数用于时间轮计算，将时间对齐到指定的时间间隔。
func truncate(x, m int64) int64 {
	if m <= 0 {
		return x
	}
	return x - x%m
}

// TimeToMS 将时间转换为毫秒级整数表示。
//
// 返回自 1970 年 1 月 1 日 UTC 以来的毫秒数。
// 这是时间轮内部使用的标准时间格式。
func TimeToMS(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

// MSToTime 将毫秒级整数转换回 UTC 时间。
//
// 参数 t 是自 1970 年 1 月 1 日 UTC 以来的毫秒数。
// 返回对应的 time.Time 对象。
func MSToTime(t int64) time.Time {
	return time.Unix(0, t*int64(time.Millisecond))
}

// waitGroupWrapper 封装 sync.WaitGroup，提供便捷的 goroutine 管理方法。
//
// Wrap 方法自动处理 WaitGroup 的 Add 和 Done 调用，
// 使得在 goroutine 中执行回调函数更加简洁。
type waitGroupWrapper struct {
	sync.WaitGroup
}

// Wrap 在新的 goroutine 中执行回调函数，并自动管理 WaitGroup。
//
// 此方法会：
//  1. 调用 Add(1) 增加 WaitGroup 计数
//  2. 在新的 goroutine 中执行回调函数 cb
//  3. 回调完成后调用 Done() 减少计数
//
// 使用示例：
//
//	var wg waitGroupWrapper
//	wg.Wrap(func() {
//		// 执行耗时操作
//	})
//	wg.Wait() // 等待所有 goroutine 完成
func (w *waitGroupWrapper) Wrap(cb func()) {
	w.Go(cb)
}

var _nextId uint64 // 全局定时器 ID 计数器

// NextId 生成并返回下一个唯一的定时器 ID。
//
// 使用原子操作保证并发安全，确保每个定时器都有唯一的标识符。
// 返回的值从 1 开始递增。
//
// 注意：在长时间运行的程序中，如果定时器创建非常频繁，
// ID 最终会溢出。但对于大多数应用场景，这不是问题。
func NextId() uint64 {
	return atomic.AddUint64(&_nextId, 1)
}
