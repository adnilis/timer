package timer

import (
	"container/list"
	"sync/atomic"
	"unsafe"
)

// Timer 表示单个定时器事件。当定时器过期时，指定的任务将被执行。
//
// Timer 使用 bucket 和 element 来管理定时器在不同时间轮层级间的移动。
// bucket 字段使用原子操作保证并发安全，element 是定时器在链表中的位置。
//
// 支持同步和异步两种任务执行模式。
type Timer struct {
	id         uint64 // 定时器唯一标识符
	expiration int64  // 过期时间（毫秒）
	task       func() // 到期时执行的任务
	// bucket 保存定时器元素所属的链表。
	//
	// 注意：此字段可能通过 Timer.Stop() 和 Bucket.Flush() 并发更新和读取。
	// 使用 atomic 操作保证并发安全。
	b       unsafe.Pointer // type: *bucket 指向定时器所在的 bucket
	element *list.Element  // 定时器在 bucket 链表中的元素节点
	isAsync bool           // 是否异步执行任务
}

func (t *Timer) ID() uint64 {
	return t.id
}

func (t *Timer) getBucket() *bucket {
	return (*bucket)(atomic.LoadPointer(&t.b))
}

func (t *Timer) setBucket(b *bucket) {
	atomic.StorePointer(&t.b, unsafe.Pointer(b))
}

// Stop 阻止定时器触发。如果调用成功停止了定时器，返回 true；
// 如果定时器已经过期或已被停止，返回 false。
//
// 如果定时器已经过期并且 t.task 已经在自己的 goroutine 中启动；
// Stop 不会等待 t.task 完成就返回。如果调用者需要知道 t.task 是否完成，
// 必须与 t.task 显式协调。
//
// 实现细节：
// 使用重试机制（最多 3 次）处理并发场景。在调用 Stop() 时，
// 时间轮的 goroutine 可能正在将定时器移动到另一个 bucket，
// 因此需要重新获取 bucket 并重试直到 bucket 为 nil。
func (t *Timer) Stop() bool {
	const maxRetries = 3
	retries := 0
	stopped := false

	for b := t.getBucket(); b != nil && retries < maxRetries; b = t.getBucket() {
		// 如果 b.Remove 在时间轮的 goroutine 刚刚执行以下操作后被调用：
		//     1. 从 b 中移除 t（通过 b.Flush -> b.remove）
		//     2. 将 t 从 b 移动到另一个 bucket ab（通过 b.Flush -> b.remove 和 ab.Add）
		// 由于 t 的 bucket 发生变化，此处可能无法移除 t。
		stopped = b.Remove(t)

		// 因此，这里重新获取 t 可能的新 bucket（情况 1 为 nil，情况 2 为 ab（非 nil）），
		// 并重试直到 bucket 变为 nil，这表示 t 最终已被移除。
		retries++
	}
	return stopped
}
