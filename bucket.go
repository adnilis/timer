package timer

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// bucket 表示一个时间轮桶，用于存储具有相似过期时间的定时器
type bucket struct {
	expiration int64      // 过期时间（毫秒）
	mu         sync.Mutex // 互斥锁，保证并发安全
	timers     *list.List // 定时器列表
}

// newBucket 创建一个新的空桶
func newBucket() *bucket {
	return &bucket{
		timers:     list.New(),
		expiration: -1, // 初始过期时间设为 -1，表示未设置
	}
}

// Expiration 返回桶的过期时间（毫秒）
func (b *bucket) Expiration() int64 {
	return atomic.LoadInt64(&b.expiration)
}

// SetExpiration 设置桶的过期时间，返回过期时间是否发生变化
func (b *bucket) SetExpiration(expiration int64) bool {
	return atomic.SwapInt64(&b.expiration, expiration) != expiration
}

// Add 将定时器添加到桶中
func (b *bucket) Add(t *Timer) {
	b.mu.Lock()

	e := b.timers.PushBack(t) // 将定时器添加到双链表末尾
	t.setBucket(b)            // 设置定时器所属的桶
	t.element = e             // 保存定时器在列表中的元素引用

	b.mu.Unlock()
}

// remove 从桶中移除定时器（不加锁版本）
// 返回值：true 表示成功移除，false 表示定时器不在当前桶中
func (b *bucket) remove(t *Timer) bool {
	if t.getBucket() != b {
		// 如果从 t.Stop 调用 remove，且在时间轮的 goroutine 刚刚执行了以下操作：
		//     1. 从 b 中移除了 t（通过 b.Flush -> b.remove）
		//     2. 将 t 从 b 移动到另一个桶 ab（通过 b.Flush -> b.remove 和 ab.Add）
		// 那么对于情况 1，t.getBucket 将返回 nil；对于情况 2，将返回 ab（非 nil）。
		// 无论哪种情况，返回值都不等于 b。
		return false
	}

	b.timers.Remove(t.element)
	t.setBucket(nil)
	t.element = nil
	return true
}

// Remove 从桶中移除定时器（加锁版本）
func (b *bucket) Remove(t *Timer) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remove(t)
}

// Flush 清空桶中的所有定时器，并通过 reinsert 函数重新插入它们
func (b *bucket) Flush(reinsert func(*Timer)) {
	// 先收集所有定时器，避免在持有锁的情况下调用 reinsert
	var timers []*Timer

	b.mu.Lock()
	e := b.timers.Front()
	for e != nil {
		next := e.Next()
		t := e.Value.(*Timer)
		b.remove(t) // 从桶中移除定时器
		timers = append(timers, t)
		e = next
	}
	b.mu.Unlock()

	b.SetExpiration(-1) // 重置过期时间

	// 在锁外重新插入定时器，避免死锁
	for _, t := range timers {
		reinsert(t)
	}
}
