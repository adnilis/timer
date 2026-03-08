package timer

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

// ==================== 优先队列实现开始 ====================

// item 表示优先队列中的一个元素
type item struct {
	Value    interface{} // 元素值
	Priority int64       // 优先级（过期时间，毫秒）
	Index    int         // 在堆中的索引
}

// priorityQueue 是通过最小堆实现的优先队列
// 即第 0 个元素是*最小*值（最早过期的元素）
type priorityQueue []*item

// newPriorityQueue 创建一个具有指定容量优先队列
func newPriorityQueue(capacity int) priorityQueue {
	return make(priorityQueue, 0, capacity)
}

// Len 返回优先队列的长度
func (pq priorityQueue) Len() int {
	return len(pq)
}

// Less 比较两个元素的优先级，返回 i 是否比 j 优先级更高
func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority // 使用最小堆，优先级越小越优先
}

// Swap 交换两个元素的位置
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

// Push 将元素推入优先队列
func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		// 容量不足，扩容为原来的 2 倍
		newCap := c * 2
		if newCap == 0 {
			newCap = 1
		}
		npq := make(priorityQueue, n, newCap)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	item := x.(*item)
	item.Index = n
	(*pq)[n] = item
}

// Pop 从优先队列中弹出元素
func (pq *priorityQueue) Pop() interface{} {
	n := len(*pq)
	c := cap(*pq)
	// 只有当元素数量小于容量的 1/4 且容量大于 25 时才缩容
	// 避免频繁扩容缩容
	if n < (c/4) && c > 25 {
		newCap := c / 2
		npq := make(priorityQueue, n, newCap)
		copy(npq, *pq)
		*pq = npq
	}
	item := (*pq)[n-1]
	item.Index = -1
	*pq = (*pq)[0 : n-1]

	return item
}

// PeekAndShift 查看并移除优先级最高（最早过期）的元素
// 参数：
//   - max: 当前时间（毫秒）
//
// 返回值：
//   - *item: 已过期的元素，如果没有元素过期则为 nil
//   - int64: 距离元素过期还需等待的毫秒数，如果元素已过期则为 0
func (pq *priorityQueue) PeekAndShift(max int64) (*item, int64) {
	if pq.Len() == 0 {
		return nil, 0
	}

	item := (*pq)[0]
	if item.Priority > max {
		// 元素还未过期，返回需要等待的时间
		return nil, item.Priority - max
	}
	// 元素已过期，从堆中移除
	heap.Remove(pq, 0)

	return item, 0
}

// ==================== 优先队列实现结束 ====================

// DelayQueue 是一个无界阻塞队列，其中的元素只有在延迟过期后才能被取出。
// 队列头是延迟过期时间最早的元素。
type DelayQueue struct {
	C        chan interface{} // 过期元素输出的通道
	mu       sync.Mutex       // 互斥锁，保证并发安全
	pq       priorityQueue    // 优先队列
	sleeping int32            // 休眠状态，类似于 runtime.timers 的休眠状态
	wakeupC  chan struct{}    // 唤醒通道
}

// NewDelayQueue 创建一个指定大小的延迟队列实例
func NewDelayQueue(size int) *DelayQueue {
	return &DelayQueue{
		C:       make(chan interface{}, size),
		pq:      newPriorityQueue(size),
		wakeupC: make(chan struct{}),
	}
}

// Offer 将元素插入延迟队列
// 参数：
//   - elem: 要插入的元素
//   - expiration: 过期时间（毫秒）
func (dq *DelayQueue) Offer(elem interface{}, expiration int64) {
	item := &item{Value: elem, Priority: expiration}

	dq.mu.Lock()
	heap.Push(&dq.pq, item)
	index := item.Index
	dq.mu.Unlock()

	if index == 0 {
		// 添加了一个具有最早过期时间的新元素
		if atomic.CompareAndSwapInt32(&dq.sleeping, 1, 0) {
			dq.wakeupC <- struct{}{}
		}
	}
}

// Poll 启动一个无限循环，持续等待元素过期，然后将过期元素发送到通道 C
// 参数：
//   - exitC: 退出信号通道
//   - nowF: 获取当前时间的函数（毫秒）
func (dq *DelayQueue) Poll(exitC chan struct{}, nowF func() int64) {
	for {
		now := nowF()

		dq.mu.Lock()
		item, delta := dq.pq.PeekAndShift(now)
		if item == nil {
			// 没有元素剩余或至少有一个元素待处理

			// 必须确保整个操作的原子性，该操作由上述 PeekAndShift
			// 和后续的 StoreInt32 组成，以避免 Offer 和 Poll 之间可能出现的竞争条件。
			atomic.StoreInt32(&dq.sleeping, 1)
		}
		dq.mu.Unlock()

		if item == nil {
			if delta == 0 {
				// 没有元素剩余
				select {
				case <-dq.wakeupC:
					// 等待新元素添加
					continue
				case <-exitC:
					goto exit
				}
			} else if delta > 0 {
				// 至少有一个元素待处理
				select {
				case <-dq.wakeupC:
					// 添加了一个比当前“最早”元素“更早”过期的新元素
					continue
				case <-time.After(time.Duration(delta) * time.Millisecond):
					// 当前的“最早”元素已过期

					// 重置休眠状态，因为不需要从 wakeupC 接收
					if atomic.SwapInt32(&dq.sleeping, 0) == 0 {
						// Offer() 的调用者被阻塞在向 wakeupC 发送时，
						// 清空 wakeupC 以解除调用者的阻塞
						<-dq.wakeupC
					}
					continue
				case <-exitC:
					goto exit
				}
			}
		}

		select {
		case dq.C <- item.Value:
			// 过期元素已成功发送
		case <-exitC:
			goto exit
		}
	}

exit:
	// 重置状态
	atomic.StoreInt32(&dq.sleeping, 0)
}
