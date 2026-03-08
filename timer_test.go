package timer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTimerID(t *testing.T) {
	t1 := &Timer{id: 12345}
	if t1.ID() != 12345 {
		t.Errorf("expected ID 12345, got %d", t1.ID())
	}
}

func TestTimerSetBucket(t *testing.T) {
	t1 := &Timer{}
	b := newBucket()

	t1.setBucket(b)
	if t1.getBucket() != b {
		t.Error("failed to set bucket")
	}

	t1.setBucket(nil)
	if t1.getBucket() != nil {
		t.Error("failed to set nil bucket")
	}
}

func TestTimerStop(t *testing.T) {
	b := newBucket()
	t1 := &Timer{id: 1, expiration: 1000}

	b.Add(t1)

	// Stop should remove timer from bucket
	if !t1.Stop() {
		t.Error("Stop() returned false for active timer")
	}

	if t1.getBucket() != nil {
		t.Error("timer bucket should be nil after Stop()")
	}

	// Stop again should return false
	if t1.Stop() {
		t.Error("Stop() should return false for already stopped timer")
	}
}

func TestTimerStopConcurrent(t *testing.T) {
	buckets := make([]*bucket, 10)
	for i := range buckets {
		buckets[i] = newBucket()
	}

	t1 := &Timer{id: 1, expiration: 1000}
	buckets[0].Add(t1)

	var wg sync.WaitGroup
	stopSuccessCount := int32(0)

	// Concurrent Stop calls
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if t1.Stop() {
				atomic.AddInt32(&stopSuccessCount, 1)
			}
		}()
	}

	wg.Wait()

	// Only one Stop should succeed
	successCount := atomic.LoadInt32(&stopSuccessCount)
	if successCount != 1 {
		t.Errorf("expected exactly 1 successful Stop, got %d", successCount)
	}

	if t1.getBucket() != nil {
		t.Error("timer bucket should be nil after concurrent Stop()")
	}
}

func TestTimerStopInProgress(t *testing.T) {
	b1 := newBucket()
	b2 := newBucket()
	t1 := &Timer{id: 1, expiration: 1000}

	b1.Add(t1)

	// Simulate Flush moving timer to another bucket
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		b1.Remove(t1)
		b2.Add(t1)
	}()

	go func() {
		defer wg.Done()
		t1.Stop()
	}()

	wg.Wait()

	// Timer should either be in nil (removed by Stop) or in b2
	b := t1.getBucket()
	if b != nil && b != b2 {
		t.Error("timer should be in nil or b2 bucket")
	}
}

func TestTimerCreation(t *testing.T) {
	taskCalled := false
	task := func() {
		taskCalled = true
		_ = taskCalled // Use the variable
	}

	t1 := &Timer{
		id:         NextId(),
		expiration: TimeToMS(time.Now().UTC()) + 1000,
		task:       task,
		isAsync:    true,
	}

	if t1.id == 0 {
		t.Error("timer ID should not be 0")
	}
	if t1.expiration == 0 {
		t.Error("timer expiration should not be 0")
	}
	if t1.task == nil {
		t.Error("timer task should not be nil")
	}
	if !t1.isAsync {
		t.Error("timer should be marked as async")
	}
}

func BenchmarkTimerStop(b *testing.B) {
	bucket := newBucket()
	timers := make([]*Timer, b.N)

	for i := 0; i < b.N; i++ {
		timers[i] = &Timer{id: uint64(i), expiration: int64(i)}
		bucket.Add(timers[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timers[i].Stop()
	}
}

func BenchmarkTimerConcurrentStop(b *testing.B) {
	bucket := newBucket()
	t1 := &Timer{id: 1, expiration: 1000}
	bucket.Add(t1)

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t1.Stop()
		}()
	}
	wg.Wait()
}
