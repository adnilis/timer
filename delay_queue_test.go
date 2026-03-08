package timer

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewDelayQueue(t *testing.T) {
	size := 100
	dq := NewDelayQueue(size)

	if dq == nil {
		t.Fatal("NewDelayQueue() returned nil")
	}
	if dq.C == nil {
		t.Fatal("DelayQueue channel is nil")
	}
	if cap(dq.C) == 0 {
		t.Error("DelayQueue channel should be buffered")
	}
	if atomic.LoadInt32(&dq.sleeping) != 0 {
		t.Error("sleeping state should be 0 initially")
	}
}

func TestDelayQueueOffer(t *testing.T) {
	dq := NewDelayQueue(10)

	done := make(chan struct{})
	go func() {
		dq.Poll(make(chan struct{}), func() int64 { return TimeToMS(time.Now().UTC()) })
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	// Offer items with different expirations
	now := TimeToMS(time.Now().UTC())
	dq.Offer("item1", now+100)
	dq.Offer("item2", now+200)
	dq.Offer("item3", now+50)

	// Receive items in order
	item1 := <-dq.C
	item2 := <-dq.C
	item3 := <-dq.C

	if item1.(string) != "item3" {
		t.Errorf("expected item3 first (earliest), got %s", item1.(string))
	}
	if item2.(string) != "item1" {
		t.Errorf("expected item1 second, got %s", item2.(string))
	}
	if item3.(string) != "item2" {
		t.Errorf("expected item2 third, got %s", item3.(string))
	}
}

func TestDelayQueueStop(t *testing.T) {
	dq := NewDelayQueue(10)

	exitC := make(chan struct{})
	done := make(chan struct{})

	go func() {
		dq.Poll(exitC, func() int64 { return TimeToMS(time.Now().UTC()) })
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	// Stop the Poll goroutine
	close(exitC)

	// Wait for Poll to exit
	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Poll did not exit in time")
	}
}

func TestDelayQueueOfferWakeup(t *testing.T) {
	dq := NewDelayQueue(10)
	exitC := make(chan struct{})
	done := make(chan struct{})

	now := TimeToMS(time.Now().UTC())

	go func() {
		dq.Poll(exitC, func() int64 { return TimeToMS(time.Now().UTC()) })
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	// Offer an item with long delay - should sleep
	longDelayItem := "long-delay"
	dq.Offer(longDelayItem, now+5000)

	// Wait a bit then offer an item with short delay - should wakeup
	time.Sleep(100 * time.Millisecond)
	shortDelayItem := "short-delay"
	dq.Offer(shortDelayItem, now+50)

	// Should receive the short delay item first
	item := <-dq.C
	if item.(string) != shortDelayItem {
		t.Errorf("expected short-delay item, got %s", item.(string))
	}

	close(exitC)
	<-done
}

func TestDelayQueueConcurrentOffer(t *testing.T) {
	dq := NewDelayQueue(1000)
	exitC := make(chan struct{})
	done := make(chan struct{})

	go func() {
		dq.Poll(exitC, func() int64 { return TimeToMS(time.Now().UTC()) })
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	numGoroutines := 100
	itemsPerGoroutine := 10
	var wg sync.WaitGroup
	now := TimeToMS(time.Now().UTC())

	// Concurrent offers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < itemsPerGoroutine; j++ {
				delay := now + int64((i*itemsPerGoroutine+j)*10)
				dq.Offer(i*itemsPerGoroutine+j, delay)
			}
		}(i)
	}

	wg.Wait()

	// Wait for all items to be processed
	receivedCount := 0
	// Increase timeout to 15 seconds to account for items with delays up to 10 seconds
	timeout := time.After(15 * time.Second)
	for {
		select {
		case <-dq.C:
			receivedCount++
			if receivedCount == numGoroutines*itemsPerGoroutine {
				close(exitC)
				<-done
				return
			}
		case <-timeout:
			t.Fatalf("timeout waiting for items, received %d/%d",
				receivedCount, numGoroutines*itemsPerGoroutine)
		}
	}
}

func TestDelayQueueEmpty(t *testing.T) {
	dq := NewDelayQueue(10)
	exitC := make(chan struct{})
	done := make(chan struct{})

	go func() {
		dq.Poll(exitC, func() int64 { return TimeToMS(time.Now().UTC()) })
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	// No items offered, should be sleeping
	if atomic.LoadInt32(&dq.sleeping) != 1 {
		t.Error("queue should be sleeping when empty")
	}

	// Offer an item to wake it up
	now := TimeToMS(time.Now().UTC())
	dq.Offer("test", now+50)

	// Should wakeup and process
	timeout := time.After(1 * time.Second)
	select {
	case <-dq.C:
		// Success
		close(exitC)
		<-done
	case <-timeout:
		t.Fatal("timeout waiting for item")
	}
}

func BenchmarkDelayQueueOffer(b *testing.B) {
	dq := NewDelayQueue(b.N)
	now := TimeToMS(time.Now().UTC())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dq.Offer(i, now+int64(i))
	}
}

func BenchmarkDelayQueueOfferAndPoll(b *testing.B) {
	dq := NewDelayQueue(b.N)
	exitC := make(chan struct{})
	defer close(exitC)

	go func() {
		now := TimeToMS(time.Now().UTC())
		for i := 0; i < b.N; i++ {
			dq.Offer(i, now+int64(i))
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		<-dq.C
	}
}

func BenchmarkDelayQueueConcurrentOffer(b *testing.B) {
	dq := NewDelayQueue(b.N)
	now := TimeToMS(time.Now().UTC())

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dq.Offer(i, now+int64(i))
		}(i)
	}
	wg.Wait()
}
