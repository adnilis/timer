package timer

import (
	"sync"
	"testing"
)

func TestNewBucket(t *testing.T) {
	b := newBucket()
	if b == nil {
		t.Fatal("newBucket() returned nil")
	}
	if b.timers == nil {
		t.Fatal("bucket timers list is nil")
	}
	if b.expiration != -1 {
		t.Errorf("expected expiration -1, got %d", b.expiration)
	}
}

func TestBucketExpiration(t *testing.T) {
	b := newBucket()

	// Test SetExpiration and Expiration
	// First call: old value (-1) != new value (100), so should return true (value changed)
	if !b.SetExpiration(100) {
		t.Error("first SetExpiration should return true (value changed)")
	}
	if exp := b.Expiration(); exp != 100 {
		t.Errorf("expected expiration 100, got %d", exp)
	}

	// Test that setting same value returns false (value not changed)
	if b.SetExpiration(100) {
		t.Error("setting same expiration should return false (value unchanged)")
	}

	// Test that setting different value returns true
	if !b.SetExpiration(200) {
		t.Error("setting different expiration should return true")
	}
}

func TestBucketAddRemove(t *testing.T) {
	b := newBucket()
	t1 := &Timer{id: 1, expiration: 1000}
	t2 := &Timer{id: 2, expiration: 2000}

	// Add timers
	b.Add(t1)
	b.Add(t2)

	if t1.getBucket() != b {
		t.Error("t1 should be in bucket b")
	}
	if t2.getBucket() != b {
		t.Error("t2 should be in bucket b")
	}
	if b.timers.Len() != 2 {
		t.Errorf("expected 2 timers, got %d", b.timers.Len())
	}

	// Remove t1
	if !b.Remove(t1) {
		t.Error("failed to remove t1")
	}
	if t1.getBucket() != nil {
		t.Error("t1 bucket should be nil after removal")
	}
	if b.timers.Len() != 1 {
		t.Errorf("expected 1 timer after removal, got %d", b.timers.Len())
	}

	// Try to remove t1 again
	if b.Remove(t1) {
		t.Error("removing already removed timer should return false")
	}
}

func TestBucketFlush(t *testing.T) {
	b := newBucket()
	timerList := []*Timer{
		{id: 1, expiration: 1000},
		{id: 2, expiration: 2000},
		{id: 3, expiration: 3000},
	}

	for _, t := range timerList {
		b.Add(t)
	}

	reinserted := make([]*Timer, 0)
	reinsertFunc := func(t *Timer) {
		reinserted = append(reinserted, t)
	}

	b.Flush(reinsertFunc)

	if b.timers.Len() != 0 {
		t.Errorf("bucket should be empty after flush, got %d", b.timers.Len())
	}
	if len(reinserted) != 3 {
		t.Errorf("expected 3 reinserted timers, got %d", len(reinserted))
	}
	for _, timer := range timerList {
		if timer.getBucket() != nil {
			t.Error("timer bucket should be nil after flush")
		}
	}
}

func TestBucketConcurrentAddRemove(t *testing.T) {
	b := newBucket()
	var wg sync.WaitGroup

	numTimers := 1000
	addedTimers := make([]*Timer, numTimers)

	// Create timers
	for i := 0; i < numTimers; i++ {
		addedTimers[i] = &Timer{id: uint64(i), expiration: int64(i)}
	}

	// Concurrent add
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, t := range addedTimers {
			b.Add(t)
		}
	}()

	// Concurrent remove
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, t := range addedTimers {
			b.Remove(t)
		}
	}()

	wg.Wait()

	// Verify no nil timers in bucket
	for e := b.timers.Front(); e != nil; e = e.Next() {
		if e.Value == nil {
			t.Error("found nil timer in bucket")
		}
	}
}

func TestBucketRemoveConcurrency(t *testing.T) {
	b := newBucket()
	t1 := &Timer{id: 1, expiration: 1000}
	b.Add(t1)

	var wg sync.WaitGroup
	stopCount := 0

	// Simulate concurrent Stop calls
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Remove(t1) {
				stopCount++
			}
		}()
	}

	wg.Wait()

	if stopCount != 1 {
		t.Errorf("expected exactly 1 successful removal, got %d", stopCount)
	}
}

func BenchmarkBucketAdd(b *testing.B) {
	bucket := newBucket()
	timers := make([]*Timer, b.N)

	for i := 0; i < b.N; i++ {
		timers[i] = &Timer{id: uint64(i), expiration: int64(i)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucket.Add(timers[i])
	}
}

func BenchmarkBucketRemove(b *testing.B) {
	bucket := newBucket()
	timers := make([]*Timer, b.N)

	for i := 0; i < b.N; i++ {
		timers[i] = &Timer{id: uint64(i), expiration: int64(i)}
		bucket.Add(timers[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucket.Remove(timers[i])
	}
}

func BenchmarkBucketConcurrentAddRemove(b *testing.B) {
	bucket := newBucket()
	timers := make([]*Timer, b.N)

	for i := 0; i < b.N; i++ {
		timers[i] = &Timer{id: uint64(i), expiration: int64(i)}
	}

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bucket.Add(timers[i])
			bucket.Remove(timers[i])
		}(i)
	}
	wg.Wait()
}
