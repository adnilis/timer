package timer

import (
	"sync"
	"testing"
	"time"
)

func TestReproAsyncAdd(t *testing.T) {
	actor := NewDefaultTimerActor(10*time.Millisecond, 60)
	if err := actor.Start(10*time.Millisecond, 60); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer actor.Stop()

	var wg sync.WaitGroup
	const (
		timerCount         = 5
		executionsPerTimer = 3
	)
	var mu sync.Mutex
	counts := make([]int, timerCount)
	ids := make([]uint64, 0, timerCount)

	wg.Add(timerCount * executionsPerTimer)
	for i := 0; i < timerCount; i++ {
		idx := i
		id := actor.Add(20*time.Millisecond, func() {
			mu.Lock()
			defer mu.Unlock()

			if counts[idx] >= executionsPerTimer {
				return
			}
			counts[idx]++
			wg.Done()
		}, true)
		if id == 0 {
			t.Fatalf("async Add #%d returned 0", i)
		}
		ids = append(ids, id)
	}

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("async Add timeout")
	}

	for _, id := range ids {
		actor.Remove(id)
	}

	mu.Lock()
	defer mu.Unlock()
	for i, count := range counts {
		if count != executionsPerTimer {
			t.Fatalf("async Add #%d executed %d times, expected %d", i, count, executionsPerTimer)
		}
	}
}

func TestReproScheduleOnce(t *testing.T) {
	actor := NewDefaultTimerActor(10*time.Millisecond, 60)
	if err := actor.Start(10*time.Millisecond, 60); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer actor.Stop()

	schedule := &FixedDateSchedule{Hour: 0, Minute: 0, Second: 0}

	for i := 0; i < 3; i++ {
		id := actor.AddScheduleOnce(schedule, func() {})
		t.Logf("AddScheduleOnce #%d -> id=%d", i, id)
		if id == 0 {
			t.Fatalf("AddScheduleOnce #%d returned 0", i)
		}
	}
}
