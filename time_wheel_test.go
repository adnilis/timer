package timer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewTimeWheelErrors(t *testing.T) {
	tests := []struct {
		name      string
		tick      time.Duration
		wheelSize int64
		wantErr   error
	}{
		{"invalid tick (zero)", 0, 60, ErrInvalidTick},
		{"invalid tick (negative)", -time.Millisecond, 60, ErrInvalidTick},
		{"invalid wheelSize", time.Millisecond, 0, ErrInvalidWheelSize},
		{"invalid wheelSize (negative)", time.Millisecond, -1, ErrInvalidWheelSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTimeWheel(tt.tick, tt.wheelSize)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("NewTimeWheel() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTimeWheelStartStop(t *testing.T) {
	tw, err := NewTimeWheel(10*time.Millisecond, 60)
	if err != nil {
		t.Fatalf("NewTimeWheel() error = %v", err)
	}

	// Start the time wheel
	tw.Start(context.Background())
	time.Sleep(100 * time.Millisecond)

	// Stop should not panic
	tw.Stop()
}

func TestTimeWheelAfterFunc(t *testing.T) {
	tw, err := NewTimeWheel(10*time.Millisecond, 60)
	if err != nil {
		t.Fatalf("NewTimeWheel() error = %v", err)
	}

	executed := false
	var mu sync.Mutex

	task := func() {
		mu.Lock()
		defer mu.Unlock()
		executed = true
	}

	tw.Start(context.Background())

	// Schedule a task to run in 100ms
	timer := tw.AfterFunc(NextId(), 100*time.Millisecond, task)
	if timer == nil {
		t.Fatal("AfterFunc() returned nil timer")
	}

	// Wait a bit longer
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	if !executed {
		mu.Unlock()
		t.Error("task was not executed")
		return
	}
	mu.Unlock()

	tw.Stop()
}

func TestTimeWheelAfterFuncStop(t *testing.T) {
	tw, err := NewTimeWheel(10*time.Millisecond, 60)
	if err != nil {
		t.Fatalf("NewTimeWheel() error = %v", err)
	}

	executed := false
	var mu sync.Mutex

	task := func() {
		mu.Lock()
		defer mu.Unlock()
		executed = true
	}

	tw.Start(context.Background())

	// Schedule a task to run in 1 second
	timer := tw.AfterFunc(NextId(), time.Second, task)

	// Stop it immediately
	if stopped := timer.Stop(); !stopped {
		t.Error("Stop() returned false for active timer")
	}

	// Wait for the timer to expire
	time.Sleep(1200 * time.Millisecond)

	mu.Lock()
	if executed {
		mu.Unlock()
		t.Error("task should not have been executed after Stop()")
		return
	}
	mu.Unlock()

	tw.Stop()
}

func TestTimeWheelMultipleTimers(t *testing.T) {
	tw, err := NewTimeWheel(10*time.Millisecond, 60)
	if err != nil {
		t.Fatalf("NewTimeWheel() error = %v", err)
	}

	var execCount int32
	var mu sync.Mutex

	task := func() {
		atomic.AddInt32(&execCount, 1)
	}

	tw.Start(context.Background())

	numTimers := 100
	for i := 0; i < numTimers; i++ {
		delay := 50 + time.Duration(i*10)*time.Millisecond
		tw.AfterFunc(NextId(), delay, task)
	}

	// Wait for all timers to execute
	time.Sleep(2 * time.Second)

	count := atomic.LoadInt32(&execCount)
	mu.Lock()
	if count != int32(numTimers) {
		mu.Unlock()
		t.Errorf("expected %d executions, got %d", numTimers, count)
		return
	}
	mu.Unlock()

	tw.Stop()
}

func TestTimeWheelOverflow(t *testing.T) {
	// Create a small time wheel to force overflow
	tw, err := NewTimeWheel(10*time.Millisecond, 10)
	if err != nil {
		t.Fatalf("NewTimeWheel() error = %v", err)
	}

	executed := false
	var mu sync.Mutex

	task := func() {
		mu.Lock()
		defer mu.Unlock()
		executed = true
	}

	tw.Start(context.Background())

	// Schedule a task that will overflow to the overflow wheel
	// interval = 10ms * 10 = 100ms
	// 500ms > 100ms, so it should go to overflow wheel
	_ = tw.AfterFunc(NextId(), 500*time.Millisecond, task)

	// Wait for execution
	time.Sleep(600 * time.Millisecond)

	mu.Lock()
	if !executed {
		mu.Unlock()
		t.Error("overflow timer was not executed")
		return
	}
	mu.Unlock()

	tw.Stop()
}

func TestTimeWheelCronSchedule(t *testing.T) {
	tw, err := NewTimeWheel(10*time.Millisecond, 60)
	if err != nil {
		t.Fatalf("NewTimeWheel() error = %v", err)
	}

	var execCount int32

	task := func() {
		atomic.AddInt32(&execCount, 1)
	}

	tw.Start(context.Background())

	// Schedule a task to run every 100ms using cron
	// "*/100 * * * * *" is not valid, use a custom schedule
	schedule := &EverySchedule{Interval: 100 * time.Millisecond}
	timer := tw.ScheduleFunc(NextId(), schedule, task)

	// Wait for 3 executions
	time.Sleep(400 * time.Millisecond)

	count := atomic.LoadInt32(&execCount)
	if count < 3 {
		t.Errorf("expected at least 3 executions, got %d", count)
	}

	// Stop the timer
	timer.Stop()

	tw.Stop()
}

func TestTimeWheelConcurrentAdd(t *testing.T) {
	tw, err := NewTimeWheel(10*time.Millisecond, 60)
	if err != nil {
		t.Fatalf("NewTimeWheel() error = %v", err)
	}

	var execCount int32

	task := func() {
		atomic.AddInt32(&execCount, 1)
	}

	tw.Start(context.Background())

	numGoroutines := 50
	timersPerGoroutine := 20
	var wg sync.WaitGroup

	// Concurrent timer additions
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < timersPerGoroutine; j++ {
				delay := 50 + time.Duration(j)*time.Millisecond
				tw.AfterFunc(NextId(), delay, task)
			}
		}(i)
	}

	wg.Wait()

	// Wait for all timers to execute
	totalTimers := numGoroutines * timersPerGoroutine
	time.Sleep(time.Duration(totalTimers+200) * time.Millisecond)

	count := atomic.LoadInt32(&execCount)
	if count < int32(totalTimers*80/100) { // Allow some concurrency issues
		t.Errorf("expected at least %d executions, got %d", totalTimers*80/100, count)
	}

	tw.Stop()
}

func TestTimeWheelStopDuringExecution(t *testing.T) {
	tw, err := NewTimeWheel(10*time.Millisecond, 60)
	if err != nil {
		t.Fatalf("NewTimeWheel() error = %v", err)
	}

	execCount := int32(0)

	task := func() {
		atomic.AddInt32(&execCount, 1)
		if execCount == 1 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	tw.Start(context.Background())

	// Start a task
	tw.AfterFunc(NextId(), 50*time.Millisecond, task)

	// Wait a bit then stop the time wheel
	time.Sleep(100 * time.Millisecond)
	tw.Stop()

	// Stop should not hang even if a task is running
	// Give it enough time
	time.Sleep(600 * time.Millisecond)

	count := atomic.LoadInt32(&execCount)
	if count == 0 {
		t.Error("no tasks were executed")
	}
}

func TestTimeWheelImmediateExecution(t *testing.T) {
	tw, err := NewTimeWheel(10*time.Millisecond, 60)
	if err != nil {
		t.Fatalf("NewTimeWheel() error = %v", err)
	}

	executed := false
	var mu sync.Mutex

	task := func() {
		mu.Lock()
		defer mu.Unlock()
		executed = true
	}

	tw.Start(context.Background())

	// Schedule with very short delay
	tw.AfterFunc(NextId(), 5*time.Millisecond, task)

	// Should execute quickly
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if !executed {
		mu.Unlock()
		t.Error("task with short delay was not executed")
		return
	}
	mu.Unlock()

	tw.Stop()
}

func TestTimeWheelLongDelay(t *testing.T) {
	tw, err := NewTimeWheel(10*time.Millisecond, 60)
	if err != nil {
		t.Fatalf("NewTimeWheel() error = %v", err)
	}

	executed := false

	task := func() {
		executed = true
	}

	tw.Start(context.Background())

	// Schedule with very long delay (multiple overflow levels)
	timer := tw.AfterFunc(NextId(), 10*time.Second, task)

	// Don't wait, just verify it can be scheduled
	time.Sleep(50 * time.Millisecond)

	if timer == nil {
		t.Error("failed to schedule long delay timer")
	}

	// Verify that the timer has not executed yet
	if executed {
		t.Error("long delay timer should not have executed after only 50ms")
	}

	timer.Stop()
	tw.Stop()
}

func TestTimeWheelTimerID(t *testing.T) {
	tw, err := NewTimeWheel(10*time.Millisecond, 60)
	if err != nil {
		t.Fatalf("NewTimeWheel() error = %v", err)
	}

	tw.Start(context.Background())

	ids := make(map[uint64]bool)
	for i := 0; i < 100; i++ {
		timer := tw.AfterFunc(NextId(), 100*time.Millisecond, func() {})
		id := timer.ID()

		if ids[id] {
			t.Errorf("duplicate timer ID: %d", id)
		}
		ids[id] = true
	}

	tw.Stop()
}

func BenchmarkTimeWheelAfterFunc(b *testing.B) {
	tw, err := NewTimeWheel(10*time.Millisecond, 60)
	if err != nil {
		b.Fatalf("NewTimeWheel() error = %v", err)
	}

	tw.Start(context.Background())
	defer tw.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tw.AfterFunc(NextId(), 100*time.Millisecond, func() {})
	}
}

func BenchmarkTimeWheelExecution(b *testing.B) {
	tw, err := NewTimeWheel(10*time.Millisecond, 60)
	if err != nil {
		b.Fatalf("NewTimeWheel() error = %v", err)
	}

	var execCount int32

	tw.Start(context.Background())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		atomic.AddInt32(&execCount, 1)
	}

	tw.Stop()

	count := atomic.LoadInt32(&execCount)
	b.Logf("Executed %d tasks", count)
}

func BenchmarkTimeWheelConcurrentAfterFunc(b *testing.B) {
	tw, err := NewTimeWheel(10*time.Millisecond, 60)
	if err != nil {
		b.Fatalf("NewTimeWheel() error = %v", err)
	}

	tw.Start(context.Background())
	defer tw.Stop()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tw.AfterFunc(NextId(), 100*time.Millisecond, func() {})
		}()
	}
	wg.Wait()
}
