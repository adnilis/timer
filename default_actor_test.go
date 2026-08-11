package timer

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestDefaultAutoAutoCreate 测试在没有调用 StartActor 的情况下，包级函数能自动创建默认 actor
func TestDefaultAutoAutoCreate(t *testing.T) {
	// 重置全局状态
	defaultActor = nil

	var executed bool
	var wg sync.WaitGroup
	wg.Add(1)

	// 直接调用 Once，应该自动创建默认 actor
	id := Once(100*time.Millisecond, func() {
		executed = true
		wg.Done()
	})

	if id == 0 {
		t.Fatal("Expected non-zero timer ID")
	}

	// 等待任务执行
	wg.Wait()

	// 验证任务被执行
	if !executed {
		t.Fatal("Expected task to be executed")
	}

	// 测试 Remove 功能
	executed2 := false
	id2 := Once(500*time.Millisecond, func() {
		executed2 = true
	})

	// 取消任务
	getDefaultActor().(*DefaultTimerActor).Remove(id2)

	// 等待一段时间，确保任务不会执行
	time.Sleep(600 * time.Millisecond)

	if executed2 {
		t.Fatal("Expected cancelled task not to be executed")
	}

	t.Log("Test passed: default actor auto-creation and Remove work correctly")
}

// TestDefaultTimerActor_Concurrent 测试并发使用 DefaultTimerActor
func TestDefaultTimerActor_Concurrent(t *testing.T) {
	actor := NewDefaultTimerActor(10*time.Millisecond, 60)
	err := actor.Start(10*time.Millisecond, 60)
	if err != nil {
		t.Fatalf("Failed to start actor: %v", err)
	}
	defer actor.Stop()

	var mu sync.Mutex
	var counter int
	var wg sync.WaitGroup

	// 并发添加多个定时器
	for i := 0; i < 100; i++ {
		wg.Add(1)
		id := actor.Once(time.Duration(i+1)*10*time.Millisecond, func() {
			mu.Lock()
			counter++
			mu.Unlock()
			wg.Done()
		})

		if id == 0 {
			t.Fatal("Expected non-zero timer ID")
		}
	}

	// 等待所有任务完成
	wg.Wait()

	if counter != 100 {
		t.Fatalf("Expected 100 tasks, got %d", counter)
	}
}

func TestDefaultTimerActor_AddRepeatsUntilRemoved(t *testing.T) {
	actor := NewDefaultTimerActor(10*time.Millisecond, 60)
	if err := actor.Start(10*time.Millisecond, 60); err != nil {
		t.Fatalf("Failed to start actor: %v", err)
	}
	defer actor.Stop()

	const interval = 20 * time.Millisecond
	var mu sync.Mutex
	count := 0
	reached := make(chan struct{})

	id := actor.Add(interval, func() {
		mu.Lock()
		defer mu.Unlock()

		count++
		if count == 3 {
			close(reached)
		}
	})
	if id == 0 {
		t.Fatal("Expected non-zero timer ID")
	}

	select {
	case <-reached:
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for repeated executions")
	}

	actor.Remove(id)

	time.Sleep(3 * interval)
	mu.Lock()
	countAfterRemove := count
	mu.Unlock()

	time.Sleep(3 * interval)
	mu.Lock()
	defer mu.Unlock()

	if countAfterRemove < 3 {
		t.Fatalf("Expected at least 3 executions before Remove, got %d", countAfterRemove)
	}
	if count != countAfterRemove {
		t.Fatalf("Expected no executions after Remove, got %d then %d", countAfterRemove, count)
	}
}

func TestDefaultTimerActor_OnceRunsOnce(t *testing.T) {
	actor := NewDefaultTimerActor(10*time.Millisecond, 60)
	if err := actor.Start(10*time.Millisecond, 60); err != nil {
		t.Fatalf("Failed to start actor: %v", err)
	}
	defer actor.Stop()

	const delay = 20 * time.Millisecond
	var mu sync.Mutex
	count := 0
	fired := make(chan struct{})

	id := actor.Once(delay, func() {
		mu.Lock()
		defer mu.Unlock()

		count++
		if count == 1 {
			close(fired)
		}
	})
	if id == 0 {
		t.Fatal("Expected non-zero timer ID")
	}

	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for one-time execution")
	}

	time.Sleep(4 * delay)
	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("Expected Once to execute exactly once, got %d", count)
	}
}

func TestDefaultTimerActor_AddRejectsNonPositiveInterval(t *testing.T) {
	actor := &DefaultTimerActor{}

	for _, interval := range []time.Duration{0, -time.Millisecond} {
		if id := actor.Add(interval, func() {}); id != 0 {
			t.Fatalf("Expected invalid interval %s to return 0, got %d", interval, id)
		}
	}

	if actor.tw != nil {
		t.Fatal("Invalid Add interval should not start the actor")
	}
}

// TestDefaultTimerActor_RemoveConcurrent 测试并发删除定时器
func TestDefaultTimerActor_RemoveConcurrent(t *testing.T) {
	actor := NewDefaultTimerActor(10*time.Millisecond, 60)
	err := actor.Start(10*time.Millisecond, 60)
	if err != nil {
		t.Fatalf("Failed to start actor: %v", err)
	}
	defer actor.Stop()

	var executed int
	var mu sync.Mutex

	// 添加定时器，1秒后执行
	var ids []uint64
	for i := 0; i < 5; i++ {
		id := actor.Add(1*time.Second, func() {
			mu.Lock()
			executed++
			mu.Unlock()
		})
		ids = append(ids, id)
	}

	// 立即删除这些定时器
	for _, id := range ids {
		actor.Remove(id)
	}

	// 等待一段时间，确保已经过了定时器的执行时间
	time.Sleep(2 * time.Second)

	// 验证任务没有被执行
	if executed > 0 {
		t.Fatalf("Expected 0 tasks to execute, got %d", executed)
	}

	t.Log("Test passed: Remove successfully cancelled all timers")
}

// TestStartActor_Nil 测试传入 nil 时创建默认 actor
func TestStartActor_Nil(t *testing.T) {
	// 重置
	defaultActor = nil

	// 传入 nil
	StartActor(nil)

	// 应该创建一个 DefaultTimerActor
	if defaultActor == nil {
		t.Fatal("Expected defaultActor to be initialized")
	}

	// 测试它能正常工作
	var executed bool
	var wg sync.WaitGroup
	wg.Add(1)

	id := Once(100*time.Millisecond, func() {
		executed = true
		wg.Done()
	})

	if id == 0 {
		t.Fatal("Expected non-zero timer ID")
	}

	wg.Wait()

	if !executed {
		t.Fatal("Expected task to be executed")
	}
}

// TestDefaultTimerActor_EagerStart 测试 ensureStarted 方法的行为
func TestDefaultTimerActor_EagerStart(t *testing.T) {
	actor := &DefaultTimerActor{}

	// 不显式调用 Start，直接调用 Once
	var executed bool
	var wg sync.WaitGroup
	wg.Add(1)

	id := actor.Once(100*time.Millisecond, func() {
		executed = true
		wg.Done()
	})

	if id == 0 {
		t.Fatal("Expected non-zero timer ID")
	}

	wg.Wait()

	if !executed {
		t.Fatal("Expected task to be executed")
	}

	actor.Stop()
}

// TestDefaultTimerActor_Stop 测试 Stop 方法
func TestDefaultTimerActor_Stop(t *testing.T) {
	actor := NewDefaultTimerActor(10*time.Millisecond, 60)
	err := actor.Start(10*time.Millisecond, 60)
	if err != nil {
		t.Fatalf("Failed to start actor: %v", err)
	}

	actor.Stop()

	// 停止后添加任务，应该自动重启
	var executed bool
	var wg sync.WaitGroup
	wg.Add(1)

	id := actor.Once(100*time.Millisecond, func() {
		executed = true
		wg.Done()
	})

	if id == 0 {
		t.Fatal("Expected non-zero timer ID")
	}

	// 等待任务执行或超时
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if !executed {
			t.Fatal("Expected task to be executed after restart")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out waiting for task to execute")
	}

	actor.Stop()
}

// TestDefaultTimerActor_DoubleStop 测试重复调用 Stop
func TestDefaultTimerActor_DoubleStop(t *testing.T) {
	actor := NewDefaultTimerActor(10*time.Millisecond, 60)
	actor.Start(10*time.Millisecond, 60)

	actor.Stop()
	actor.Stop() // 不应该 panic

	t.Log("Test passed: double Stop is safe")
}

// TestPackageLevelFunctions 测试所有包级函数
func TestPackageLevelFunctions(t *testing.T) {
	// 重置
	defaultActor = nil

	// 创建一个在下一分钟执行的 FixedDateSchedule，避免等待太久
	now := time.Now()
	schedule := &FixedDateSchedule{
		Hour:   now.Hour(),
		Minute: (now.Minute() + 1) % 60,
		Second: 0,
	}

	const interval = 20 * time.Millisecond
	var mu sync.Mutex
	var addCount, onceCount int
	addRepeated := make(chan struct{})
	onceExecuted := make(chan struct{})

	// 测试 Add 的周期执行语义
	id1 := Add(interval, func() {
		mu.Lock()
		defer mu.Unlock()

		addCount++
		if addCount == 2 {
			close(addRepeated)
		}
	})

	if id1 == 0 {
		t.Fatal("Add failed")
	}

	// 测试 Once
	id2 := Once(interval, func() {
		mu.Lock()
		defer mu.Unlock()

		onceCount++
		if onceCount == 1 {
			close(onceExecuted)
		}
	})

	if id2 == 0 {
		t.Fatal("Once failed")
	}

	select {
	case <-addRepeated:
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for Add to repeat")
	}

	select {
	case <-onceExecuted:
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for Once")
	}

	Remove(id1)
	Remove(id2)

	time.Sleep(3 * interval)
	mu.Lock()
	addCountAfterRemove := addCount
	onceCountAfterRemove := onceCount
	mu.Unlock()

	time.Sleep(3 * interval)
	mu.Lock()
	if addCount < 2 {
		t.Fatalf("Expected Add to execute at least twice, got %d", addCount)
	}
	if addCount != addCountAfterRemove {
		t.Fatalf("Expected Add to stop after Remove, got %d then %d", addCountAfterRemove, addCount)
	}
	if onceCount != 1 || onceCountAfterRemove != 1 {
		t.Fatalf("Expected Once to execute exactly once, got %d", onceCount)
	}
	mu.Unlock()

	// 测试 AddScheduleOnce (不等待，只是验证能调用)
	id3 := AddScheduleOnce(schedule, func() {
		// 不会立即执行，我们只是验证能调用这个函数
	})

	if id3 == 0 {
		t.Fatal("AddScheduleOnce failed")
	}

	// 测试 AddSchedule (不等待，只是验证能调用)
	id4 := AddSchedule(schedule, func() {
		// 不会执行，我们只是验证能调用这个函数
	})

	if id4 == 0 {
		t.Fatal("AddSchedule failed")
	}

	// 清理
	getDefaultActor().(*DefaultTimerActor).Remove(id3)
	getDefaultActor().(*DefaultTimerActor).Remove(id4)

	t.Log("All package-level functions work correctly")
}

// TestDefaultActorExample 是一个使用示例，展示如何使用默认 actor
func TestDefaultActorExample(t *testing.T) {
	// 场景 1: 直接使用包级函数，无需显式设置
	Once(50*time.Millisecond, func() {
		fmt.Println("任务 1 执行")
	})

	// 场景 2: 显式创建并使用自定义参数的 actor
	actor := NewDefaultTimerActor(5*time.Millisecond, 120)
	actor.Start(5*time.Millisecond, 120)
	defer actor.Stop()

	actor.Once(50*time.Millisecond, func() {
		fmt.Println("任务 2 执行")
	})

	// 等待执行
	time.Sleep(200 * time.Millisecond)
}
