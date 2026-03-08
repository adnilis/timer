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

	// 直接调用 Add，应该自动创建默认 actor
	id := Add(100*time.Millisecond, func() {
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
	id2 := Add(500*time.Millisecond, func() {
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
		id := actor.Add(time.Duration(i+1)*10*time.Millisecond, func() {
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

	id := Add(100*time.Millisecond, func() {
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

	// 不显式调用 Start，直接调用 Add
	var executed bool
	var wg sync.WaitGroup
	wg.Add(1)

	id := actor.Add(100*time.Millisecond, func() {
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

	id := actor.Add(100*time.Millisecond, func() {
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

	var addCalled, onceCalled bool
	var wg sync.WaitGroup

	// 测试 Add
	wg.Add(1)
	id1 := Add(100*time.Millisecond, func() {
		addCalled = true
		wg.Done()
	})

	if id1 == 0 {
		t.Fatal("Add failed")
	}

	// 测试 Once
	wg.Add(1)
	id2 := Once(100*time.Millisecond, func() {
		onceCalled = true
		wg.Done()
	})

	if id2 == 0 {
		t.Fatal("Once failed")
	}

	wg.Wait()

	if !addCalled {
		t.Fatal("Add task not executed")
	}
	if !onceCalled {
		t.Fatal("Once task not executed")
	}

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
	Add(50*time.Millisecond, func() {
		fmt.Println("任务 1 执行")
	})

	// 场景 2: 显式创建并使用自定义参数的 actor
	actor := NewDefaultTimerActor(5*time.Millisecond, 120)
	actor.Start(5*time.Millisecond, 120)
	defer actor.Stop()

	actor.Add(50*time.Millisecond, func() {
		fmt.Println("任务 2 执行")
	})

	// 等待执行
	time.Sleep(200 * time.Millisecond)
}
