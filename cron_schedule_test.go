package timer

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestAddCronSchedule 测试包级别的 AddCronSchedule 函数
func TestAddCronSchedule(t *testing.T) {
	defaultActor = nil

	var count int
	var mu sync.Mutex
	var wg sync.WaitGroup

	// 每 1 秒执行一次（使用 6 字段格式：秒 分 时 日 月 周）
	wg.Add(2)
	id := AddCronSchedule("*/1 * * * * *", func() {
		mu.Lock()
		count++
		mu.Unlock()
		wg.Done()
	})

	if id == 0 {
		t.Fatal("AddCronSchedule failed")
	}

	// 等待执行 2 次，设置超时
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 成功完成
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out waiting for cron executions")
	}

	mu.Lock()
	if count < 2 {
		t.Fatalf("Expected at least 2 executions, got %d", count)
	}
	mu.Unlock()

	// 取消定时器
	Remove(id)

	// 等待一段时间，确保定时器已被取消
	time.Sleep(1500 * time.Millisecond)

	// 验证定时器已停止
	lastCount := 0
	mu.Lock()
	lastCount = count
	mu.Unlock()

	time.Sleep(1500 * time.Millisecond)

	mu.Lock()
	if count > lastCount {
		t.Error("Timer should have been cancelled")
	}
	mu.Unlock()
}

// TestAddCronSchedule_InvalidExpr 测试无效的 cron 表达式
func TestAddCronSchedule_InvalidExpr(t *testing.T) {
	defaultActor = nil

	// 无效的 cron 表达式应该返回 0
	id := AddCronSchedule("invalid cron expr", func() {
		t.Fatal("This should not execute")
	})

	if id != 0 {
		t.Fatal("Invalid cron expression should return 0")
	}
}

// TestAddCronSchedule_Actor 测试 DefaultTimerActor 的 AddCronSchedule 方法
func TestAddCronSchedule_Actor(t *testing.T) {
	actor := NewDefaultTimerActor(10*time.Millisecond, 60)
	actor.Start(10*time.Millisecond, 60)
	defer actor.Stop()

	var count int
	var mu sync.Mutex

	// 每 1 秒执行一次（使用 6 字段格式）
	id := actor.AddCronSchedule("*/1 * * * * *", func() {
		mu.Lock()
		count++
		mu.Unlock()
	})

	if id == 0 {
		t.Fatal("AddCronSchedule failed")
	}

	// 等待执行
	time.Sleep(2500 * time.Millisecond)

	mu.Lock()
	if count < 1 {
		t.Fatalf("Expected at least 1 execution, got %d", count)
	}
	mu.Unlock()

	// 取消定时器
	actor.Remove(id)
}

// TestAddCronSchedule_Concurrent 测试并发添加 cron 定时器
func TestAddCronSchedule_Concurrent(t *testing.T) {
	defaultActor = nil

	var wg sync.WaitGroup
	const numTimers = 10

	// 并发添加多个 cron 定时器
	for i := 0; i < numTimers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			var executed bool
			var mu sync.Mutex
			var innerWg sync.WaitGroup
			innerWg.Add(1)

			// 每 100ms 执行一次
			id := AddCronSchedule("*/100 * * * * *", func() {
				mu.Lock()
				if !executed {
					executed = true
					innerWg.Done()
				}
				mu.Unlock()
			})

			if id == 0 {
				t.Errorf("Failed to add cron timer %d", idx)
				return
			}

			// 等待执行后取消
			innerWg.Wait()
			Remove(id)
		}(i)
	}

	wg.Wait()
}

// TestAddCronSchedule_Descriptor 测试 cron 描述符
func TestAddCronSchedule_Descriptor(t *testing.T) {
	defaultActor = nil

	descriptors := []string{
		"@yearly",
		"@monthly",
		"@weekly",
		"@daily",
		"@hourly",
	}

	for _, desc := range descriptors {
		id := AddCronSchedule(desc, func() {
			fmt.Printf("Descriptor %s executed\n", desc)
		})

		if id == 0 {
			t.Errorf("AddCronSchedule failed for descriptor %s", desc)
		}

		// 取消定时器
		Remove(id)
	}
}

// TestAddCronSchedule_ComplexExpr 测试复杂的 cron 表达式
func TestAddCronSchedule_ComplexExpr(t *testing.T) {
	defaultActor = nil

	// 每周一到周五，上午 9 点到下午 5 点，每 30 分钟执行一次
	complexExpr := "0/30 9-17 * * 1-5"
	id := AddCronSchedule(complexExpr, func() {
		fmt.Println("Complex cron task executed")
	})

	if id == 0 {
		t.Fatalf("AddCronSchedule failed for complex expression: %s", complexExpr)
	}

	Remove(id)
}

// TestAddCronSchedule_Range 测试范围和列表表达式
func TestAddCronSchedule_Range(t *testing.T) {
	defaultActor = nil

	// 只测试快速触发的表达式，避免测试超时
	testExprs := []string{
		"*/5 * * * * *",      // 每 5 秒
		"10,20,30 * * * * *", // 每分钟的 10、20、30 秒
	}

	for _, expr := range testExprs {
		var executed bool
		var mu sync.Mutex
		var wg sync.WaitGroup
		wg.Add(1)

		id := AddCronSchedule(expr, func() {
			mu.Lock()
			if !executed {
				executed = true
				wg.Done()
			}
			mu.Unlock()
		})

		if id == 0 {
			t.Errorf("AddCronSchedule failed for expression: %s", expr)
			continue
		}

		// 等待第一次执行，设置超时
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// 成功执行
		case <-time.After(15 * time.Second):
			t.Errorf("Timeout waiting for expression: %s", expr)
		}

		Remove(id)
	}
}

// TestAddCronSchedule_RemoveBeforeExecution 测试在执行前取消定时器
func TestAddCronSchedule_RemoveBeforeExecution(t *testing.T) {
	defaultActor = nil

	executed := false
	var mu sync.Mutex

	// 添加一个在 5 秒后才会执行的任务
	id := AddCronSchedule("0 0,5,10 * * * *", func() {
		mu.Lock()
		executed = true
		mu.Unlock()
	})

	if id == 0 {
		t.Fatal("AddCronSchedule failed")
	}

	// 立即取消
	Remove(id)

	// 等待足够的时间，验证任务没有执行
	time.Sleep(6 * time.Second)

	mu.Lock()
	if executed {
		t.Error("Task should not have been executed after being removed")
	}
	mu.Unlock()
}
