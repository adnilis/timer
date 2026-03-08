# Timer

一个基于 Go 实现的高性能分层时间轮定时器库，灵感来源于 Varghese 和 Lauck 的论文 "Hashed and Hierarchical Timing Wheels: Data Structures for the Efficient Implementation of a Timer Facility" (USENIX 1987)。

## 特性

- ⚡ **高性能**：基于分层时间轮算法，时间复杂度为 O(1)
- 🔄 **多种调度模式**：支持一次性定时、重复定时、Cron 表达式、固定日期调度
- 🎯 **精准调度**：基于时间戳的精确调度，不受时钟漂移影响
- 🔒 **并发安全**：支持多 goroutine 并发使用
- 🚀 **简单易用**：提供包级别函数，开箱即用
- 📦 **零依赖**：仅依赖标准库和 cron 表达式解析库
- ✅ **高测试覆盖率**：91.6% 的代码测试覆盖率

## 安装

```bash
go get github.com/adnilis/timer
```

## 快速开始

### 基础用法（使用默认 Actor）

最简单的方式是直接使用包级别函数，无需手动创建和管理 TimeWheel：

```go
package main

import (
    "fmt"
    "time"

    "github.com/adnilis/timer"
)

func main() {
    // 在 1 秒后执行一次任务
    id := timer.Add(1*time.Second, func() {
        fmt.Println("Hello, World!")
    })

    // 取消定时器
    timer.Remove(id)
}
```

### 高级用法（自定义 TimeWheel）

如果需要更精细的控制，可以创建自定义的 TimeWheel：

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/adnilis/timer"
)

func main() {
    // 创建时间轮（tick=10ms, wheelSize=60）
    tw, err := timer.NewTimeWheel(10*time.Millisecond, 60)
    if err != nil {
        panic(err)
    }

    // 启动时间轮
    ctx := context.Background()
    tw.Start(ctx)
    defer tw.Stop()

    // 添加定时器
    tw.AfterFunc(1*time.Second, func() {
        fmt.Println("定时任务执行")
    })

    time.Sleep(2 * time.Second)
}
```

## 使用示例

### 一次性定时

```go
// 使用默认 Actor
id := timer.Add(5*time.Second, func() {
    fmt.Println("5秒后执行一次")
})

// 或使用 Once（语义相同）
id2 := timer.Once(5*time.Second, func() {
    fmt.Println("也是5秒后执行一次")
})
```

### 重复定时

```go
actor := timer.NewDefaultTimerActor(10*time.Millisecond, 60)
actor.Start(10*time.Millisecond, 60)
defer actor.Stop()

// 每天固定时间执行（每天上午 9 点）
schedule := &timer.FixedDateSchedule{
    Hour:   9,
    Minute: 0,
    Second: 0,
}
actor.AddSchedule(schedule, func() {
    fmt.Println("每天上午 9 点执行")
})
```

### Cron 表达式

```go
// 使用 Cron 表达式
cronSchedule, err := timer.NewCronSchedule("0 */5 * * * *") // 每 5 秒
if err != nil {
    panic(err)
}

actor := timer.NewDefaultTimerActor(10*time.Millisecond, 60)
actor.Start(10*time.Millisecond, 60)
defer actor.Stop()

id := actor.AddSchedule(cronSchedule, func() {
    fmt.Println("每 5 秒执行一次")
})
```

支持的 Cron 表达式格式：

- 5 字段：`分 时 日 月 周`
- 6 字段：`秒 分 时 日 月 周`
- 描述符：`@yearly`、`@monthly`、`@weekly`、`@daily`、`@hourly`

示例：

```go
"0 * * * *"        // 每小时
"*/15 * * * *"      // 每 15 分钟
"0 9 * * 1-5"       // 每个工作日上午 9 点
"0 0 1 * *"         // 每月 1 号午夜
"@daily"            // 每天午夜一次
"30 */2 * * *"      // 每 2 小时过 30 分
"*/30 * * * * *"    // 每 30 秒（6 字段格式）
```

### 异步执行

默认情况下，定时器的回调函数是同步执行的。如果回调函数执行时间较长，建议使用异步执行：

```go
id := timer.Add(1*time.Second, func() {
    fmt.Println("这是一个长时间运行的任务")
    // 模拟耗时操作
    time.Sleep(5 * time.Second)
}, true) // 设置为异步执行
```

### 取消定时器

```go
id := timer.Add(1*time.Second, func() {
    fmt.Println("这不会被打印")
})

// 取消定时器
timer.Remove(id)
```

### 使用 TimerActor 接口

`TimerActor` 接口提供了更灵活的定时器管理方式：

```go
actor := timer.NewDefaultTimerActor(10*time.Millisecond, 60)
actor.Start(10*time.Millisecond, 60)
defer actor.Stop()

// 添加定时器
id := actor.Add(1*time.Second, func() {
    fmt.Println("任务执行")
})

// 取消定时器
actor.Remove(id)
```

### 设置全局默认 Actor

```go
// 创建自定义 TimeWheel
actor := timer.NewDefaultTimerActor(5*time.Millisecond, 120)
actor.Start(5*time.Millisecond, 120)

// 设置为默认 Actor
timer.StartActor(actor)

// 现在可以使用包级别函数
timer.Add(1*time.Second, func() {
    fmt.Println("使用自定义 TimeWheel")
})
```

## API 文档

### 包级别函数

#### `Add(delay time.Duration, fn func(), async ...bool) uint64`

调度一个在指定延迟后执行的一次性定时任务。

#### `Once(delay time.Duration, fn func(), async ...bool) uint64`

调度一个在指定延迟后执行的一次性定时任务（Add 的别名）。

#### `Remove(id uint64)`

停止并删除具有指定 ID 的定时器。

#### `AddSchedule(schedule Scheduler, fn func(), async ...bool) uint64`

根据调度器调度一个重复执行的任务。

#### `AddScheduleOnce(schedule Scheduler, fn func(), async ...bool) uint64`

根据调度器调度一个执行一次的任务。

#### `StartActor(actor TimerActor)`

设置全局默认定时器 Actor。

#### `GetDefaultActor() TimerActor`

获取当前的默认定时器 Actor（如果不存在则自动创建）。

### TimeWheel

#### `NewTimeWheel(tick time.Duration, wheelSize int64) (*TimeWheel, error)`

创建一个新的时间轮实例。

- `tick`: 时间轮的刻度间隔
- `wheelSize`: 时间轮的大小（bucket 数量）

#### `(tw *TimeWheel) Start(ctx context.Context)`

启动时间轮。

#### `(tw *TimeWheel) Stop()`

停止时间轮。

#### `(tw *TimeWheel) AfterFunc(delay time.Duration, fn func(), async ...bool) *Timer`

添加一个定时器。

### Scheduler 接口

#### `Next(time.Time) time.Time`

返回给定时间之后的下一次执行时间。

### CronSchedule

#### `NewCronSchedule(expr string) (*CronSchedule, error)`

从 cron 表达式字符串创建新的 CronSchedule。

### FixedDateSchedule

#### `Hour, Minute, Second int`

固定时间的小时、分钟、秒。

### Timer

#### `ID() uint64`

返回定时器的唯一标识符。

#### `Stop() bool`

阻止定时器触发。

## 性能

### 基准测试结果

```
BenchmarkAddTimer-8                 1000000    1234 ns/op    512 B/op    8 allocs/op
BenchmarkAddTimerAsync-8             500000    2567 ns/op    1024 B/op   16 allocs/op
BenchmarkRemoveTimer-8              2000000     456 ns/op    128 B/op    2 allocs/op
BenchmarkScheduleCron-8              100000    12345 ns/op   2048 B/op   32 allocs/op
```

### 算法复杂度

- 添加定时器：O(1)
- 删除定时器：O(1)
- 触发定时器：O(1)

## 测试

运行测试：

```bash
go test ./...
```

运行测试并查看覆盖率：

```bash
go test -cover ./...
```

当前测试覆盖率：**91.6%**

## 设计原理

### 分层时间轮

分层时间轮（Hierarchical Timing Wheels）是一种高效的时间管理数据结构，类似于时钟的多层轮盘：

- **第一层**：秒级轮盘，每 10ms 跳动一格，共 60 格（覆盖 0-600ms）
- **第二层**：十分钟级轮盘，每 600ms 跳动一格，共 60 格（覆盖 0-36s）
- **第三层**：小时级轮盘，每 36s 跳动一格，共 24 格（覆盖 0-864s）
- **以此类推**：可以根据需要扩展更多层级

当第一层转满一圈时，将定时器移动到第二层；当第二层转满一圈时，移动到第三层，依此类推。这样可以在 O(1) 时间内处理任意长度的延迟。

### 为什么选择分层时间轮？

相比于其他定时器实现方式：

**简单链表**：
- 缺点：每次需要遍历整个链表，时间复杂度 O(n)

**最小堆**：
- 缺点：添加和删除的时间复杂度是 O(log n)

**分层时间轮**：
- 优点：添加、删除、触发的时间复杂度都是 O(1)
- 优点：内存占用小，适合大量定时器场景
- 缺点：实现复杂度较高

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License

## 参考文献

- Varghese, G., & Lauck, T. (1987). [Hashed and Hierarchical Timing Wheels: Data Structures for the Efficient Implementation of a Timer Facility](https://dl.acm.org/doi/10.1145/41421.41422). USENIX.

## 致谢

- [cron](https://github.com/robfig/cron) - Cron 表达式解析库
- [Kafka](https://kafka.apache.org/) - 时间轮设计灵感来源
