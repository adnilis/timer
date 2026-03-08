package timer

import (
	"fmt"
	"strings"
	"time"

	cronexpr "github.com/robfig/cron/v3"
)

// Scheduler 定义任务执行计划的标准接口
type Scheduler interface {
	// Next 返回给定时间（上一次执行时间）之后的下一次执行时间。
	// 如果没有安排下次执行时间，则返回零时。
	//
	// 所有时间必须是 UTC 时间。
	Next(time.Time) time.Time
}

// CronSchedule 实现了标准 Linux cron 表达式的 Scheduler 接口。
// 支持 5 字段（分 时 日 月 周）、6 字段（秒 分 时 日 月 周）格式
// 以及预定义描述符，如 @yearly、@monthly、@weekly、@daily、@hourly、@reboot。
//
// 示例：
//   - "0 * * * *" - 每小时
//   - "*/15 * * * *" - 每 15 分钟
//   - "0 9 * * 1-5" - 每个工作日上午 9 点
//   - "0 0 1 * *" - 每月 1 号午夜
//   - "@daily" - 每天午夜一次
//   - "30 */2 * * *" - 每 2 小时过 30 分
//   - "*/30 * * * * *" - 每 30 秒（6 字段格式）
//
// 注意：调度器使用本地时间进行 cron 表达式计算，并返回 UTC 时间。
// 这与本地系统上的典型 cron 行为相匹配。
type CronSchedule struct {
	schedule cronexpr.Schedule // 底层的 cron 表达式调度器
}

// NewCronSchedule 从 cron 表达式字符串创建新的 CronSchedule。
// 支持标准 Linux 5 字段和扩展的 6 字段 cron 表达式，
// 以及预定义描述符（@yearly、@monthly、@weekly、@daily、@hourly）。
//
// 解析器选项包括：
//   - Second: 支持可选的秒字段（6 字段格式）
//   - Minute | Hour | Dom | Month | Dow: 标准 5 字段格式
//   - Descriptor: 预定义的时间描述符（@yearly、@monthly 等）
//
// 如果表达式无效，则返回错误。
func NewCronSchedule(expr string) (*CronSchedule, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("cron 表达式为空")
	}

	// 先尝试 6 字段格式（带秒）
	p6 := cronexpr.NewParser(
		cronexpr.Second |
			cronexpr.Minute |
			cronexpr.Hour |
			cronexpr.Dom |
			cronexpr.Month |
			cronexpr.Dow |
			cronexpr.Descriptor,
	)

	schedule, err := p6.Parse(expr)
	if err == nil {
		return &CronSchedule{schedule: schedule}, nil
	}

	// 尝试 5 字段格式（标准 Linux cron，不带秒）
	p5 := cronexpr.NewParser(
		cronexpr.Minute |
			cronexpr.Hour |
			cronexpr.Dom |
			cronexpr.Month |
			cronexpr.Dow |
			cronexpr.Descriptor,
	)

	schedule, err = p5.Parse(expr)
	if err != nil {
		return nil, err
	}

	return &CronSchedule{schedule: schedule}, nil
}

// MustNewCronSchedule 创建新的 CronSchedule，并在出错时 panic。
// 这对于在包初始化时使用常量表达式创建调度器很有用。
func MustNewCronSchedule(expr string) *CronSchedule {
	schedule, err := NewCronSchedule(expr)
	if err != nil {
		panic(err)
	}
	return schedule
}

// Next 返回给定上一次执行时间之后的下一次执行时间。
// 它将 UTC 时间转换为本地时间进行 cron 表达式计算，
// 计算下一次执行时间，并返回 UTC 时间。
//
// 示例：
//
//	schedule := NewCronSchedule("0 9 * * 1-5") // 工作日上午 9 点
//	next := schedule.Next(time.Now().UTC())
func (s *CronSchedule) Next(prev time.Time) time.Time {
	// 将 UTC 时间转换为本地时间进行 cron 表达式计算
	localTime := prev.Local()
	// 计算下一次执行时间（基于本地时间）
	nextLocal := s.schedule.Next(localTime)
	// 转换回 UTC 时间
	return nextLocal.UTC()
}

// EverySchedule 实现了每隔固定时间间隔执行的调度器
type EverySchedule struct {
	Interval time.Duration // 执行间隔
}

// Next 返回上一次执行时间加上间隔时间
func (s *EverySchedule) Next(prev time.Time) time.Time {
	return prev.Add(s.Interval)
}

// FixedDateSchedule 实现了在每天固定时间执行的调度器
type FixedDateSchedule struct {
	Hour, Minute, Second int // 固定时间的小时、分钟、秒
}

// Next 返回下一次执行时间（每天固定时间）
func (s *FixedDateSchedule) Next(prev time.Time) time.Time {
	hour := prev.Hour()
	if s.Hour >= 0 {
		hour = s.Hour
	}

	fixedTime := time.Date(
		prev.Year(),
		prev.Month(),
		prev.Day(),
		hour,
		s.Minute,
		s.Second,
		0,
		prev.Location(),
	)

	remain := fixedTime.UnixNano() - prev.UnixNano()
	if remain > 0 {
		// 固定时间还未到达，返回固定时间
		return prev.Add(time.Duration(remain))
	}

	if s.Hour >= 0 {
		// 固定时间已过，返回明天的固定时间
		return fixedTime.Add(24 * time.Hour)
	}

	// 如果未设置小时，返回下一小时的固定时间
	return fixedTime.Add(time.Hour)
}

// CronExpression 验证并解析 cron 表达式字符串。
// 如果表达式有效，则返回 true。
// 这是一个便捷函数，用于在不创建调度器的情况下验证 cron 表达式。
func CronExpression(expr string) bool {
	_, err := NewCronSchedule(expr)
	return err == nil
}
