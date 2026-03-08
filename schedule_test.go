package timer

import (
	"testing"
	"time"
)

func TestNewCronSchedule(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{"5-field", "0 * * * *", false},
		{"6-field", "*/5 * * * * *", false},
		{"descriptor", "@hourly", false},
		{"daily", "@daily", false},
		{"weekly", "@weekly", false},
		{"minute", "*/15 * * * *", false},
		{"weekday", "0 9 * * 1-5", false},
		{"invalid", "invalid-cron", true},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewCronSchedule(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCronSchedule() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCronScheduleNext(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		expr          string
		expectedAfter time.Duration
	}{
		{"hourly", "0 * * * *", time.Hour},
		{"every minute", "* * * * *", time.Minute},
		{"every 15 minutes", "*/15 * * * *", 15 * time.Minute},
		{"6-field seconds", "10 * * * * *", 10 * time.Second},
		{"every 5 seconds", "*/5 * * * * *", 5 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule, err := NewCronSchedule(tt.expr)
			if err != nil {
				t.Fatalf("NewCronSchedule() error = %v", err)
			}

			next := schedule.Next(now)
			gap := next.Sub(now)

			// Allow small margin of error
			if gap < tt.expectedAfter || gap > tt.expectedAfter+time.Second {
				t.Errorf("Next() gap = %v, want %v", gap, tt.expectedAfter)
			}
		})
	}
}

func TestCronExpression(t *testing.T) {
	tests := []struct {
		expr  string
		valid bool
	}{
		{"0 * * * *", true},
		{"*/5 * * * * *", true},
		{"@hourly", true},
		{"invalid", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			result := CronExpression(tt.expr)
			if result != tt.valid {
				t.Errorf("CronExpression(%q) = %v, want %v", tt.expr, result, tt.valid)
			}
		})
	}
}

func TestMustNewCronSchedule(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustNewCronSchedule should panic with invalid expression")
		}
	}()

	MustNewCronSchedule("invalid-cron")
}

func TestEverySchedule(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	schedule := &EverySchedule{Interval: 5 * time.Minute}

	next := schedule.Next(now)
	expected := now.Add(5 * time.Minute)

	if !next.Equal(expected) {
		t.Errorf("EverySchedule.Next() = %v, want %v", next, expected)
	}
}

func TestFixedDateSchedule(t *testing.T) {
	tests := []struct {
		name            string
		hour            int
		minute          int
		second          int
		now             time.Time
		expectedInHours int
	}{
		{
			name:            "fixed hour 9",
			hour:            9,
			minute:          0,
			second:          0,
			now:             time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC),
			expectedInHours: 1,
		},
		{
			name:            "fixed hour same day passed",
			hour:            9,
			minute:          0,
			second:          0,
			now:             time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			expectedInHours: 23, // Next day at 9 AM
		},
		{
			name:            "fixed hour and minute",
			hour:            9,
			minute:          30,
			second:          0,
			now:             time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC),
			expectedInHours: 0, // Same hour, 30 minutes later
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := &FixedDateSchedule{
				Hour:   tt.hour,
				Minute: tt.minute,
				Second: tt.second,
			}

			next := schedule.Next(tt.now)
			hoursUntil := int(next.Sub(tt.now).Hours())

			if hoursUntil != tt.expectedInHours {
				t.Errorf("FixedDateSchedule.Next() hours until = %d, want %d", hoursUntil, tt.expectedInHours)
			}

			// Verify the time is correct
			if next.Hour() != tt.hour {
				t.Errorf("Expected hour %d, got %d", tt.hour, next.Hour())
			}
			if next.Minute() != tt.minute {
				t.Errorf("Expected minute %d, got %d", tt.minute, next.Minute())
			}
			if next.Second() != tt.second {
				t.Errorf("Expected second %d, got %d", tt.second, next.Second())
			}
		})
	}
}

func BenchmarkCronScheduleNext(b *testing.B) {
	schedule, _ := NewCronSchedule("*/5 * * * * *")
	now := time.Now().UTC()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schedule.Next(now)
	}
}

func BenchmarkNewCronSchedule(b *testing.B) {
	expr := "*/5 * * * * *"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewCronSchedule(expr)
	}
}
