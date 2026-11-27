package acp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const sampleAgyUsageScreen = `
Antigravity CLI 1.5.18
a76515791@gmail.com
────────────────────────────────────────
>
────────────────────────────────────────
└ Models & Quota

  Account: user@example.com

GEMINI MODELS
  Models within this group: Gemini 4 Neon, Gemini 4 Carbon

  Weekly Limit Remaining
[████████████████████████████████████████████████░░] 96.09%
Refreshes in 6d 23h 19m

  Five Hour Limit Remaining
[████████████████████████████████████████████░░░░░░] 87.34%
Refreshes in 4h 19m


CLAUDE AND GPT MODELS
  Models within this group: Claude Opus 5.5, Claude Sonnet 5.5, GPT-OSS 120B

  Weekly Limit Remaining
[██████████████████████████████████████████████████] 100.00%
Quota available

  Five Hour Limit Remaining
[██████████████████████████████████████████████████] 100.00%
Quota available

  │Within each group, models share a weekly limit and a 5-hour limit.
auto · Gemini 4 Neon · high (Google AI Pro)
`

func TestParseAgyUsageReport(t *testing.T) {
	now := time.Date(2026, 11, 27, 20, 0, 0, 0, time.UTC)
	report, err := parseAgyUsageReport(sampleAgyUsageScreen, now)
	if err != nil {
		t.Fatalf("parseAgyUsageReport: %v", err)
	}
	if report.Provider != "antigravity" {
		t.Fatalf("Provider = %q, want antigravity", report.Provider)
	}
	if report.Email != "user@example.com" {
		t.Fatalf("Email = %q, want user@example.com", report.Email)
	}
	if report.Plan != "Google AI Pro" {
		t.Fatalf("Plan = %q, want Google AI Pro", report.Plan)
	}
	if len(report.Buckets) != 2 {
		t.Fatalf("len(Buckets) = %d, want 2 (%+v)", len(report.Buckets), report.Buckets)
	}

	gem := report.Buckets[0]
	if gem.Name != "Gemini Models" || len(gem.Windows) != 2 {
		t.Fatalf("unexpected first bucket: %+v", gem)
	}
	// Weekly: 96.09% remaining -> 4% used; 6d 23h 19m = 602340s
	if gem.Windows[0].WindowSeconds != agyUsageWeekSeconds || gem.Windows[0].UsedPercent != 4 {
		t.Fatalf("unexpected Gemini weekly window: %+v", gem.Windows[0])
	}
	wantWeekSecs := (6*24+23)*3600 + 19*60
	if gem.Windows[0].ResetAfterSeconds != wantWeekSecs {
		t.Fatalf("Gemini weekly ResetAfterSeconds = %d, want %d", gem.Windows[0].ResetAfterSeconds, wantWeekSecs)
	}
	// 5h: 87.34% remaining -> 13% used; 4h 19m = 15540s
	if gem.Windows[1].WindowSeconds != agyUsageFiveHourSeconds || gem.Windows[1].UsedPercent != 13 {
		t.Fatalf("unexpected Gemini 5h window: %+v", gem.Windows[1])
	}
	if gem.Windows[1].ResetAfterSeconds != 4*3600+19*60 {
		t.Fatalf("Gemini 5h ResetAfterSeconds = %d, want %d", gem.Windows[1].ResetAfterSeconds, 4*3600+19*60)
	}

	cg := report.Buckets[1]
	if cg.Name != "Claude & GPT Models" || len(cg.Windows) != 2 {
		t.Fatalf("unexpected second bucket: %+v", cg)
	}
	if cg.Windows[0].UsedPercent != 0 || cg.Windows[0].ResetAfterSeconds != 0 {
		t.Fatalf("unexpected Claude/GPT weekly window: %+v", cg.Windows[0])
	}
	if cg.Windows[1].UsedPercent != 0 || cg.Windows[1].ResetAfterSeconds != 0 {
		t.Fatalf("unexpected Claude/GPT 5h window: %+v", cg.Windows[1])
	}
}

func TestGetUsage_MockPTYProbe(t *testing.T) {
	script := filepath.Join(t.TempDir(), "agy")
	py := `#!/usr/bin/env python3
import sys, time
sys.stdout.write("Antigravity CLI 1.5.18\r\n> \r\n? for shortcuts\r\n")
sys.stdout.flush()
buf = ""
while True:
    ch = sys.stdin.read(1)
    if not ch:
        break
    buf += ch
    if "/usage" in buf and ("\r" in buf or "\n" in buf):
        sys.stdout.write("""\r\n└ Models & Quota\r\n\r\n  Account: mock@example.com\r\n\r\nGEMINI MODELS\r\n  Weekly Limit Remaining\r\n[██████████] 90.00%\r\nRefreshes in 5d 2h\r\n  Five Hour Limit Remaining\r\n[████████░░] 80.00%\r\nRefreshes in 3h 10m\r\n\r\nCLAUDE AND GPT MODELS\r\n  Weekly Limit Remaining\r\n[██████████] 100.00%\r\nQuota available\r\n  Five Hour Limit Remaining\r\n[██████████] 100.00%\r\nQuota available\r\nauto · Gemini 4 Neon · high (Google AI Pro)\r\n""")
        sys.stdout.flush()
        time.sleep(2)
        break
`
	if err := os.WriteFile(script, []byte(py), 0o755); err != nil {
		t.Fatalf("write mock agy: %v", err)
	}

	agent := &Agent{
		workDir: t.TempDir(),
		cmd:     script,
	}
	if !agent.SupportsUsage() {
		t.Fatal("expected agy agent to support usage")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	report, err := agent.GetUsage(ctx)
	if err != nil {
		t.Fatalf("GetUsage: %v", err)
	}
	if report.Email != "mock@example.com" || report.Plan != "Google AI Pro" {
		t.Fatalf("unexpected account metadata: %+v", report)
	}
	if len(report.Buckets) != 2 || report.Buckets[0].Windows[1].UsedPercent != 20 {
		t.Fatalf("unexpected buckets: %+v", report.Buckets)
	}

	// Subsequent call within cache TTL with short deadline must succeed from cache.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer shortCancel()
	cached, err := agent.GetUsage(shortCtx)
	if err != nil || cached.Email != "mock@example.com" {
		t.Fatalf("expected cached report on short deadline, got %+v err=%v", cached, err)
	}
}

func TestSupportsUsage_FalseForNonAntigravity(t *testing.T) {
	a := &Agent{cmd: "/usr/local/bin/kimi", displayName: "Kimi"}
	if a.SupportsUsage() {
		t.Fatal("non-Antigravity ACP backend must not report SupportsUsage")
	}
	if _, err := a.GetUsage(context.Background()); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("expected unsupported error, got %v", err)
	}
}
