package acp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/chenhg5/cc-connect/core"
	"github.com/creack/pty"
)

const (
	agyUsageFiveHourSeconds = 5 * 60 * 60
	agyUsageWeekSeconds     = 7 * 24 * 60 * 60
	agyUsagePollInterval    = 100 * time.Millisecond
	agyUsageStableFor       = 450 * time.Millisecond
	agyUsageActionGap       = 250 * time.Millisecond
	agyUsageCacheTTL        = 20 * time.Second
	agyUsageMinProbeBudget  = 4 * time.Second
)

var (
	_ core.UsageReporter = (*Agent)(nil)

	agyUsageAccountRe    = regexp.MustCompile(`(?i)^account:\s*(\S+)`)
	agyUsagePlanRe       = regexp.MustCompile(`\((Google AI [^()]+|Gemini Enterprise[^()]*|Antigravity [^()]+)\)`)
	agyUsageGroupRe      = regexp.MustCompile(`^[A-Z][A-Z0-9 &/+_-]*MODELS$`)
	agyUsagePctRe        = regexp.MustCompile(`(\d{1,3}(?:\.\d+)?)\s*%`)
	agyUsageRefreshRe    = regexp.MustCompile(`(?i)^refreshes?\s+in\s+(.+?)\s*$`)
	agyUsageDurPartRe    = regexp.MustCompile(`(?i)(\d+)\s*([dhms])`)
	agyUsageWhitespaceRe = regexp.MustCompile(`[ \t]+`)
	agyUsageRuleLineRe   = regexp.MustCompile(`^[\p{Zs}\-─━_=]{4,}$`)
)

type agyUsageCache struct {
	report    *core.UsageReport
	fetchedAt time.Time
}

type agyUsageProbeState struct {
	sentUsage      bool
	sentEnterRetry bool
	sentUsageRetry bool
	lastActionAt   time.Time
	usageSentAt    time.Time
	startedAt      time.Time
}

type agyLockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *agyLockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *agyLockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// SupportsUsage reports whether this ACP agent can fetch account-level quota
// via `GetUsage`. Only Antigravity CLI (`agy`) supports the interactive
// `/usage` panel probe; other ACP backends fall back cleanly.
func (a *Agent) SupportsUsage() bool {
	return a.isAntigravityACP()
}

// GetUsage implements core.UsageReporter for Antigravity ACP by running the
// documented interactive `agy` CLI `/usage` command inside a PTY and parsing
// its "Models & Quota" panel.
func (a *Agent) GetUsage(ctx context.Context) (*core.UsageReport, error) {
	if !a.isAntigravityACP() {
		return nil, fmt.Errorf("acp: account usage is not supported by this ACP backend")
	}

	a.usageCacheMu.Lock()
	cached := a.usageCache
	a.usageCacheMu.Unlock()
	if cached.report != nil && time.Since(cached.fetchedAt) < agyUsageCacheTTL {
		return cloneUsageReport(cached.report), nil
	}

	// Short-deadline callers (e.g. reply footer 1.5s budget) cannot wait for a
	// fresh PTY probe (~6s). Return a slightly older cached snapshot if we have
	// one rather than spawning a doomed subprocess on every turn.
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < agyUsageMinProbeBudget {
		if cached.report != nil && time.Since(cached.fetchedAt) < 5*time.Minute {
			return cloneUsageReport(cached.report), nil
		}
		return nil, context.DeadlineExceeded
	}

	screen, err := a.runAgyUsageProbe(ctx)
	if err != nil {
		return nil, err
	}
	report, err := parseAgyUsageReport(screen, time.Now())
	if err != nil {
		return nil, err
	}

	a.usageCacheMu.Lock()
	a.usageCache = agyUsageCache{report: cloneUsageReport(report), fetchedAt: time.Now()}
	a.usageCacheMu.Unlock()
	return report, nil
}

func cloneUsageReport(r *core.UsageReport) *core.UsageReport {
	if r == nil {
		return nil
	}
	cp := *r
	if len(r.Buckets) > 0 {
		cp.Buckets = make([]core.UsageBucket, len(r.Buckets))
		for i, b := range r.Buckets {
			cp.Buckets[i] = b
			if len(b.Windows) > 0 {
				cp.Buckets[i].Windows = append([]core.UsageWindow(nil), b.Windows...)
			}
		}
	}
	if r.Credits != nil {
		c := *r.Credits
		cp.Credits = &c
	}
	return &cp
}

func (a *Agent) runAgyUsageProbe(ctx context.Context) (string, error) {
	probeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	a.mu.RLock()
	command := a.cmd
	workDir := a.workDir
	extra := append([]string(nil), a.extraEnv...)
	extra = append(extra, a.sessionEnv...)
	a.mu.RUnlock()

	if workDir == "" {
		workDir = "."
	}

	cmd := exec.CommandContext(probeCtx, command)
	cmd.Dir = workDir
	env := core.MergeEnv(os.Environ(), extra)
	env = core.MergeEnv(env, []string{"TERM=xterm-256color"})
	cmd.Env = env

	var stderr agyLockedBuffer
	cmd.Stderr = &stderr

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: 60, Cols: 160})
	if err != nil {
		return "", fmt.Errorf("acp: start agy /usage probe: %w", err)
	}

	var waitErr error
	processDone := make(chan struct{})
	go func() {
		waitErr = cmd.Wait()
		close(processDone)
	}()

	terminal := newAgyUsageTerminal()
	readDone := make(chan error, 1)
	readerExited := make(chan struct{})
	go func() {
		defer close(readerExited)
		buf := make([]byte, 8192)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 {
				terminal.Write(buf[:n])
			}
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
					readDone <- nil
				} else {
					readDone <- err
				}
				return
			}
		}
	}()

	defer func() {
		_, _ = io.WriteString(ptmx, "\x1b\x03\x03")
		if err := ptmx.Close(); err != nil {
			slog.Debug("acp: close usage ptmx", "error", err)
		}
		cancel()
		<-readerExited
		select {
		case <-processDone:
		case <-time.After(2 * time.Second):
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			<-processDone
		}
	}()

	ticker := time.NewTicker(agyUsagePollInterval)
	defer ticker.Stop()

	now := time.Now()
	state := agyUsageProbeState{startedAt: now}
	var (
		lastScreen  string
		lastChange  = now
		usageScreen string
	)

	for {
		select {
		case <-probeCtx.Done():
			if usageScreen != "" {
				return usageScreen, nil
			}
			if screenErr := detectAgyUsageOutputError(lastScreen, stderr.String()); screenErr != nil {
				return "", screenErr
			}
			return "", fmt.Errorf("acp: timed out waiting for agy /usage panel: %w", probeCtx.Err())
		case err := <-readDone:
			if err != nil && usageScreen == "" {
				return "", fmt.Errorf("acp: read agy /usage output: %w", err)
			}
		case <-processDone:
			if usageScreen != "" {
				return usageScreen, nil
			}
			if screenErr := detectAgyUsageOutputError(lastScreen, stderr.String()); screenErr != nil {
				return "", screenErr
			}
			if waitErr != nil {
				return "", fmt.Errorf("acp: agy exited before /usage rendered: %w", waitErr)
			}
			return "", fmt.Errorf("acp: agy exited before /usage rendered")
		case <-ticker.C:
			screen := normalizeAgyUsageText(terminal.String())
			if screen != lastScreen {
				lastScreen = screen
				lastChange = time.Now()
			}
			if agyUsageReady(lastScreen) {
				usageScreen = lastScreen
			}
			if screenErr := detectAgyUsageOutputError(lastScreen, stderr.String()); screenErr != nil {
				return "", screenErr
			}
			if usageScreen != "" && time.Since(lastChange) >= agyUsageStableFor {
				return usageScreen, nil
			}
			if action := nextAgyUsageProbeAction(lastScreen, &state, time.Now()); action != "" {
				if _, err := io.WriteString(ptmx, action); err != nil {
					return "", fmt.Errorf("acp: write agy /usage probe input: %w", err)
				}
			}
		}
	}
}

func nextAgyUsageProbeAction(screen string, state *agyUsageProbeState, now time.Time) string {
	if now.Sub(state.lastActionAt) < agyUsageActionGap {
		return ""
	}
	if agyUsageReady(screen) {
		return ""
	}

	lower := strings.ToLower(screen)
	if !state.sentUsage {
		// Wait until agy finishes signing in and renders its main prompt.
		signedIn := strings.Contains(lower, "antigravity cli") &&
			!strings.Contains(lower, "signing in") &&
			(strings.Contains(screen, ">") || strings.Contains(lower, "? for shortcuts"))
		if !signedIn && now.Sub(state.startedAt) < 8*time.Second {
			return ""
		}
		state.sentUsage = true
		state.usageSentAt = now
		state.lastActionAt = now
		return "/usage\r"
	}

	if !state.sentEnterRetry && now.Sub(state.usageSentAt) >= 1200*time.Millisecond {
		state.sentEnterRetry = true
		state.lastActionAt = now
		return "\r"
	}

	if !state.sentUsageRetry && now.Sub(state.usageSentAt) >= 3*time.Second {
		state.sentUsageRetry = true
		state.lastActionAt = now
		return "\x15/usage\r"
	}

	return ""
}

func agyUsageReady(screen string) bool {
	lower := strings.ToLower(screen)
	return strings.Contains(lower, "models & quota") &&
		(strings.Contains(lower, "weekly limit remaining") || strings.Contains(lower, "five hour limit remaining")) &&
		agyUsagePctRe.MatchString(screen) &&
		(strings.Contains(lower, "refreshes in") || strings.Contains(lower, "quota available") || strings.Contains(lower, "limit reached"))
}

func detectAgyUsageOutputError(screen, stderr string) error {
	joined := strings.ToLower(strings.TrimSpace(screen + "\n" + stderr))
	switch {
	case joined == "":
		return nil
	case strings.Contains(joined, "antigravity blocked:"):
		return fmt.Errorf("acp: %s", strings.TrimSpace(stderr))
	case strings.Contains(joined, "currently not signed in") && !strings.Contains(joined, "signing in") && !strings.Contains(joined, "antigravity cli"):
		return fmt.Errorf("acp: agy is not signed in; run `agy acp --login`")
	default:
		return nil
	}
}

func normalizeAgyUsageText(raw string) string {
	raw = strings.ReplaceAll(raw, "\r", "\n")
	lines := strings.Split(raw, "\n")
	out := make([]string, 0, len(lines))
	lastBlank := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			if !lastBlank {
				out = append(out, "")
				lastBlank = true
			}
			continue
		}
		line = agyUsageWhitespaceRe.ReplaceAllString(line, " ")
		if agyUsageRuleLineRe.MatchString(line) {
			continue
		}
		out = append(out, line)
		lastBlank = false
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

func parseAgyUsageReport(text string, now time.Time) (*core.UsageReport, error) {
	text = normalizeAgyUsageText(text)
	if text == "" {
		return nil, fmt.Errorf("acp: agy /usage produced empty output")
	}
	if err := detectAgyUsageOutputError(text, ""); err != nil {
		return nil, err
	}

	report := &core.UsageReport{
		Provider: "antigravity",
	}

	lines := strings.Split(text, "\n")
	var (
		currentBucket *core.UsageBucket
		currentWindow *core.UsageWindow
	)
	flushWindow := func() {
		if currentBucket != nil && currentWindow != nil {
			currentBucket.Windows = append(currentBucket.Windows, *currentWindow)
			currentWindow = nil
		}
	}
	flushBucket := func() {
		flushWindow()
		if currentBucket != nil && len(currentBucket.Windows) > 0 {
			report.Buckets = append(report.Buckets, *currentBucket)
		}
		currentBucket = nil
	}

	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		if report.Email == "" {
			if m := agyUsageAccountRe.FindStringSubmatch(line); len(m) == 2 {
				report.Email = strings.TrimSpace(m[1])
				continue
			}
		}
		if report.Plan == "" {
			if m := agyUsagePlanRe.FindStringSubmatch(line); len(m) == 2 {
				report.Plan = strings.TrimSpace(m[1])
			}
		}

		// Stop parsing quota buckets once we hit the explanatory footer.
		if strings.HasPrefix(line, "│") || strings.Contains(strings.ToLower(line), "within each group") {
			flushBucket()
			continue
		}

		if agyUsageGroupRe.MatchString(line) {
			flushBucket()
			currentBucket = &core.UsageBucket{
				Name:    formatAgyGroupName(line),
				Allowed: true,
			}
			continue
		}

		lower := strings.ToLower(line)
		switch {
		case strings.HasPrefix(lower, "weekly limit remaining"):
			flushWindow()
			if currentBucket == nil {
				currentBucket = &core.UsageBucket{Name: "Antigravity", Allowed: true}
			}
			currentWindow = &core.UsageWindow{
				Name:          "Weekly limit",
				WindowSeconds: agyUsageWeekSeconds,
			}
			continue
		case strings.HasPrefix(lower, "five hour limit remaining") || strings.HasPrefix(lower, "5 hour limit remaining") || strings.HasPrefix(lower, "5-hour limit remaining"):
			flushWindow()
			if currentBucket == nil {
				currentBucket = &core.UsageBucket{Name: "Antigravity", Allowed: true}
			}
			currentWindow = &core.UsageWindow{
				Name:          "5-hour limit",
				WindowSeconds: agyUsageFiveHourSeconds,
			}
			continue
		}

		if currentWindow == nil {
			continue
		}
		if m := agyUsagePctRe.FindStringSubmatch(line); len(m) == 2 {
			if rem, err := strconv.ParseFloat(m[1], 64); err == nil {
				used := int(math.Round(100.0 - rem))
				if used < 0 {
					used = 0
				}
				if used > 100 {
					used = 100
				}
				currentWindow.UsedPercent = used
				if used >= 100 && currentBucket != nil {
					currentBucket.LimitReached = true
				}
			}
			continue
		}
		if m := agyUsageRefreshRe.FindStringSubmatch(line); len(m) == 2 {
			if dur := parseAgyDuration(m[1]); dur > 0 {
				secs := int(dur.Seconds())
				currentWindow.ResetAfterSeconds = secs
				currentWindow.ResetAtUnix = now.Add(dur).Unix()
			}
			continue
		}
	}
	flushBucket()

	if len(report.Buckets) == 0 {
		return nil, fmt.Errorf("acp: failed to parse quota buckets from agy /usage output")
	}
	return report, nil
}

func formatAgyGroupName(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "GEMINI MODELS":
		return "Gemini Models"
	case "CLAUDE AND GPT MODELS":
		return "Claude & GPT Models"
	default:
		return strings.TrimSpace(raw)
	}
}

func parseAgyDuration(raw string) time.Duration {
	var total time.Duration
	for _, m := range agyUsageDurPartRe.FindAllStringSubmatch(raw, -1) {
		n, err := strconv.Atoi(m[1])
		if err != nil || n < 0 {
			continue
		}
		switch strings.ToLower(m[2]) {
		case "d":
			total += time.Duration(n) * 24 * time.Hour
		case "h":
			total += time.Duration(n) * time.Hour
		case "m":
			total += time.Duration(n) * time.Minute
		case "s":
			total += time.Duration(n) * time.Second
		}
	}
	return total
}

// agyUsageTerminal is a minimal VT100/CSI screen buffer for rendering
// interactive `agy /usage` output cleanly.
type agyUsageTerminal struct {
	mu    sync.RWMutex
	lines [][]rune
	row   int
	col   int
}

const (
	agyMaxTermRows = 500
	agyMaxTermCols = 500
)

func newAgyUsageTerminal() *agyUsageTerminal {
	return &agyUsageTerminal{lines: [][]rune{nil}}
}

func (t *agyUsageTerminal) Write(p []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := 0; i < len(p); {
		switch p[i] {
		case 0x1b:
			next := t.consumeEscape(p[i:])
			if next <= 0 {
				i++
			} else {
				i += next
			}
		case '\r':
			t.col = 0
			i++
		case '\n':
			t.row++
			t.ensureRow(t.row)
			i++
		case '\b':
			if t.col > 0 {
				t.col--
			}
			i++
		case '\t':
			advance := 4 - (t.col % 4)
			for j := 0; j < advance; j++ {
				t.writeRune(' ')
			}
			i++
		default:
			if p[i] < 0x20 || p[i] == 0x7f {
				i++
				continue
			}
			r, size := utf8.DecodeRune(p[i:])
			if r == utf8.RuneError && size == 1 {
				i++
				continue
			}
			t.writeRune(r)
			i += size
		}
	}
}

func (t *agyUsageTerminal) String() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	lines := make([]string, 0, len(t.lines))
	for _, line := range t.lines {
		lines = append(lines, strings.TrimRight(string(line), " "))
	}
	return strings.Join(lines, "\n")
}

func (t *agyUsageTerminal) consumeEscape(p []byte) int {
	if len(p) < 2 {
		return len(p)
	}
	switch p[1] {
	case '[':
		for i := 2; i < len(p); i++ {
			if p[i] >= '@' && p[i] <= '~' {
				t.applyCSI(string(p[2:i]), p[i])
				return i + 1
			}
		}
		return len(p)
	case ']':
		for i := 2; i < len(p); i++ {
			if p[i] == '\a' {
				return i + 1
			}
			if p[i] == 0x1b && i+1 < len(p) && p[i+1] == '\\' {
				return i + 2
			}
		}
		return len(p)
	default:
		return 2
	}
}

func (t *agyUsageTerminal) applyCSI(params string, final byte) {
	switch final {
	case 'A':
		t.row -= parseAgyCSIInt(params, 1)
		if t.row < 0 {
			t.row = 0
		}
	case 'B':
		t.row += parseAgyCSIInt(params, 1)
		t.ensureRow(t.row)
	case 'C':
		t.col += parseAgyCSIInt(params, 1)
	case 'D':
		t.col -= parseAgyCSIInt(params, 1)
		if t.col < 0 {
			t.col = 0
		}
	case 'G':
		t.col = parseAgyCSIInt(params, 1) - 1
		if t.col < 0 {
			t.col = 0
		}
	case 'H', 'f':
		row, col := parseAgyCSICursor(params)
		if row < 0 {
			row = 0
		}
		if col < 0 {
			col = 0
		}
		t.row, t.col = row, col
		t.ensureRow(t.row)
	case 'J':
		if params == "2" || params == "" {
			t.lines = [][]rune{nil}
			t.row, t.col = 0, 0
		}
	case 'K':
		t.ensureRow(t.row)
		switch params {
		case "", "0":
			if t.col < len(t.lines[t.row]) {
				t.lines[t.row] = t.lines[t.row][:t.col]
			}
		case "2":
			t.lines[t.row] = nil
			t.col = 0
		}
	}
}

func (t *agyUsageTerminal) writeRune(r rune) {
	if t.row >= agyMaxTermRows || t.col >= agyMaxTermCols {
		return
	}
	t.ensureCell(t.row, t.col)
	if t.row < len(t.lines) && t.col < len(t.lines[t.row]) {
		t.lines[t.row][t.col] = r
	}
	t.col++
}

func (t *agyUsageTerminal) ensureRow(row int) {
	if row >= agyMaxTermRows {
		return
	}
	for len(t.lines) <= row {
		t.lines = append(t.lines, nil)
	}
}

func (t *agyUsageTerminal) ensureCell(row, col int) {
	if row >= agyMaxTermRows || col >= agyMaxTermCols {
		return
	}
	t.ensureRow(row)
	for len(t.lines[row]) <= col {
		t.lines[row] = append(t.lines[row], ' ')
	}
}

func parseAgyCSIInt(raw string, fallback int) int {
	raw = strings.TrimPrefix(raw, "?")
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func parseAgyCSICursor(raw string) (int, int) {
	raw = strings.TrimPrefix(raw, "?")
	if raw == "" {
		return 0, 0
	}
	parts := strings.Split(raw, ";")
	row := parseAgyCSIInt(parts[0], 1) - 1
	col := 0
	if len(parts) > 1 {
		col = parseAgyCSIInt(parts[1], 1) - 1
	}
	return row, col
}
