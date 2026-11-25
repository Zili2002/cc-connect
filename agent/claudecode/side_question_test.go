package claudecode

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// TestSideQuestionHelperProcess is a fake Claude Code CLI used by the tests
// below. Like the real CLI it reports its session_id only once the first user
// turn arrives; a session started with --fork-session reports a new ID derived
// from the --resume source. side_question control requests are answered per
// CC_SIDEQ_MODE: "" (answer), "error" (error response) or "hang" (no reply).
func TestSideQuestionHelperProcess(t *testing.T) {
	if os.Getenv("CC_SIDEQ_HELPER") != "1" {
		return
	}
	resume, fork := "", false
	for i, a := range os.Args {
		switch a {
		case "--fork-session":
			fork = true
		case "--resume":
			if i+1 < len(os.Args) {
				resume = os.Args[i+1]
			}
		}
	}
	sid := "fresh-session"
	switch {
	case fork:
		sid = "forked-from-" + resume
	case resume != "":
		sid = resume
	}
	write := func(v any) {
		b, _ := json.Marshal(v)
		_, _ = os.Stdout.Write(append(b, '\n'))
	}
	initSent := false
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var msg map[string]any
		if json.Unmarshal(scanner.Bytes(), &msg) != nil {
			continue
		}
		switch msg["type"] {
		case "user":
			if !initSent {
				write(map[string]any{"type": "system", "subtype": "init", "session_id": sid})
				initSent = true
			}
			write(map[string]any{"type": "result", "subtype": "success", "is_error": false, "num_turns": 1,
				"result": "ok", "session_id": sid, "usage": map[string]any{"input_tokens": 1, "output_tokens": 1}})
		case "control_request":
			req, _ := msg["request"].(map[string]any)
			if req["subtype"] != "side_question" {
				continue // e.g. get_settings: unsupported here, cc-connect must not care
			}
			id := msg["request_id"]
			switch os.Getenv("CC_SIDEQ_MODE") {
			case "hang":
			case "error":
				write(map[string]any{"type": "control_response", "response": map[string]any{
					"subtype": "error", "request_id": id, "error": "side_question: boom"}})
			default:
				q, _ := req["question"].(string)
				write(map[string]any{"type": "control_response", "response": map[string]any{
					"subtype": "success", "request_id": id,
					"response": map[string]any{"response": "side: " + q, "synthetic": false}}})
			}
		}
	}
	os.Exit(0)
}

func startSideQuestionHelper(t *testing.T, sessionID, mode string) *claudeSession {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cs, err := newClaudeSession(ctx, t.TempDir(), os.Args[0],
		[]string{"-test.run=^TestSideQuestionHelperProcess$", "--"},
		"", "", "", sessionID, "default", "", "", nil, nil, nil,
		[]string{"CC_SIDEQ_HELPER=1", "CC_SIDEQ_MODE=" + mode},
		"", false, core.SpawnOptions{}, 0, 0, "", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func waitForResult(t *testing.T, cs *claudeSession) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case ev, ok := <-cs.Events():
			if !ok {
				t.Fatal("session exited before result")
			}
			if ev.Type == core.EventResult {
				return
			}
		case <-timeout:
			t.Fatal("timed out waiting for result")
		}
	}
}

func TestSideQuestion_RoundTripWithoutTouchingTurns(t *testing.T) {
	cs := startSideQuestionHelper(t, "", "")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	answer, err := cs.SideQuestion(ctx, "  what is the codename?  ")
	if err != nil {
		t.Fatal(err)
	}
	if answer != "side: what is the codename?" {
		t.Fatalf("answer = %q", answer)
	}
	// A normal turn still works afterwards on the same process.
	if err := cs.Send("hello", "m1", nil, nil); err != nil {
		t.Fatal(err)
	}
	waitForResult(t, cs)

	// Concurrent side questions are routed to their own callers.
	errs := make(chan error, 2)
	for _, q := range []string{"one", "two"} {
		q := q
		go func() {
			got, err := cs.SideQuestion(ctx, q)
			if err == nil && got != "side: "+q {
				err = errors.New("misrouted answer " + got + " for " + q)
			}
			errs <- err
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestSideQuestion_ErrorResponse(t *testing.T) {
	cs := startSideQuestionHelper(t, "", "error")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cs.SideQuestion(ctx, "q"); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected boom error, got %v", err)
	}
}

func TestSideQuestion_TimeoutAndEmpty(t *testing.T) {
	cs := startSideQuestionHelper(t, "", "hang")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := cs.SideQuestion(ctx, "q"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if _, err := cs.SideQuestion(context.Background(), "   "); err == nil {
		t.Fatal("expected error for empty question")
	}
	cs.sideQuestionMu.Lock()
	pending := len(cs.sideQuestions)
	cs.sideQuestionMu.Unlock()
	if pending != 0 {
		t.Fatalf("waiter leaked: %d pending", pending)
	}
}

func TestSideQuestion_LateResponseIgnored(t *testing.T) {
	cs := &claudeSession{}
	// No waiter registered: must not panic or block.
	cs.deliverSideQuestion(sideQuestionRequestPrefix+"99", map[string]any{"subtype": "success"})
}

func TestForkedSession_SpawnsFromSourceAndReportsNewID(t *testing.T) {
	const src = "96c02d06-840c-4ad7-ab3a-7632dea8888b"
	cs := startSideQuestionHelper(t, forkSessionIDPrefix+src, "")
	if got := cs.CurrentSessionID(); got != "" {
		t.Fatalf("fork must not report an ID before its first turn, got %q", got)
	}
	if err := cs.Send("continue on the branch", "m1", nil, nil); err != nil {
		t.Fatal(err)
	}
	waitForResult(t, cs)
	if got, want := cs.CurrentSessionID(), "forked-from-"+src; got != want {
		t.Fatalf("CurrentSessionID = %q, want %q (fork flags not passed?)", got, want)
	}
}

func TestStartForkedSession_RejectsNonUUID(t *testing.T) {
	a := &Agent{}
	for _, bad := range []string{"", "--dangerously-skip-permissions", "abc", forkSessionIDPrefix + "x"} {
		if _, err := a.StartForkedSession(context.Background(), bad); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
}

func TestForkSourceFromSessionID(t *testing.T) {
	if src, ok := forkSourceFromSessionID(forkSessionIDPrefix + "abc"); !ok || src != "abc" {
		t.Fatalf("got %q %v", src, ok)
	}
	for _, s := range []string{"", "abc", core.ContinueSession} {
		if _, ok := forkSourceFromSessionID(s); ok {
			t.Fatalf("%q must not decode as fork", s)
		}
	}
	if !(&Agent{}).SupportsSideQuestion() {
		t.Fatal("claudecode must advertise side questions")
	}
}
