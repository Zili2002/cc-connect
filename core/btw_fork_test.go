package core

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ── fakes ────────────────────────────────────────────────────

// btwAgent supports native side questions and /fork.
type btwAgent struct {
	stubAgent
	mu         sync.Mutex
	forkedFrom []string
	forkErr    error
	session    AgentSession
}

func (a *btwAgent) SupportsSideQuestion() bool { return true }
func (a *btwAgent) StartSession(_ context.Context, _ string) (AgentSession, error) {
	if a.session != nil {
		return a.session, nil
	}
	return &stubAgentSession{}, nil
}
func (a *btwAgent) StartForkedSession(_ context.Context, src string) (AgentSession, error) {
	a.mu.Lock()
	a.forkedFrom = append(a.forkedFrom, src)
	a.mu.Unlock()
	if a.forkErr != nil {
		return nil, a.forkErr
	}
	return a.StartSession(context.Background(), "")
}
func (a *btwAgent) getForkedFrom() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.forkedFrom...)
}

// sideQSession answers side questions; optionally blocks until released.
type sideQSession struct {
	stubAgentSession
	id      string
	answer  string
	release chan struct{}
	asked   chan string
}

func (s *sideQSession) CurrentSessionID() string { return s.id }
func (s *sideQSession) SideQuestion(ctx context.Context, q string) (string, error) {
	if s.asked != nil {
		s.asked <- q
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return s.answer + q, nil
}

// previewPlatform supports editable placeholders.
type previewPlatform struct {
	stubPlatformEngine
	mu      sync.Mutex
	started []string
	updates []string
}

func (p *previewPlatform) SendPreviewStart(_ context.Context, _ any, content string) (any, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.started = append(p.started, content)
	return len(p.started), nil
}
func (p *previewPlatform) UpdateMessage(_ context.Context, _ any, content string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.updates = append(p.updates, content)
	return nil
}
func (p *previewPlatform) getUpdates() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.updates...)
}

func installLiveSession(e *Engine, key string, s AgentSession, p Platform) {
	e.interactiveMu.Lock()
	e.interactiveStates[key] = &interactiveState{agentSession: s, platform: p}
	e.interactiveMu.Unlock()
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func sentContains(p interface{ getSent() []string }, sub string) bool {
	for _, s := range p.getSent() {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// ── builtin registration ─────────────────────────────────────

func TestBtwAndForkAreSeparateBuiltins(t *testing.T) {
	for in, want := range map[string]string{"btw": "btw", "ps": "ps", "fork": "fork"} {
		if got := matchPrefix(in, builtinCommands); got != want {
			t.Fatalf("matchPrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

// ── /btw ─────────────────────────────────────────────────────

func TestCmdBtw_AgentWithoutSideQuestionsKeepsPsBehavior(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	e.cmdBtw(p, &Message{SessionKey: "test:u1", ReplyCtx: "ctx"}, []string{"hello"})
	if !sentContains(p, e.i18n.T(MsgPsNoSession)) {
		t.Fatalf("expected legacy /ps reply, got %v", p.getSent())
	}
}

func TestCmdBtw_UsageAndNoSession(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &btwAgent{}, []Platform{p}, "", LangEnglish)
	msg := &Message{SessionKey: "test:u1", ReplyCtx: "ctx"}
	e.cmdBtw(p, msg, nil)
	e.cmdBtw(p, msg, []string{"why?"})
	e.cmdBtw(p, msg, []string{"cancel"})
	for _, k := range []MsgKey{MsgBtwUsage, MsgBtwNoSession, MsgBtwNothingToCancel} {
		if !sentContains(p, e.i18n.T(k)) {
			t.Fatalf("missing %s reply in %v", k, p.getSent())
		}
	}
}

func TestCmdBtw_AnswerSentWithoutTouchingHistory(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &btwAgent{}, []Platform{p}, "", LangEnglish)
	key := "test:u1"
	installLiveSession(e, key, &sideQSession{answer: "A: "}, p)

	e.cmdBtw(p, &Message{SessionKey: key, ReplyCtx: "ctx"}, []string{"what", "now?"})
	waitUntil(t, "btw answer", func() bool { return sentContains(p, "A: what now?") })
	if !sentContains(p, e.i18n.T(MsgBtwNotInHistory)) || !sentContains(p, "btw-1") {
		t.Fatalf("answer missing header/note: %v", p.getSent())
	}
	if h := e.sessions.GetOrCreateActive(key).History; len(h) != 0 {
		t.Fatalf("side question leaked into history: %v", h)
	}
}

func TestCmdBtw_ReplacesPlaceholderWhenPlatformCanEdit(t *testing.T) {
	p := &previewPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", &btwAgent{}, []Platform{p}, "", LangEnglish)
	key := "test:u1"
	installLiveSession(e, key, &sideQSession{answer: "A: "}, p)

	e.cmdBtw(p, &Message{SessionKey: key, ReplyCtx: "ctx"}, []string{"q"})
	waitUntil(t, "placeholder update", func() bool { return len(p.getUpdates()) == 1 })
	if !strings.Contains(p.getUpdates()[0], "A: q") {
		t.Fatalf("placeholder not replaced with answer: %v", p.getUpdates())
	}
	if p.started[0] == "" || !strings.Contains(p.started[0], e.i18n.T(MsgBtwThinking)) {
		t.Fatalf("placeholder not shown: %v", p.started)
	}
}

func TestCmdBtw_QueuesPerConversationAndCancels(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &btwAgent{}, []Platform{p}, "", LangEnglish)
	key := "test:u1"
	s := &sideQSession{answer: "A: ", release: make(chan struct{}), asked: make(chan string, 4)}
	installLiveSession(e, key, s, p)
	msg := &Message{SessionKey: key, ReplyCtx: "ctx"}

	e.cmdBtw(p, msg, []string{"first"})
	if q := <-s.asked; q != "first" {
		t.Fatalf("asked %q", q)
	}
	e.cmdBtw(p, msg, []string{"second"})
	if !sentContains(p, "btw-2") {
		t.Fatalf("second request not reported as queued: %v", p.getSent())
	}
	select {
	case q := <-s.asked:
		t.Fatalf("second question ran concurrently: %q", q)
	case <-time.After(50 * time.Millisecond):
	}

	e.cmdBtw(p, msg, []string{"cancel"})
	waitUntil(t, "cancel note", func() bool { return sentContains(p, e.i18n.T(MsgBtwCancelledNote)) })
	if q := <-s.asked; q != "second" {
		t.Fatalf("queued question did not run after cancel, got %q", q)
	}
	close(s.release)
	waitUntil(t, "second answer", func() bool { return sentContains(p, "A: second") })
	if sentContains(p, "A: first") {
		t.Fatal("cancelled answer was still delivered")
	}
}

func TestBtwQueue_TimeoutIsReported(t *testing.T) {
	q := newBtwQueue()
	done := make(chan error, 1)
	q.enqueue(context.Background(), "k", 20*time.Millisecond, func(ctx context.Context, _ string, _ func() bool) {
		<-ctx.Done()
		done <- ctx.Err()
	})
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got %v", err)
	}
}

// ── /fork ────────────────────────────────────────────────────

func TestCmdFork_Guards(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	msg := &Message{SessionKey: "test:u1", ReplyCtx: "ctx"}

	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	e.cmdFork(p, msg, nil)
	if !sentContains(p, e.i18n.T(MsgForkUnsupported)) {
		t.Fatalf("expected unsupported, got %v", p.getSent())
	}

	e2 := NewEngine("test", &btwAgent{}, []Platform{p}, "", LangEnglish)
	e2.cmdFork(p, msg, nil)
	if !sentContains(p, e2.i18n.T(MsgForkNothing)) {
		t.Fatalf("expected nothing-to-fork, got %v", p.getSent())
	}

	s := e2.sessions.GetOrCreateActive(msg.SessionKey)
	s.SetAgentSessionID("11111111-1111-1111-1111-111111111111", "stub")
	if _, ok := s.TryLock(); !ok {
		t.Fatal("TryLock")
	}
	defer s.Unlock(0)
	e2.cmdFork(p, msg, nil)
	if !sentContains(p, e2.i18n.T(MsgForkBusy)) {
		t.Fatalf("expected busy, got %v", p.getSent())
	}
}

func TestCmdFork_CreatesPendingBranchFromLiveSession(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &btwAgent{}, []Platform{p}, "", LangEnglish)
	key := "test:u1"
	orig := e.sessions.GetOrCreateActive(key)
	orig.SetAgentSessionID("stored-id", "stub")
	// The live process may have moved on to a newer ID than the stored one.
	installLiveSession(e, key, &sideQSession{id: "live-id"}, p)

	e.cmdFork(p, &Message{SessionKey: key, ReplyCtx: "ctx"}, []string{"experiment"})

	fork := e.sessions.GetOrCreateActive(key)
	if fork == orig {
		t.Fatal("fork did not create a new active session")
	}
	if fork.GetForkFrom() != "live-id" || fork.GetAgentSessionID() != "" || fork.GetName() != "experiment" {
		t.Fatalf("fork = from %q id %q name %q", fork.GetForkFrom(), fork.GetAgentSessionID(), fork.GetName())
	}
	if orig.GetAgentSessionID() != "stored-id" {
		t.Fatalf("source session was modified: %q", orig.GetAgentSessionID())
	}
	e.interactiveMu.Lock()
	_, stillLive := e.interactiveStates[key]
	e.interactiveMu.Unlock()
	if stillLive {
		t.Fatal("source process should be released after /fork")
	}
	if !sentContains(p, "experiment") || !sentContains(p, "live-id") {
		t.Fatalf("missing fork confirmation: %v", p.getSent())
	}

	// Forking again before the branch took a turn branches from the same source.
	e.cmdFork(p, &Message{SessionKey: key, ReplyCtx: "ctx"}, nil)
	if got := e.sessions.GetOrCreateActive(key).GetForkFrom(); got != "live-id" {
		t.Fatalf("fork of pending fork: source %q", got)
	}
}

func TestStartAgentSessionMaybeFork(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	a := &btwAgent{}
	e := NewEngine("test", a, []Platform{p}, "", LangEnglish)
	s := e.sessions.GetOrCreateActive("test:u1")

	if _, handled := e.startAgentSessionMaybeFork(a, s, e.sessions, p, "ctx", "test:u1"); handled {
		t.Fatal("no pending fork must not be handled")
	}
	s.SetForkFrom("src-1")
	if sess, handled := e.startAgentSessionMaybeFork(a, s, e.sessions, p, "ctx", "test:u1"); !handled || sess == nil {
		t.Fatal("pending fork not started")
	}
	if got := a.getForkedFrom(); len(got) != 1 || got[0] != "src-1" {
		t.Fatalf("forked from %v", got)
	}
	if s.GetForkFrom() != "src-1" {
		t.Fatal("marker must stay until the branch reports its own ID")
	}

	a.forkErr = errors.New("boom")
	if _, handled := e.startAgentSessionMaybeFork(a, s, e.sessions, p, "ctx", "test:u1"); handled {
		t.Fatal("failed fork must fall back to a normal start")
	}
	if s.GetForkFrom() != "" || !sentContains(p, e.i18n.T(MsgForkStartFailed)) {
		t.Fatalf("failed fork: marker %q, sent %v", s.GetForkFrom(), p.getSent())
	}
}

// echoIDSession reports a new agent session ID once it has taken a turn,
// like a Claude Code fork.
type echoIDSession struct {
	stubAgentSession
	events chan Event
	id     atomic.Value
}

func (s *echoIDSession) Events() <-chan Event { return s.events }
func (s *echoIDSession) CurrentSessionID() string {
	v, _ := s.id.Load().(string)
	return v
}
func (s *echoIDSession) Send(prompt, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.id.Store("branch-id")
	s.events <- Event{Type: EventResult, Content: "echo: " + prompt, Done: true}
	return nil
}

func TestFork_EndToEndBranchTakesOverOnFirstMessage(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	a := &btwAgent{session: &echoIDSession{events: make(chan Event, 4)}}
	e := NewEngine("test", a, []Platform{p}, "", LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })
	const key = "test:u1"
	src := e.sessions.GetOrCreateActive(key)
	src.SetAgentSessionID("source-id", "stub")
	send := func(content string) {
		e.ReceiveMessage(p, &Message{SessionKey: key, Platform: p.Name(), UserID: "u1", Content: content, ReplyCtx: "ctx"})
	}

	send("/fork")
	waitUntil(t, "fork reply", func() bool { return sentContains(p, "source-") })
	send("hello branch")
	waitUntil(t, "branch answer", func() bool { return sentContains(p, "echo: hello branch") })

	if got := a.getForkedFrom(); len(got) != 1 || got[0] != "source-id" {
		t.Fatalf("branch not started from source: %v", got)
	}
	branch := e.sessions.GetOrCreateActive(key)
	waitUntil(t, "branch ID recorded", func() bool { return branch.GetAgentSessionID() == "branch-id" })
	if branch.GetForkFrom() != "" {
		t.Fatalf("fork marker not cleared: %q", branch.GetForkFrom())
	}
	if src.GetAgentSessionID() != "source-id" {
		t.Fatalf("source changed to %q", src.GetAgentSessionID())
	}
}

// ── session persistence ──────────────────────────────────────

func TestSession_ForkMarkerPersistsAndClearsOnBind(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	sm := NewSessionManager(path)
	s := sm.NewSession("k", "branch")
	s.SetForkFrom("src")
	sm.Save()

	reloaded := NewSessionManager(path).GetOrCreateActive("k")
	if reloaded.GetForkFrom() != "src" {
		t.Fatalf("pending fork lost across restart: %q", reloaded.GetForkFrom())
	}
	reloaded.SetAgentSessionID(ContinueSession, "stub")
	if reloaded.GetForkFrom() != "src" {
		t.Fatal("ContinueSession sentinel must not clear the marker")
	}
	if !reloaded.CompareAndSetAgentSessionID("new-id", "stub") || reloaded.GetForkFrom() != "" {
		t.Fatalf("binding a real ID must clear the marker: %q", reloaded.GetForkFrom())
	}
}
