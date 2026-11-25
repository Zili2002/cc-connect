package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// /btw — transient side questions answered from the live conversation context
// without entering session history — and /fork — branch the current agent
// session into a new cc-connect session, leaving the source untouched.

const (
	// btwTimeout bounds one side question, including queue wait time spent
	// after it starts. The underlying model request may still finish and
	// consume usage after a timeout or cancel.
	btwTimeout = 3 * time.Minute
	// btwChunkLimit keeps each delivered piece comfortably inside platform
	// message limits (Slack renders ~4000 chars per message well).
	btwChunkLimit = 3500
)

// btwQueue serializes side questions per conversation key (one in flight,
// later ones queued FIFO) and tracks the active one so it can be cancelled.
type btwQueue struct {
	mu     sync.Mutex
	seq    uint64
	tails  map[string]chan struct{} // last enqueued run per key; closed when it finishes
	active map[string]*btwRun       // run currently executing per key
}

type btwRun struct {
	id        string
	cancel    context.CancelFunc
	cancelled atomic.Bool
}

func newBtwQueue() *btwQueue {
	return &btwQueue{
		tails:  make(map[string]chan struct{}),
		active: make(map[string]*btwRun),
	}
}

// enqueue schedules run for key after any earlier request for the same key.
// It returns the request ID and whether it had to wait behind another request.
// run receives a context bounded by timeout and a func reporting whether the
// request was cancelled through cancel().
func (q *btwQueue) enqueue(parent context.Context, key string, timeout time.Duration,
	run func(ctx context.Context, id string, cancelled func() bool)) (string, bool) {
	q.mu.Lock()
	q.seq++
	id := fmt.Sprintf("btw-%d", q.seq)
	prev := q.tails[key]
	done := make(chan struct{})
	q.tails[key] = done
	q.mu.Unlock()

	go func() {
		defer func() {
			q.mu.Lock()
			if q.tails[key] == done {
				delete(q.tails, key)
			}
			q.mu.Unlock()
			close(done)
		}()
		if prev != nil {
			select {
			case <-prev:
			case <-parent.Done():
				return
			}
		}
		ctx, cancel := context.WithTimeout(parent, timeout)
		r := &btwRun{id: id, cancel: cancel}
		q.mu.Lock()
		q.active[key] = r
		q.mu.Unlock()
		defer func() {
			cancel()
			q.mu.Lock()
			if q.active[key] == r {
				delete(q.active, key)
			}
			q.mu.Unlock()
		}()
		run(ctx, id, r.cancelled.Load)
	}()
	return id, prev != nil
}

// cancel stops delivery of the request currently running for key. Queued
// requests behind it still run. Returns the cancelled request ID.
func (q *btwQueue) cancel(key string) (string, bool) {
	q.mu.Lock()
	r := q.active[key]
	q.mu.Unlock()
	if r == nil || r.cancelled.Swap(true) {
		return "", false
	}
	r.cancel()
	return r.id, true
}

func agentSupportsSideQuestion(agent Agent) bool {
	sqa, ok := agent.(SideQuestionAgent)
	return ok && sqa.SupportsSideQuestion()
}

// liveSideQuestioner returns the live session for interactiveKey when it can
// answer side questions, or nil.
func (e *Engine) liveSideQuestioner(interactiveKey string) SideQuestioner {
	e.interactiveMu.Lock()
	state := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if state == nil || state.agentSession == nil || !state.agentSession.Alive() {
		return nil
	}
	sq, _ := state.agentSession.(SideQuestioner)
	return sq
}

// cmdBtw answers a side question from the live conversation without adding it
// to history. Agents without native side questions keep the legacy behavior
// where /btw is an alias of /ps (supplement the running task).
func (e *Engine) cmdBtw(p Platform, msg *Message, args []string) {
	agent, _, interactiveKey, err := e.commandContext(p, msg)
	if err != nil {
		e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgWsResolutionError, err))
		return
	}
	if !agentSupportsSideQuestion(agent) {
		e.cmdPs(p, msg, args)
		return
	}

	question := strings.TrimSpace(strings.Join(args, " "))
	if question == "" {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgBtwUsage))
		return
	}
	if strings.EqualFold(question, "cancel") {
		if id, ok := e.btwQueue.cancel(interactiveKey); ok {
			e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgBtwCancelled, id))
		} else {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgBtwNothingToCancel))
		}
		return
	}
	if e.liveSideQuestioner(interactiveKey) == nil {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgBtwNoSession))
		return
	}

	replyCtx := msg.ReplyCtx
	id, queued := e.btwQueue.enqueue(e.ctx, interactiveKey, btwTimeout,
		func(ctx context.Context, id string, cancelled func() bool) {
			// Re-resolve at run time: a queued question must follow the
			// session that is live when its turn comes (e.g. after /new).
			e.runBtw(ctx, p, replyCtx, e.liveSideQuestioner(interactiveKey), id, question, cancelled)
		})
	slog.Info("btw: side question enqueued", "session_key", msg.SessionKey, "request_id", id, "queued", queued)
	if queued {
		e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgBtwQueued, id))
	}
}

func (e *Engine) runBtw(ctx context.Context, p Platform, replyCtx any, sq SideQuestioner,
	id, question string, cancelled func() bool) {
	header := fmt.Sprintf("💬 **BTW** `%s`", id)
	handle := e.startBtwPlaceholder(p, replyCtx, header+"\n\n"+e.i18n.T(MsgBtwThinking))

	var body string
	if sq == nil {
		body = header + "\n\n" + e.i18n.T(MsgBtwNoSession)
	} else {
		start := time.Now()
		answer, err := sq.SideQuestion(ctx, question)
		answer = strings.TrimSpace(answer)
		switch {
		case cancelled():
			body = header + "\n\n" + e.i18n.T(MsgBtwCancelledNote)
		case errors.Is(err, context.DeadlineExceeded):
			body = header + "\n\n" + e.i18n.T(MsgBtwTimedOut)
		case err != nil:
			slog.Warn("btw: side question failed", "request_id", id, "error", err)
			body = header + "\n\n❌ " + err.Error()
		case answer == "":
			body = header + "\n\n❌ " + e.i18n.T(MsgBtwEmptyAnswer)
		default:
			body = header + " · " + e.i18n.T(MsgBtwNotInHistory) + "\n\n" + answer
		}
		slog.Info("btw: side question finished", "request_id", id,
			"elapsed", time.Since(start), "answer_len", len(answer), "ok", err == nil && !cancelled())
	}
	e.deliverBtw(p, replyCtx, handle, body)
}

// startBtwPlaceholder posts an editable "thinking" message when the platform
// supports in-place edits; returns nil otherwise.
func (e *Engine) startBtwPlaceholder(p Platform, replyCtx any, content string) any {
	starter, ok := p.(PreviewStarter)
	if !ok {
		return nil
	}
	if _, ok := p.(MessageUpdater); !ok {
		return nil
	}
	if err := e.waitOutgoing(p); err != nil {
		return nil
	}
	handle, err := starter.SendPreviewStart(e.ctx, replyCtx, content)
	if err != nil {
		slog.Warn("btw: placeholder send failed", "platform", p.Name(), "error", err)
		return nil
	}
	return handle
}

// deliverBtw replaces the placeholder with the first chunk (when possible)
// and sends any remaining chunks as follow-up messages.
func (e *Engine) deliverBtw(p Platform, replyCtx, handle any, body string) {
	chunks := SplitMessageCodeFenceAware(body, btwChunkLimit)
	if len(chunks) == 0 {
		return
	}
	next := 0
	if handle != nil {
		if updater, ok := p.(MessageUpdater); ok {
			if err := e.waitOutgoing(p); err == nil {
				if err := updater.UpdateMessage(e.ctx, handle, chunks[0]); err != nil {
					slog.Warn("btw: placeholder update failed", "platform", p.Name(), "error", err)
				} else {
					next = 1
				}
			}
		}
	}
	for _, chunk := range chunks[next:] {
		e.send(p, replyCtx, chunk)
	}
}

// cmdFork branches the current agent session into a new cc-connect session.
// The source session is left untouched and stays reachable via /list and
// /switch. The branch is materialized by the agent on the next message.
func (e *Engine) cmdFork(p Platform, msg *Message, args []string) {
	agent, sessions, interactiveKey, err := e.commandContext(p, msg)
	if err != nil {
		e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgWsResolutionError, err))
		return
	}
	if _, ok := agent.(SessionForker); !ok {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgForkUnsupported))
		return
	}
	session := sessions.GetOrCreateActive(msg.SessionKey)
	if session.Busy() {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgForkBusy))
		return
	}

	source := ""
	e.interactiveMu.Lock()
	if st := e.interactiveStates[interactiveKey]; st != nil && st.agentSession != nil {
		source = st.agentSession.CurrentSessionID()
	}
	e.interactiveMu.Unlock()
	if source == "" || source == ContinueSession {
		source = session.GetAgentSessionID()
	}
	if source == "" || source == ContinueSession {
		// Forking a fork that has not taken its first turn yet: branch from
		// the same source.
		source = session.GetForkFrom()
	}
	if source == "" || source == ContinueSession {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgForkNothing))
		return
	}

	e.cleanupInteractiveState(interactiveKey)

	name := strings.TrimSpace(strings.Join(args, " "))
	if name == "" {
		name = defaultForkName(session.GetName(), source)
	}
	fork := sessions.NewSession(msg.SessionKey, name)
	fork.SetForkFrom(source)
	sessions.Save()
	slog.Info("fork: created branch session", "session_key", msg.SessionKey,
		"source_session_id", source, "name", name)
	e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgForkCreated, name, shortSessionID(source)))
}

func defaultForkName(baseName, sourceID string) string {
	switch strings.TrimSpace(baseName) {
	case "", "session", "default":
		return "fork-" + shortSessionID(sourceID)
	default:
		return baseName + " (fork)"
	}
}

func shortSessionID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// startAgentSessionMaybeFork starts the agent session for a cc-connect session
// that has no agent session yet but carries a pending /fork source. It returns
// handled=false when no fork applies, so the caller proceeds with a normal start.
// On fork failure it clears the marker, starts fresh and tells the user.
func (e *Engine) startAgentSessionMaybeFork(agent Agent, session *Session, sessions *SessionManager,
	p Platform, replyCtx any, sessionKey string) (AgentSession, bool) {
	source := session.GetForkFrom()
	if source == "" || session.GetAgentSessionID() != "" {
		return nil, false
	}
	forker, ok := agent.(SessionForker)
	if ok {
		if validator, isValidator := agent.(SessionIDValidator); isValidator && !validator.ValidateSessionID(e.ctx, source) {
			slog.Warn("fork: source session not found in this project, starting fresh",
				"session_key", sessionKey, "source_session_id", source)
			ok = false
		}
	}
	if !ok {
		session.SetForkFrom("")
		sessions.Save()
		e.send(p, replyCtx, e.i18n.T(MsgForkStartFailed))
		return nil, false
	}
	start := time.Now()
	agentSession, err := forker.StartForkedSession(e.ctx, source)
	if err != nil {
		slog.Error("fork: starting forked session failed, starting fresh",
			"session_key", sessionKey, "source_session_id", source, "error", err)
		session.SetForkFrom("")
		sessions.Save()
		e.send(p, replyCtx, e.i18n.T(MsgForkStartFailed))
		return nil, false
	}
	slog.Info("fork: forked session started", "session_key", sessionKey,
		"source_session_id", source, "elapsed", time.Since(start))
	return agentSession, true
}
