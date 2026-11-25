package claudecode

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/chenhg5/cc-connect/core"
)

// Native /btw and /fork support for Claude Code.
//
// /btw uses the stream-json `side_question` control request: Claude Code
// answers from the live conversation (including a turn in progress) without
// adding the question or answer to the transcript, and the main turn keeps
// running. /fork resumes the source transcript with --fork-session, which
// writes the branch under a new session ID and leaves the source untouched.

// sideQuestionRequestPrefix namespaces our side_question request IDs so their
// control responses are routed here instead of the get_settings handler.
const sideQuestionRequestPrefix = "cc-side-question-"

// forkSessionIDPrefix marks a StartSession sessionID as "fork from this
// source". It is internal to this package and never persisted: the engine
// calls StartForkedSession, which builds it, and newClaudeSession consumes it.
const forkSessionIDPrefix = "\x00fork:"

var claudeSessionIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type sideQuestionReply struct {
	answer string
	err    error
}

var (
	_ core.SideQuestioner    = (*claudeSession)(nil)
	_ core.SideQuestionAgent = (*Agent)(nil)
	_ core.SessionForker     = (*Agent)(nil)
)

// SupportsSideQuestion implements core.SideQuestionAgent.
func (a *Agent) SupportsSideQuestion() bool { return true }

// StartForkedSession implements core.SessionForker.
func (a *Agent) StartForkedSession(ctx context.Context, sourceSessionID string) (core.AgentSession, error) {
	if !claudeSessionIDPattern.MatchString(sourceSessionID) {
		return nil, fmt.Errorf("claudecode: fork: invalid source session ID %q", sourceSessionID)
	}
	return a.StartSession(ctx, forkSessionIDPrefix+sourceSessionID)
}

// forkSourceFromSessionID decodes a sessionID produced by StartForkedSession.
func forkSourceFromSessionID(sessionID string) (string, bool) {
	if !strings.HasPrefix(sessionID, forkSessionIDPrefix) {
		return "", false
	}
	return strings.TrimPrefix(sessionID, forkSessionIDPrefix), true
}

// SideQuestion implements core.SideQuestioner using Claude Code's native
// side_question control request.
func (cs *claudeSession) SideQuestion(ctx context.Context, question string) (string, error) {
	question = strings.TrimSpace(question)
	if question == "" {
		return "", errors.New("claudecode: side question is empty")
	}
	if !cs.Alive() {
		return "", errors.New("claudecode: session is not running")
	}

	reply := make(chan sideQuestionReply, 1)
	cs.sideQuestionMu.Lock()
	cs.sideQuestionSeq++
	requestID := fmt.Sprintf("%s%d", sideQuestionRequestPrefix, cs.sideQuestionSeq)
	if cs.sideQuestions == nil {
		cs.sideQuestions = make(map[string]chan sideQuestionReply)
	}
	cs.sideQuestions[requestID] = reply
	cs.sideQuestionMu.Unlock()
	defer func() {
		cs.sideQuestionMu.Lock()
		delete(cs.sideQuestions, requestID)
		cs.sideQuestionMu.Unlock()
	}()

	if err := cs.writeJSON(map[string]any{
		"type":       "control_request",
		"request_id": requestID,
		"request":    map[string]any{"subtype": "side_question", "question": question},
	}); err != nil {
		return "", fmt.Errorf("claudecode: send side question: %w", err)
	}

	select {
	case r := <-reply:
		return r.answer, r.err
	case <-ctx.Done():
		// Best effort: ask the CLI to abandon the request. Delivery has
		// stopped either way.
		_ = cs.writeJSON(map[string]any{"type": "control_cancel_request", "request_id": requestID})
		return "", ctx.Err()
	case <-cs.done:
		return "", errors.New("claudecode: session exited before answering the side question")
	}
}

// deliverSideQuestion hands a side_question control response to its waiter.
// The response body is model output; it is never logged.
func (cs *claudeSession) deliverSideQuestion(requestID string, resp map[string]any) {
	cs.sideQuestionMu.Lock()
	ch := cs.sideQuestions[requestID]
	cs.sideQuestionMu.Unlock()
	if ch == nil {
		return // late response after timeout/cancel
	}
	var r sideQuestionReply
	switch subtype, _ := resp["subtype"].(string); subtype {
	case "success":
		payload, _ := resp["response"].(map[string]any)
		answer, _ := payload["response"].(string)
		if strings.TrimSpace(answer) == "" {
			r.err = errors.New("claudecode: side question returned no answer")
		} else {
			r.answer = answer
		}
	default:
		msg, _ := resp["error"].(string)
		if msg == "" {
			msg = "side question failed"
		}
		r.err = fmt.Errorf("claudecode: %s", msg)
	}
	select {
	case ch <- r:
	default:
	}
}
