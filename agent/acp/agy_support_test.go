package acp

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

func createTestAgyDB(t *testing.T, appDataDir, sessionID string) {
	t.Helper()
	convDir := filepath.Join(appDataDir, "conversations")
	if err := os.MkdirAll(convDir, 0o700); err != nil {
		t.Fatalf("mkdir conversations: %v", err)
	}
	dbPath := filepath.Join(convDir, sessionID+".db")
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbPath))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	stmts := []string{
		`CREATE TABLE trajectory_meta (trajectory_id TEXT PRIMARY KEY, cascade_id TEXT, trajectory_type INTEGER, source INTEGER)`,
		`CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER NOT NULL DEFAULT 0, payload TEXT)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
	if _, err := db.Exec(`INSERT INTO trajectory_meta VALUES (?, ?, 4, 17)`, "orig-traj-id", sessionID); err != nil {
		t.Fatalf("insert meta: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO steps VALUES (0, 1, 'hello world')`); err != nil {
		t.Fatalf("insert step: %v", err)
	}

	brainDir := filepath.Join(appDataDir, "brain", sessionID, ".system_generated", "logs")
	if err := os.MkdirAll(brainDir, 0o700); err != nil {
		t.Fatalf("mkdir brain: %v", err)
	}
	if err := os.WriteFile(filepath.Join(brainDir, "transcript.jsonl"), []byte(`{"step":0}`+"\n"), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
}

func TestResolveAgyAppDataDir(t *testing.T) {
	env := []string{"HOME=/tmp/testhome"}
	if got := resolveAgyAppDataDir(nil, env); got != "/tmp/testhome/.gemini/antigravity-cli" {
		t.Fatalf("default = %q", got)
	}
	if got := resolveAgyAppDataDir([]string{"acp", "--app-data-dir", "custom-agy", "--gemini-dir=.gem"}, env); got != "/tmp/testhome/.gem/custom-agy" {
		t.Fatalf("custom flags = %q", got)
	}
	if got := resolveAgyAppDataDir([]string{"acp", "--app_data_dir=/var/lib/agy"}, env); got != "/var/lib/agy" {
		t.Fatalf("absolute app_data_dir = %q", got)
	}
}

func TestIsAntigravityACP(t *testing.T) {
	agyAgent := &Agent{cmd: "/home/user/.local/bin/agy"}
	if !agyAgent.isAntigravityACP() || !agyAgent.SupportsSideQuestion() || !agyAgent.SupportsFork() {
		t.Fatal("expected agy command to enable Antigravity btw/fork support")
	}
	kimiAgent := &Agent{cmd: "/usr/local/bin/kimi", displayName: "Kimi"}
	if kimiAgent.isAntigravityACP() || kimiAgent.SupportsSideQuestion() || kimiAgent.SupportsFork() {
		t.Fatal("non-Antigravity ACP backend must not report Antigravity btw/fork support")
	}
}

func TestCloneAndCleanupAgySessionDB(t *testing.T) {
	appDir := t.TempDir()
	srcID := "11111111-2222-3333-4444-555555555555"
	createTestAgyDB(t, appDir, srcID)

	// Create a summaries DB to verify cleanup removes transient rows.
	sumPath := filepath.Join(appDir, "conversation_summaries.db")
	sumDB, err := sql.Open("sqlite", "file:"+filepath.ToSlash(sumPath))
	if err != nil {
		t.Fatalf("open summaries db: %v", err)
	}
	_, _ = sumDB.Exec(`CREATE TABLE conversation_summaries (conversation_id TEXT PRIMARY KEY)`)
	_, _ = sumDB.Exec(`CREATE TABLE conversation_parents (conversation_id TEXT PRIMARY KEY, parent_id TEXT)`)
	sumDB.Close()

	newID, err := cloneAgySessionDB(appDir, srcID, true)
	if err != nil {
		t.Fatalf("cloneAgySessionDB: %v", err)
	}
	if newID == srcID || !agySessionIDPattern.MatchString(newID) {
		t.Fatalf("unexpected cloned id %q", newID)
	}

	// Verify cloned DB has updated trajectory_meta and preserved steps.
	dstDB, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(appDir, "conversations", newID+".db")))
	if err != nil {
		t.Fatalf("open cloned db: %v", err)
	}
	var trajID, cascID string
	if err := dstDB.QueryRow(`SELECT trajectory_id, cascade_id FROM trajectory_meta`).Scan(&trajID, &cascID); err != nil {
		t.Fatalf("query cloned meta: %v", err)
	}
	if trajID != newID || cascID != newID {
		t.Fatalf("cloned meta = (%q, %q), want (%q, %q)", trajID, cascID, newID, newID)
	}
	var payload string
	if err := dstDB.QueryRow(`SELECT payload FROM steps WHERE idx = 0`).Scan(&payload); err != nil || payload != "hello world" {
		t.Fatalf("cloned step payload = %q (err=%v)", payload, err)
	}
	dstDB.Close()

	// Verify source DB was not modified.
	srcDB, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(appDir, "conversations", srcID+".db")))
	if err != nil {
		t.Fatalf("open src db: %v", err)
	}
	if err := srcDB.QueryRow(`SELECT trajectory_id, cascade_id FROM trajectory_meta`).Scan(&trajID, &cascID); err != nil {
		t.Fatalf("query src meta: %v", err)
	}
	if trajID != "orig-traj-id" || cascID != srcID {
		t.Fatalf("source meta mutated: (%q, %q)", trajID, cascID)
	}
	srcDB.Close()

	// Verify brain copy exists.
	if _, err := os.Stat(filepath.Join(appDir, "brain", newID, ".system_generated", "logs", "transcript.jsonl")); err != nil {
		t.Fatalf("cloned transcript missing: %v", err)
	}

	// Insert transient summary row and verify cleanup removes everything.
	sumDB, _ = sql.Open("sqlite", "file:"+filepath.ToSlash(sumPath))
	_, _ = sumDB.Exec(`INSERT INTO conversation_summaries VALUES (?)`, newID)
	sumDB.Close()

	cleanupAgySessionFiles(appDir, newID)
	if _, err := os.Stat(filepath.Join(appDir, "conversations", newID+".db")); !os.IsNotExist(err) {
		t.Fatalf("cloned db still exists after cleanup")
	}
	if _, err := os.Stat(filepath.Join(appDir, "brain", newID)); !os.IsNotExist(err) {
		t.Fatalf("cloned brain still exists after cleanup")
	}
	sumDB, _ = sql.Open("sqlite", "file:"+filepath.ToSlash(sumPath))
	var cnt int
	_ = sumDB.QueryRow(`SELECT count(*) FROM conversation_summaries WHERE conversation_id = ?`, newID).Scan(&cnt)
	sumDB.Close()
	if cnt != 0 {
		t.Fatalf("expected summary row deleted, got count=%d", cnt)
	}
}

// writeMockAgyACP writes a tiny python3 fake ACP server that supports
// initialize, session/resume, session/set_config_option, and session/prompt.
// When askPerm=true it issues a session/request_permission before replying so
// tests can verify SideQuestion auto-denies permissions.
func writeMockAgyACP(t *testing.T, askPerm bool, failResume bool) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "mock_agy.py")
	py := `#!/usr/bin/env python3
import json, sys

ask_perm = ` + map[bool]string{true: "True", false: "False"}[askPerm] + `
fail_resume = ` + map[bool]string{true: "True", false: "False"}[failResume] + `
resumed_id = None

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    m = msg.get("method")
    mid = msg.get("id")
    params = msg.get("params") or {}
    if m == "initialize":
        send({"jsonrpc": "2.0", "id": mid, "result": {
            "protocolVersion": 1,
            "agentCapabilities": {"loadSession": True, "sessionCapabilities": {"resume": {}}},
            "agentInfo": {"name": "mock-agy", "version": "1.0"}
        }})
    elif m == "session/resume":
        if fail_resume:
            send({"jsonrpc": "2.0", "id": mid, "error": {"code": -32000, "message": "cannot resume"}})
        else:
            resumed_id = params.get("sessionId")
            send({"jsonrpc": "2.0", "id": mid, "result": {"sessionId": resumed_id}})
    elif m == "session/new":
        send({"jsonrpc": "2.0", "id": mid, "result": {"sessionId": "fallback-fresh-id"}})
    elif m == "session/set_config_option":
        send({"jsonrpc": "2.0", "id": mid, "result": {"configOptions": []}})
    elif m == "session/prompt":
        perm_outcome = "none"
        if ask_perm:
            send({"jsonrpc": "2.0", "id": 99, "method": "session/request_permission", "params": {
                "sessionId": resumed_id,
                "toolCall": {"toolCallId": "tc1", "title": "run_command", "kind": "execute"},
                "options": [
                    {"optionId": "allow_once", "name": "Allow", "kind": "allow_once"},
                    {"optionId": "reject_once", "name": "Reject", "kind": "reject_once"}
                ]
            }})
            resp_line = sys.stdin.readline()
            resp_obj = json.loads(resp_line)
            perm_outcome = resp_obj.get("result", {}).get("outcome", {}).get("optionId", "unknown")
        send({"jsonrpc": "2.0", "method": "session/update", "params": {
            "sessionId": resumed_id,
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": f"side-answer sid={resumed_id} perm={perm_outcome}"}
            }
        }})
        send({"jsonrpc": "2.0", "id": mid, "result": {
            "stopReason": "end_turn",
            "usage": {
                "inputTokens": 120, "outputTokens": 15, "totalTokens": 135,
                "cachedReadTokens": 50, "cachedWriteTokens": 10, "thoughtTokens": 5
            }
        }})
`
	if err := os.WriteFile(script, []byte(py), 0o755); err != nil {
		t.Fatalf("write mock script: %v", err)
	}
	return script
}

func TestStartForkedSession_ClonesAndResumes(t *testing.T) {
	appDir := t.TempDir()
	srcID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	createTestAgyDB(t, appDir, srcID)

	script := writeMockAgyACP(t, false, false)
	agent := &Agent{
		workDir:       t.TempDir(),
		cmd:           "python3",
		args:          []string{script},
		agyAppDataDir: appDir,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess, err := agent.StartForkedSession(ctx, srcID)
	if err != nil {
		t.Fatalf("StartForkedSession: %v", err)
	}
	defer sess.Close()

	forkID := sess.CurrentSessionID()
	if forkID == "" || forkID == srcID || !agySessionIDPattern.MatchString(forkID) {
		t.Fatalf("unexpected fork session ID %q", forkID)
	}
	if _, err := os.Stat(filepath.Join(appDir, "conversations", forkID+".db")); err != nil {
		t.Fatalf("expected cloned db for forkID %s: %v", forkID, err)
	}
}

func TestStartForkedSession_CleansUpOnResumeFallback(t *testing.T) {
	appDir := t.TempDir()
	srcID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	createTestAgyDB(t, appDir, srcID)

	script := writeMockAgyACP(t, false, true)
	agent := &Agent{
		workDir:       t.TempDir(),
		cmd:           "python3",
		args:          []string{script},
		agyAppDataDir: appDir,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := agent.StartForkedSession(ctx, srcID); err == nil {
		t.Fatal("expected error when backend fell back to session/new")
	}
	// Only the original source DB should remain.
	entries, _ := os.ReadDir(filepath.Join(appDir, "conversations"))
	if len(entries) != 1 || entries[0].Name() != srcID+".db" {
		t.Fatalf("expected orphan cloned db cleaned up, got entries: %v", entries)
	}
}

func TestSideQuestion_ReadOnlyAndCleansUp(t *testing.T) {
	appDir := t.TempDir()
	srcID := "22222222-3333-4444-5555-666666666666"
	createTestAgyDB(t, appDir, srcID)

	script := writeMockAgyACP(t, true, false)
	s := &acpSession{
		workDir:   t.TempDir(),
		acpSessID: srcID,
		sideCfg: acpSideConfig{
			command:       "python3",
			args:          []string{script},
			agyAppDataDir: appDir,
		},
	}
	s.alive.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ans, err := s.SideQuestion(ctx, "what did we discuss?")
	if err != nil {
		t.Fatalf("SideQuestion: %v", err)
	}
	if !strings.Contains(ans, "side-answer") || !strings.Contains(ans, "perm=reject_once") {
		t.Fatalf("unexpected side answer (must auto-deny permission): %q", ans)
	}
	if strings.Contains(ans, srcID) {
		t.Fatalf("side question must run on a cloned session ID, not source %s: %q", srcID, ans)
	}
	// Transient DB must be deleted after SideQuestion returns.
	entries, _ := os.ReadDir(filepath.Join(appDir, "conversations"))
	if len(entries) != 1 || entries[0].Name() != srcID+".db" {
		t.Fatalf("expected transient btw db cleaned up, got: %v", entries)
	}
}

func TestSession_PromptAbsorbsFullUsage(t *testing.T) {
	tr, _, cancel := startACPScriptedServer(t, []any{
		map[string]any{
			"stopReason": "end_turn",
			"usage": map[string]any{
				"inputTokens":       1000,
				"outputTokens":      80,
				"totalTokens":       1080,
				"cachedReadTokens":  400,
				"cachedWriteTokens": 50,
				"thoughtTokens":     30,
			},
		},
	})
	defer cancel()

	s := newHandshakeTestSession(tr)
	s.alive.Store(true)
	s.setACPSessionID("sess-usage")
	s.onNotification("session/update", json.RawMessage(`{
		"sessionId": "sess-usage",
		"update": {"sessionUpdate": "usage_update", "used": 1080, "size": 1048576}
	}`))

	if err := s.Send("hi", "", nil, nil); err != nil {
		t.Fatalf("Send: %v", err)
	}
	u := s.GetContextUsage()
	if u == nil {
		t.Fatal("expected non-nil ContextUsage")
	}
	if u.InputTokens != 1000 || u.OutputTokens != 80 || u.TotalTokens != 1080 ||
		u.CachedInputTokens != 400 || u.CacheCreationInputTokens != 50 ||
		u.ReasoningOutputTokens != 30 || u.ContextWindow != 1048576 {
		t.Fatalf("unexpected ContextUsage: %+v", u)
	}
	var _ core.SideQuestioner = s
}
