package acp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

// Antigravity CLI (`agy acp`) extensions for /btw (SideQuestioner) and /fork
// (SessionForker).
//
// `agy acp` (v1.5.18) does not advertise sessionCapabilities.fork or a native
// side_question RPC, but it persists each conversation in a self-contained
// SQLite database under `<appDataDir>/conversations/<sessionID>.db` and loads
// any valid cloned DB via `session/resume` (even preserving prompt cache hits).
//
//   - /fork clones `<sourceID>.db` (atomically via SQLite VACUUM INTO) and
//     `<appDataDir>/brain/<sourceID>` to a fresh UUID, updates `trajectory_meta`,
//     and starts a new ACP session resuming that UUID. The source stays untouched.
//   - /btw clones `<liveID>.db` to a transient UUID and launches a separate,
//     strictly read-only `agy acp` subprocess (auto-denying all permission and
//     elicitation requests) so the live main turn keeps running uninterrupted;
//     all temporary files are deleted on exit.

var agySessionIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

var (
	_ core.SideQuestionAgent = (*Agent)(nil)
	_ core.SessionForker     = (*Agent)(nil)
	_ core.SideQuestioner    = (*acpSession)(nil)
)

type acpSideConfig struct {
	command       string
	args          []string
	extraEnv      []string
	authMethod    string
	agyAppDataDir string
}

// isAntigravityACP reports whether this ACP agent is backed by Antigravity CLI
// (`agy`), whose local SQLite conversation store supports lossless cloning for
// /fork and read-only /btw side questions.
func (a *Agent) isAntigravityACP() bool {
	a.mu.RLock()
	cmd := a.cmd
	display := a.displayName
	overrideDir := a.agyAppDataDir
	a.mu.RUnlock()

	if overrideDir != "" {
		return true
	}
	base := strings.ToLower(filepath.Base(cmd))
	baseNoExt := strings.TrimSuffix(base, filepath.Ext(base))
	if baseNoExt == "agy" || baseNoExt == "antigravity" {
		return true
	}
	if strings.Contains(strings.ToLower(display), "antigravity") {
		dir := a.resolveAgyDataDir()
		if st, err := os.Stat(filepath.Join(dir, "conversations")); err == nil && st.IsDir() {
			return true
		}
	}
	return false
}

// resolveAgyDataDir resolves the Antigravity CLI app data directory from
// `--gemini-dir`/`--app-data-dir` flags (falling back to `~/.gemini/antigravity-cli`).
func (a *Agent) resolveAgyDataDir() string {
	a.mu.RLock()
	if a.agyAppDataDir != "" {
		dir := a.agyAppDataDir
		a.mu.RUnlock()
		return dir
	}
	allArgs := append(append([]string{}, a.cliExtraArgs...), a.args...)
	extra := append([]string(nil), a.extraEnv...)
	extra = append(extra, a.sessionEnv...)
	a.mu.RUnlock()
	return resolveAgyAppDataDir(allArgs, extra)
}

func resolveAgyAppDataDir(args []string, extraEnv []string) string {
	geminiDir := ".gemini"
	appDataDir := "antigravity-cli"

	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--gemini-dir" || arg == "--gemini_dir":
			if i+1 < len(args) {
				geminiDir = args[i+1]
				i++
			}
		case strings.HasPrefix(arg, "--gemini-dir="):
			geminiDir = strings.TrimPrefix(arg, "--gemini-dir=")
		case strings.HasPrefix(arg, "--gemini_dir="):
			geminiDir = strings.TrimPrefix(arg, "--gemini_dir=")
		case arg == "--app-data-dir" || arg == "--app_data_dir":
			if i+1 < len(args) {
				appDataDir = args[i+1]
				i++
			}
		case strings.HasPrefix(arg, "--app-data-dir="):
			appDataDir = strings.TrimPrefix(arg, "--app-data-dir=")
		case strings.HasPrefix(arg, "--app_data_dir="):
			appDataDir = strings.TrimPrefix(arg, "--app_data_dir=")
		}
	}

	if filepath.IsAbs(appDataDir) {
		return appDataDir
	}
	home := envLookup(extraEnv, "HOME")
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	if filepath.IsAbs(geminiDir) {
		return filepath.Join(geminiDir, appDataDir)
	}
	return filepath.Join(home, geminiDir, appDataDir)
}

func envLookup(env []string, key string) string {
	prefix := key + "="
	for i := len(env) - 1; i >= 0; i-- {
		if strings.HasPrefix(env[i], prefix) {
			return strings.TrimPrefix(env[i], prefix)
		}
	}
	return ""
}

// SupportsSideQuestion implements core.SideQuestionAgent.
func (a *Agent) SupportsSideQuestion() bool {
	return a.isAntigravityACP()
}

// SupportsFork reports whether this ACP agent can fork sessions.
// Checked by core.Engine.cmdFork so non-Antigravity ACP backends fail fast
// with the standard "fork unsupported" message.
func (a *Agent) SupportsFork() bool {
	return a.isAntigravityACP()
}

// StartForkedSession implements core.SessionForker for Antigravity ACP.
func (a *Agent) StartForkedSession(ctx context.Context, sourceSessionID string) (core.AgentSession, error) {
	if !a.isAntigravityACP() {
		return nil, fmt.Errorf("acp: fork is not supported by this ACP backend")
	}
	if !agySessionIDPattern.MatchString(sourceSessionID) {
		return nil, fmt.Errorf("acp: fork: invalid source session ID %q", sourceSessionID)
	}
	appDataDir := a.resolveAgyDataDir()
	newID, err := cloneAgySessionDB(appDataDir, sourceSessionID, true)
	if err != nil {
		return nil, fmt.Errorf("acp: fork: %w", err)
	}

	sess, err := a.StartSession(ctx, newID)
	if err != nil {
		cleanupAgySessionFiles(appDataDir, newID)
		return nil, fmt.Errorf("acp: fork: start resumed session: %w", err)
	}
	// Guard against handshake silently falling back to session/new if resume failed.
	if got := sess.CurrentSessionID(); got != newID {
		_ = sess.Close()
		cleanupAgySessionFiles(appDataDir, newID)
		return nil, fmt.Errorf("acp: fork: backend did not resume cloned session (want %s, got %s)", newID, got)
	}
	slog.Info("acp: forked Antigravity session", "source", sourceSessionID, "fork", newID)
	return sess, nil
}

// cloneAgySessionDB creates an independent snapshot of an Antigravity SQLite
// session DB under a fresh UUID and updates its `trajectory_meta` IDs.
// Using SQLite `VACUUM INTO` guarantees a consistent snapshot even while the
// source session is actively writing in WAL mode.
func cloneAgySessionDB(appDataDir, sourceID string, copyBrain bool) (string, error) {
	if !agySessionIDPattern.MatchString(sourceID) {
		return "", fmt.Errorf("invalid source session ID %q", sourceID)
	}
	convDir := filepath.Join(appDataDir, "conversations")
	srcDB := filepath.Join(convDir, sourceID+".db")
	if _, err := os.Stat(srcDB); err != nil {
		return "", fmt.Errorf("source session db not found: %w", err)
	}

	newID := uuid.NewString()
	dstDB := filepath.Join(convDir, newID+".db")

	if err := snapshotSQLiteDB(srcDB, dstDB); err != nil {
		cleanupAgySessionFiles(appDataDir, newID)
		return "", fmt.Errorf("snapshot sqlite db: %w", err)
	}
	_ = os.Chmod(dstDB, 0o600)

	if err := rewriteAgyTrajectoryMeta(dstDB, newID); err != nil {
		cleanupAgySessionFiles(appDataDir, newID)
		return "", fmt.Errorf("update trajectory_meta: %w", err)
	}

	if copyBrain {
		srcBrain := filepath.Join(appDataDir, "brain", sourceID)
		dstBrain := filepath.Join(appDataDir, "brain", newID)
		if st, err := os.Stat(srcBrain); err == nil && st.IsDir() {
			if err := copyDirBestEffort(srcBrain, dstBrain); err != nil {
				slog.Warn("acp: fork: brain copy incomplete", "source", sourceID, "fork", newID, "error", err)
			}
		}
	}
	return newID, nil
}

func snapshotSQLiteDB(srcDB, dstDB string) error {
	// Open source read-only with busy_timeout so concurrent WAL writes from an
	// active turn do not fail immediately.
	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(5000)", filepath.ToSlash(srcDB))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	// VACUUM INTO creates a clean, self-contained single-file snapshot with all
	// committed WAL frames folded in.
	if _, err := db.Exec("VACUUM INTO ?", dstDB); err != nil {
		// Fallback to direct copy only if VACUUM INTO is unavailable (e.g.
		// read-only temp space restrictions).
		_ = os.Remove(dstDB)
		if copyErr := copyFile(srcDB, dstDB, 0o600); copyErr != nil {
			return fmt.Errorf("vacuum into (%v); fallback copy (%w)", err, copyErr)
		}
	}
	return nil
}

func rewriteAgyTrajectoryMeta(dstDB, newID string) error {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)", filepath.ToSlash(dstDB))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec("UPDATE trajectory_meta SET trajectory_id = ?, cascade_id = ?", newID, newID); err != nil {
		return err
	}
	return nil
}

func cleanupAgySessionFiles(appDataDir, sessionID string) {
	if !agySessionIDPattern.MatchString(sessionID) || appDataDir == "" {
		return
	}
	convBase := filepath.Join(appDataDir, "conversations", sessionID+".db")
	for _, p := range []string{
		convBase,
		convBase + "-wal",
		convBase + "-shm",
		filepath.Join(appDataDir, "annotations", sessionID+".pbtxt"),
		filepath.Join(appDataDir, "presence", sessionID+".lock"),
	} {
		_ = os.Remove(p)
	}
	_ = os.RemoveAll(filepath.Join(appDataDir, "brain", sessionID))

	// Best-effort remove transient entry from conversation_summaries.db so
	// ephemeral /btw sessions don't show up in session listings.
	summariesDB := filepath.Join(appDataDir, "conversation_summaries.db")
	if _, err := os.Stat(summariesDB); err == nil {
		dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(2000)", filepath.ToSlash(summariesDB))
		if db, err := sql.Open("sqlite", dsn); err == nil {
			_, _ = db.Exec("DELETE FROM conversation_summaries WHERE conversation_id = ?", sessionID)
			_, _ = db.Exec("DELETE FROM conversation_parents WHERE conversation_id = ?", sessionID)
			_ = db.Close()
		}
	}
}

func copyFile(src, dst string, mode fs.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func copyDirBestEffort(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".lock") {
			return nil
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		return copyFile(path, target, 0o600)
	})
}

const sideQuestionPromptPrefix = "[Side question — answer concisely from the conversation context so far. Do NOT call any tools, do NOT run shell commands, and do NOT modify or create any files.]\n\n"

// SideQuestion implements core.SideQuestioner by cloning the active Antigravity
// session DB into a transient read-only `agy acp` subprocess. Because it runs
// in its own process on a snapshot, it never blocks or mutates the live turn.
func (s *acpSession) SideQuestion(ctx context.Context, question string) (string, error) {
	question = strings.TrimSpace(question)
	if question == "" {
		return "", errors.New("acp: side question is empty")
	}
	if !s.Alive() {
		return "", errors.New("acp: session is not running")
	}
	sid := s.currentACPSessionID()
	if sid == "" {
		return "", errors.New("acp: no active session ID for side question")
	}
	if s.sideCfg.command == "" || s.sideCfg.agyAppDataDir == "" {
		return "", errors.New("acp: side question not configured for this session")
	}

	tmpID, err := cloneAgySessionDB(s.sideCfg.agyAppDataDir, sid, false)
	if err != nil {
		return "", fmt.Errorf("acp: side question clone: %w", err)
	}
	defer cleanupAgySessionFiles(s.sideCfg.agyAppDataDir, tmpID)

	return s.runReadOnlySideQuestion(ctx, tmpID, question)
}

func (s *acpSession) runReadOnlySideQuestion(ctx context.Context, tmpSessionID, question string) (string, error) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := exec.CommandContext(runCtx, s.sideCfg.command, s.sideCfg.args...)
	cmd.Dir = s.workDir
	cmd.Env = core.MergeEnv(os.Environ(), s.sideCfg.extraEnv)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return "", fmt.Errorf("acp: btw stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("acp: btw stdout pipe: %w", err)
	}
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("acp: btw start %s: %w", s.sideCfg.command, err)
	}

	var answerBuf strings.Builder
	var tr *transport
	tr = newTransport(stdout, stdin,
		func(method string, params json.RawMessage) {
			if method != "session/update" {
				return
			}
			for _, ev := range mapSessionUpdate(tmpSessionID, params) {
				if ev.Type == core.EventText && ev.Content != "" {
					answerBuf.WriteString(ev.Content)
				}
			}
		},
		func(method string, id json.RawMessage, params json.RawMessage) {
			switch method {
			case "session/request_permission":
				// Strictly read-only: automatically deny every tool permission.
				var p struct {
					Options []permissionOption `json:"options"`
				}
				_ = json.Unmarshal(params, &p)
				optID := pickPermissionOptionID(false, p.Options)
				_ = tr.respondSuccess(id, buildPermissionResult(false, optID))
			case "elicitation/create":
				_ = tr.respondSuccess(id, map[string]any{"action": "decline"})
			default:
				_ = tr.respondError(id, -32601, "btw: method not supported in read-only mode")
			}
		},
	)

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		tr.readLoop(runCtx)
	}()

	defer func() {
		cancel()
		_ = stdin.Close()
		waitDone := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(waitDone)
		}()
		select {
		case <-waitDone:
		case <-time.After(2 * time.Second):
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			<-waitDone
		}
		<-readDone
	}()

	// 1. initialize
	initRes, err := probeInitialize(runCtx, tr)
	if err != nil {
		if msg := strings.TrimSpace(stderrBuf.String()); msg != "" {
			return "", fmt.Errorf("%w (stderr: %s)", err, truncateForLog(msg, 200))
		}
		return "", err
	}

	// 2. optional authenticate
	if strings.TrimSpace(s.sideCfg.authMethod) != "" {
		if _, err := tr.call(runCtx, "authenticate", map[string]any{
			"methodId": s.sideCfg.authMethod,
		}); err != nil {
			return "", fmt.Errorf("acp: btw authenticate: %w", err)
		}
	}

	// 3. resume cloned snapshot (must succeed; never fall back to empty session)
	var resumeMethod string
	switch {
	case len(initRes.AgentCapabilities.SessionCapabilities.Resume) > 0:
		resumeMethod = "session/resume"
	case initRes.AgentCapabilities.LoadSession:
		resumeMethod = "session/load"
	default:
		return "", errors.New("acp: btw: backend does not support session resume/load")
	}
	if _, err := tr.call(runCtx, resumeMethod, map[string]any{
		"sessionId":  tmpSessionID,
		"cwd":        s.workDir,
		"mcpServers": []any{},
	}); err != nil {
		return "", fmt.Errorf("acp: btw %s: %w", resumeMethod, err)
	}

	// 4. best-effort mirror the live session's model
	if model := s.GetModel(); model != "" {
		_, _ = tr.call(runCtx, "session/set_config_option", map[string]any{
			"sessionId": tmpSessionID,
			"configId":  "model",
			"value":     model,
		})
	}

	// 5. send read-only side question prompt
	promptDone := make(chan error, 1)
	go func() {
		_, err := tr.call(runCtx, "session/prompt", map[string]any{
			"sessionId": tmpSessionID,
			"prompt": []any{
				map[string]any{"type": "text", "text": sideQuestionPromptPrefix + question},
			},
		})
		promptDone <- err
	}()

	select {
	case err := <-promptDone:
		if err != nil {
			return "", fmt.Errorf("acp: btw prompt: %w", err)
		}
	case <-ctx.Done():
		_ = tr.sendNotification("session/cancel", map[string]any{"sessionId": tmpSessionID})
		return "", ctx.Err()
	}

	answer := strings.TrimSpace(answerBuf.String())
	if answer == "" {
		return "", errors.New("acp: side question returned no answer")
	}
	return answer, nil
}
