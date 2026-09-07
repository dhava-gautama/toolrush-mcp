package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// mustJSON parses a tool envelope string; tests assert on the map.
func mustJSON(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, s)
	}
	return m
}

func statInt(t *testing.T, key string) int {
	t.Helper()
	statsMu.Lock()
	defer statsMu.Unlock()
	return stats[key]
}

// ---------------------------------------------------------------- splitLines

func TestSplitLines(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"plain", "a\nb\nc", []string{"a", "b", "c"}},
		{"trailing newline", "a\nb\n", []string{"a", "b"}},
		{"CRLF normalized", "a\r\nb\r\nc\r\n", []string{"a", "b", "c"}},
		{"mixed CRLF and LF", "a\r\nb\nc\r\n", []string{"a", "b", "c"}},
		{"BOM stripped", "\xef\xbb\xbf" + "fab\n", []string{"fab"}},
		{"BOM only", "\xef\xbb\xbf", []string{""}},
		{"single line no newline", "solo", []string{"solo"}},
		{"empty", "", []string{""}},
		{"lone CRLF", "\r\n", []string{""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitLines([]byte(tt.in))
			if len(got) != len(tt.want) {
				t.Fatalf("splitLines(%q) = %q, want %q", tt.in, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("splitLines(%q) = %q, want %q", tt.in, got, tt.want)
				}
			}
		})
	}
}

// ------------------------------------------------------------------ render

func TestRender(t *testing.T) {
	lines3 := []string{"alpha", "beta", "gamma"}

	t.Run("normal page with gutter", func(t *testing.T) {
		m := mustJSON(t, render(lines3, 16, 1, 2))
		if m["success"] != true {
			t.Fatalf("success=false: %v", m)
		}
		if m["content"] != "1\talpha\n2\tbeta" {
			t.Fatalf("content = %q", m["content"])
		}
		if m["total_lines"] != float64(3) || m["file_size"] != float64(16) {
			t.Fatalf("meta = %v", m)
		}
	})

	t.Run("tail mode negative offset", func(t *testing.T) {
		m := mustJSON(t, render(lines3, 16, -2, 10))
		if m["content"] != "2\tbeta\n3\tgamma" {
			t.Fatalf("content = %q", m["content"])
		}
	})

	t.Run("tail clamped to start", func(t *testing.T) {
		m := mustJSON(t, render(lines3, 16, -99, 10))
		if m["content"] != "1\talpha\n2\tbeta\n3\tgamma" {
			t.Fatalf("content = %q", m["content"])
		}
	})

	t.Run("beyond EOF hint", func(t *testing.T) {
		m := mustJSON(t, render(lines3, 16, 99, 10))
		hint, _ := m["hint"].(string)
		if !strings.Contains(hint, "beyond the end of the file") {
			t.Fatalf("hint = %q", hint)
		}
		if m["content"] != "" {
			t.Fatalf("content = %q", m["content"])
		}
	})

	t.Run("genuinely empty", func(t *testing.T) {
		for _, lines := range [][]string{nil, {""}} {
			m := mustJSON(t, render(lines, 0, 1, 10))
			hint, _ := m["hint"].(string)
			if !strings.Contains(hint, "File is empty") {
				t.Fatalf("hint = %q for lines %q", hint, lines)
			}
			if m["total_lines"] != float64(0) {
				t.Fatalf("total_lines = %v", m["total_lines"])
			}
		}
	})

	t.Run("size zero with content (proc pseudo-file fix)", func(t *testing.T) {
		m := mustJSON(t, render([]string{"x", "y"}, 0, 1, 10))
		if m["content"] != "1\tx\n2\ty" {
			t.Fatalf("content = %q", m["content"])
		}
		if _, has := m["hint"]; has {
			t.Fatalf("unexpected hint on size-0 content render: %v", m["hint"])
		}
	})

	t.Run("maxReadBytes clipping sets hint", func(t *testing.T) {
		big := make([]string, 0, 1200)
		for i := 0; i < 1200; i++ {
			big = append(big, strings.Repeat("x", 100)+strconv.Itoa(i))
		}
		m := mustJSON(t, render(big, 140000, 1, 1200))
		hint, _ := m["hint"].(string)
		if !strings.HasPrefix(hint, "Use offset=") {
			t.Fatalf("hint = %q", hint)
		}
		if m["truncated"] != true {
			t.Fatalf("truncated = %v", m["truncated"])
		}
		content, _ := m["content"].(string)
		if strings.Contains(content, "1199") {
			t.Fatalf("clipped content still holds last line")
		}
		if m["total_lines"] != float64(1200) {
			t.Fatalf("total_lines = %v", m["total_lines"])
		}
	})

	t.Run("per-line clamp at 2000 chars", func(t *testing.T) {
		long := strings.Repeat("y", 2500)
		m := mustJSON(t, render([]string{long}, 2500, 1, 1))
		content, _ := m["content"].(string)
		if !strings.Contains(content, "... [truncated]") {
			t.Fatalf("content missing clamp marker (len %d)", len(content))
		}
		// 4-char gutter + clamped line + marker
		want := "1\t" + strings.Repeat("y", 2000) + "... [truncated]"
		if content != want {
			t.Fatalf("content len = %d, want %d", len(content), len(want))
		}
	})
}

// -------------------------------------------------------------- collectHits

// JSON message builders for the rg --json stream under test.
func jmatch(path string, no int, text string) string {
	return fmt.Sprintf(`{"type":"match","data":{"path":{"text":%q},`+
		`"lines":{"text":%q},"line_number":%d}}`, path, text, no)
}
func jctx(no int, text string) string {
	return fmt.Sprintf(`{"type":"context","data":{"lines":{"text":%q},`+
		`"line_number":%d}}`, text, no)
}
func jbegin(path string) string {
	return fmt.Sprintf(`{"type":"begin","data":{"path":{"text":%q}}}`, path)
}

func TestCollectHits(t *testing.T) {
	type hit struct {
		Path    string `json:"path"`
		Line    int    `json:"line"`
		Content string `json:"content"`
	}
	parse := func(t *testing.T, s string) (map[string]any, []hit) {
		t.Helper()
		m := mustJSON(t, s)
		var hits []hit
		b, _ := json.Marshal(m["hits"])
		if err := json.Unmarshal(b, &hits); err != nil {
			t.Fatalf("hits not parseable: %v", err)
		}
		return m, hits
	}

	t.Run("plain hit", func(t *testing.T) {
		_, hits := parse(t, collectHitsJSON(jmatch("a/b.txt", 3, "hello world\n"), "rg", 10, 0, 0, false))
		if len(hits) != 1 || hits[0].Path != "a/b.txt" ||
			hits[0].Line != 3 || hits[0].Content != "hello world" {
			t.Fatalf("hits = %+v", hits)
		}
	})

	t.Run("colon in path", func(t *testing.T) {
		_, hits := parse(t, collectHitsJSON(jmatch("co:lon/f.txt", 1, "needle\n"), "rg", 10, 0, 0, false))
		if len(hits) != 1 {
			t.Fatalf("hits = %+v", hits)
		}
		if hits[0].Path != "co:lon/f.txt" || hits[0].Line != 1 ||
			hits[0].Content != "needle" {
			t.Fatalf("hit = %+v", hits[0])
		}
	})

	t.Run("unparseable rows do not count", func(t *testing.T) {
		// with --json, garbage rows are not hits at all (the old text
		// parser counted every non-empty row toward total_hits)
		m, hits := parse(t, collectHitsJSON("garbage\n"+jmatch("x/y.txt", 2, "real\n"), "rg", 10, 0, 0, false))
		if len(hits) != 1 || hits[0].Path != "x/y.txt" {
			t.Fatalf("hits = %+v", hits)
		}
		if m["total_hits"] != float64(1) {
			t.Fatalf("total_hits = %v", m["total_hits"])
		}
	})

	t.Run("offset and limit windowing", func(t *testing.T) {
		out := jmatch("f", 1, "a\n") + "\n" + jmatch("f", 2, "b\n") + "\n" +
			jmatch("f", 3, "c\n") + "\n" + jmatch("f", 4, "d\n")
		m, hits := parse(t, collectHitsJSON(out, "rg", 2, 1, 0, false))
		if len(hits) != 2 || hits[0].Line != 2 || hits[1].Line != 3 {
			t.Fatalf("hits = %+v", hits)
		}
		if m["total_hits"] != float64(4) || m["shown"] != float64(2) {
			t.Fatalf("meta = %v", m)
		}
		if m["truncated"] != true {
			t.Fatalf("truncated = %v", m["truncated"])
		}
	})

	t.Run("truncated flag when hits exceed limit", func(t *testing.T) {
		m, hits := parse(t, collectHitsJSON(jmatch("f", 1, "a\n")+"\n"+jmatch("f", 2, "b\n"), "rg", 1, 0, 0, false))
		if len(hits) != 1 || m["truncated"] != true {
			t.Fatalf("hits = %+v truncated = %v", hits, m["truncated"])
		}
	})

	t.Run("empty output", func(t *testing.T) {
		m, hits := parse(t, collectHitsJSON("", "rg", 10, 0, 0, false))
		if len(hits) != 0 || m["total_hits"] != float64(0) {
			t.Fatalf("m = %v", m)
		}
	})

	t.Run("overlong content clamped", func(t *testing.T) {
		long := strings.Repeat("z", 2500)
		_, hits := parse(t, collectHitsJSON(jmatch("f", 1, long+"\n"), "rg", 10, 0, 0, false))
		if !strings.HasSuffix(hits[0].Content, "... [truncated]") {
			t.Fatalf("content not clamped (len %d)", len(hits[0].Content))
		}
	})

	t.Run("CRLF terminator stripped once", func(t *testing.T) {
		_, hits := parse(t, collectHitsJSON(jmatch("f", 1, "hello\r\n"), "rg", 10, 0, 0, false))
		if hits[0].Content != "hello" {
			t.Fatalf("content = %q", hits[0].Content)
		}
	})

	t.Run("capped flag always present", func(t *testing.T) {
		m, _ := parse(t, collectHitsJSON(jmatch("f", 1, "a\n"), "rg", 10, 0, 0, false))
		if c, ok := m["capped"]; !ok || c != false {
			t.Fatalf("capped = %v (missing or not false)", m["capped"])
		}
		m2, _ := parse(t, collectHitsJSON(jmatch("f", 1, "a\n"), "rg", 10, 0, 0, true))
		if m2["capped"] != true {
			t.Fatalf("capped = %v, want true", m2["capped"])
		}
	})
}

// ---------------------------------------------------------------------- shq

func TestShq(t *testing.T) {
	if got := shq("simple"); got != "'simple'" {
		t.Fatalf("shq = %q", got)
	}
	if got := shq("a'b"); got != "'a'\\''b'" {
		t.Fatalf("shq = %q", got)
	}
	// round-trip through bash itself
	if got := shq("'; rm -rf /; '"); got != "''\\''; rm -rf /; '\\'''" {
		t.Fatalf("shq = %q", got)
	}
}

// ----------------------------------------------------------- fastRead clamp

func TestFastReadClamp(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f.txt")
	if err := os.WriteFile(p, []byte("a\nb\nc\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// offset=0 must mean "page 1", not a panic or an empty window.
	m := mustJSON(t, fastRead(p, 0, 10))
	if m["success"] != true {
		t.Fatalf("envelope = %v", m)
	}
	if m["content"] != "1\ta\n2\tb\n3\tc" {
		t.Fatalf("content = %q", m["content"])
	}
}

// Regression: dispatchTool must derive isError from the envelope via
// envIsError — a missing path (success=false) sets MCP isError.
// Exposed by python/smoke.py ("isError: fast_read missing path").
func TestFastReadMissingPathIsError(t *testing.T) {
	args, _ := json.Marshal(map[string]any{"path": filepath.Join(t.TempDir(), "nope.txt")})
	_, isErr, unknown := dispatchTool("fast_read", args)
	if unknown {
		t.Fatalf("fast_read reported unknown tool")
	}
	if !isErr {
		t.Fatalf("missing path must set MCP isError")
	}
}

// --------------------------------------------------------------- cache budget

func TestCacheBudget(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "big.txt")
	// > cacheFileMax (4MiB): content served but never cached.
	var sb strings.Builder
	for i := 0; sb.Len() <= 5<<20; i++ {
		sb.WriteString(fmt.Sprintf("line-%06d-%s\n", i, strings.Repeat("p", 90)))
	}
	if err := os.WriteFile(p, []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	before := statInt(t, "cache_hits")
	m1 := mustJSON(t, fastRead(p, 1, 5))
	m2 := mustJSON(t, fastRead(p, 1, 5))
	if after := statInt(t, "cache_hits"); after != before {
		t.Fatalf("cache_hits moved %d -> %d for a >4MiB file", before, after)
	}
	c1, _ := m1["content"].(string)
	c2, _ := m2["content"].(string)
	if c1 == "" || c1 != c2 || !strings.Contains(c1, "1\tline-000000") {
		t.Fatalf("content mismatch:\n%q\n%q", c1, c2)
	}
}

// --------------------------------------------------------- warm shell lifecycle

func fdCount(t *testing.T) int {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("/proc unavailable: %v", err)
	}
	return len(ents)
}

func zombieChildren(t *testing.T) []string {
	ents, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var z []string
	self := strconv.Itoa(os.Getpid())
	for _, e := range ents {
		name := e.Name()
		if _, err := strconv.Atoi(name); err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + name + "/stat")
		if err != nil {
			continue // process exited mid-scan
		}
		s := string(b)
		// comm may contain spaces/parens: fields after the LAST ')'
		i := strings.LastIndexByte(s, ')')
		if i < 0 {
			continue
		}
		f := strings.Fields(s[i+1:])
		// f[0]=state f[1]=ppid
		if len(f) > 1 && f[0] == "Z" && f[1] == self {
			z = append(z, name)
		}
	}
	return z
}

func waitFor(t *testing.T, what string, d time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestWarmShellLifecycle(t *testing.T) {
	ws := &warmShell{}
	to := 15 * time.Second

	t.Run("echo hi rc 0", func(t *testing.T) {
		out, rc, _ := ws.run("echo hi", "", to)
		if rc != 0 || strings.TrimSpace(out) != "hi" {
			t.Fatalf("rc=%d out=%q", rc, out)
		}
	})

	t.Run("cd persistence via marker readback", func(t *testing.T) {
		if _, rc, _ := ws.run("cd /tmp", "", to); rc != 0 {
			t.Fatalf("cd rc=%d", rc)
		}
		out, rc, _ := ws.run("pwd", "", to)
		if rc != 0 || strings.TrimSpace(out) != "/tmp" {
			t.Fatalf("rc=%d out=%q", rc, out)
		}
		if ws.cwd != "/tmp" {
			t.Fatalf("tracked cwd = %q", ws.cwd)
		}
	})

	t.Run("bare exit kills shell then respawn works", func(t *testing.T) {
		out, rc, _ := ws.run("exit 3", "", to)
		if rc != -1 {
			t.Fatalf("rc=%d out=%q, want -1 (shell died)", rc, out)
		}
		out, rc, _ = ws.run("echo back", "", to)
		if rc != 0 || strings.TrimSpace(out) != "back" {
			t.Fatalf("after respawn rc=%d out=%q", rc, out)
		}
	})

	t.Run("timeout kills tree and next call gets fresh shell", func(t *testing.T) {
		start := time.Now()
		out, rc, _ := ws.run("sleep 30", "", 1*time.Second)
		if rc != 124 {
			t.Fatalf("rc=%d out=%q, want 124", rc, out)
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("timeout took %v", elapsed)
		}
		if !strings.Contains(out, "timed out") {
			t.Fatalf("out missing timeout notice: %q", out)
		}
		out, rc, _ = ws.run("echo ok", "", to)
		if rc != 0 || strings.TrimSpace(out) != "ok" {
			t.Fatalf("after timeout rc=%d out=%q", rc, out)
		}
	})

	t.Run("fd and zombie stability across respawns", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("fd/zombie assertions are linux-only")
		}
		base := fdCount(t)
		for i := 0; i < 10; i++ {
			if _, rc, _ := ws.run("exit 1", "", to); rc != -1 {
				t.Fatalf("cycle %d: bare exit rc=%d", i, rc)
			}
			if out, rc, _ := ws.run("echo cycle", "", to); rc != 0 ||
				strings.TrimSpace(out) != "cycle" {
				t.Fatalf("cycle %d: rc=%d out=%q", i, rc, out)
			}
		}
		// the reaper goroutine closes the pipe and Wait()s asynchronously —
		// give it a moment, then require the fd count to settle at baseline.
		waitFor(t, "fd count to settle", 3*time.Second, func() bool {
			d := fdCount(t) - base
			return d >= -2 && d <= 2
		})
		// and no defunct children of the test process may linger
		waitFor(t, "zombie reaping", 3*time.Second, func() bool {
			return len(zombieChildren(t)) == 0
		})
	})

	// leave no shell behind for the rest of the suite
	ws.mu.Lock()
	ws.killLocked()
	ws.mu.Unlock()
}

// --------------------------------------------------------------------- fuzz

func FuzzSplitLines(f *testing.F) {
	f.Add([]byte("a\nb\nc\n"))
	f.Add([]byte("\xef\xbb\xbfbom\r\ncrlf\r\n"))
	f.Add([]byte(""))
	f.Add([]byte("no trailing newline"))
	f.Add([]byte("a\r\nb\nc\r\n\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		lines := splitLines(data)
		if lines == nil {
			t.Fatalf("splitLines returned nil")
		}
		// invariants: no output line may contain a newline, and no CRLF
		// pair may survive (lone CRs are legitimately preserved).
		for _, l := range lines {
			if strings.ContainsAny(l, "\n") {
				t.Fatalf("line contains newline: %q", l)
			}
			if strings.Contains(l, "\r\n") {
				t.Fatalf("CRLF survived split: %q", l)
			}
		}
		if !strings.Contains(string(data), "\r") {
			for _, l := range lines {
				if strings.Contains(l, "\r") {
					t.Fatalf("stray CR without any CR in input: %q", l)
				}
			}
		}
	})
}

func FuzzCollectHits(f *testing.F) {
	f.Add(jmatch("a/b.txt", 3, "hello\n"))
	f.Add(jmatch("co:lon/f.txt", 1, "needle\n"))
	f.Add("garbage line\n" + jmatch("f", 2, "x\n") + "\n")
	f.Add("")
	f.Add(":::\n1:2:3:4:5\n")
	f.Add(jbegin("f") + "\n" + jctx(1, "c\n") + jmatch("f", 2, "m\n"))
	f.Fuzz(func(t *testing.T, data string) {
		limit := 1 + len(data)%37
		offset := len(data) % 11
		out := collectHitsJSON(data, "fuzz", limit, offset, 1, false)
		var m map[string]any
		if err := json.Unmarshal([]byte(out), &m); err != nil {
			t.Fatalf("collectHitsJSON emitted invalid JSON: %v\n%q", err, out)
		}
	})
}

// ------------------------------------------------------- rg context parsing

func parseCtxHits(t *testing.T, s string, limit, offset int) []searchHit {
	t.Helper()
	m := mustJSON(t, collectHitsJSON(s, "rg", limit, offset, 1, false))
	var hits []searchHit
	b, _ := json.Marshal(m["hits"])
	if err := json.Unmarshal(b, &hits); err != nil {
		t.Fatalf("hits not parseable: %v", err)
	}
	return hits
}

func TestCollectHitsJSONContext(t *testing.T) {
	t.Run("before and after gutter", func(t *testing.T) {
		out := jbegin("f.txt") + "\n" + jctx(9, "before\n") + "\n" +
			jmatch("f.txt", 10, "match\n") + "\n" + jctx(11, "after\n")
		hits := parseCtxHits(t, out, 10, 0)
		if len(hits) != 1 || hits[0].Line != 10 || hits[0].Content != "match" {
			t.Fatalf("hits = %+v", hits)
		}
		if hits[0].Context != "9-before\n11+after" {
			t.Fatalf("context = %q", hits[0].Context)
		}
	})

	t.Run("new file (begin) opens a new group", func(t *testing.T) {
		// rows following a begin belong to the FOLLOWING match even when a
		// match from the previous file is still the "last" hit
		out := jbegin("f") + "\n" + jmatch("f", 1, "a\n") + "\n" +
			jctx(2, "gap\n") + "\n" + jbegin("g") + "\n" +
			jctx(8, "near\n") + "\n" + jmatch("g", 9, "b\n")
		hits := parseCtxHits(t, out, 10, 0)
		if len(hits) != 2 {
			t.Fatalf("hits = %+v", hits)
		}
		if hits[0].Context != "2+gap" {
			t.Fatalf("hit0 context = %q", hits[0].Context)
		}
		if hits[1].Context != "8-near" {
			t.Fatalf("hit1 context = %q", hits[1].Context)
		}
	})

	t.Run("context rows only counted for matches", func(t *testing.T) {
		out := jctx(1, "x\n") + "\n" + jmatch("f", 2, "m1\n") + "\n" +
			jctx(3, "y\n") + "\n" + jctx(8, "z\n") + "\n" +
			jmatch("f", 9, "m2\n") + "\n" + jctx(10, "w\n")
		m := mustJSON(t, collectHitsJSON(out, "rg", 10, 0, 2, false))
		if m["total_hits"] != float64(2) || m["shown"] != float64(2) {
			t.Fatalf("meta = %v", m)
		}
	})

	t.Run("offset skips matches and their context", func(t *testing.T) {
		out := jmatch("f", 1, "a\n") + "\n" + jctx(2, "x\n") + "\n" +
			jbegin("f") + "\n" + jmatch("f", 9, "b\n") + "\n" + jctx(10, "y\n")
		hits := parseCtxHits(t, out, 10, 1)
		if len(hits) != 1 || hits[0].Line != 9 || hits[0].Context != "10+y" {
			t.Fatalf("hits = %+v", hits)
		}
	})

	t.Run("multiple matches share one group gutter", func(t *testing.T) {
		// rg prints a row between two close matches once, positioned after
		// the first match -> it becomes after-context of the preceding hit
		out := jmatch("f", 1, "a\n") + "\n" + jctx(2, "shared\n") + "\n" +
			jmatch("f", 3, "b\n") + "\n" + jctx(4, "tail\n")
		hits := parseCtxHits(t, out, 10, 0)
		if len(hits) != 2 {
			t.Fatalf("hits = %+v", hits)
		}
		if hits[0].Context != "2+shared" {
			t.Fatalf("hit0 context = %q", hits[0].Context)
		}
		if hits[1].Context != "4+tail" {
			t.Fatalf("hit1 context = %q", hits[1].Context)
		}
	})

	t.Run("no context field when a hit has none", func(t *testing.T) {
		out := jmatch("f", 1, "a\n") + "\n" + jbegin("g") + "\n" + jmatch("g", 9, "b\n")
		hits := parseCtxHits(t, out, 10, 0)
		if len(hits) != 2 || hits[0].Context != "" || hits[1].Context != "" {
			t.Fatalf("hits = %+v", hits)
		}
		b, err := json.Marshal(hits[0])
		if err != nil || strings.Contains(string(b), "context") {
			t.Fatalf("context key must be omitted: %s", b)
		}
	})

	t.Run("timestamps with colons in context rows parse exactly", func(t *testing.T) {
		// the ambiguity class that motivated --json: "12:00:01 INFO boot"
		// must stay one context row with an exact line number
		out := jbegin("log") + "\n" + jctx(4, "12:00:01 INFO boot\n") + "\n" +
			jmatch("log", 5, "needle\n") + "\n" + jctx(6, "12:00:02 INFO up\n")
		hits := parseCtxHits(t, out, 10, 0)
		if len(hits) != 1 {
			t.Fatalf("hits = %+v", hits)
		}
		if hits[0].Context != "4-12:00:01 INFO boot\n6+12:00:02 INFO up" {
			t.Fatalf("context = %q", hits[0].Context)
		}
	})
}

func TestClampCtx(t *testing.T) {
	for in, want := range map[int]int{-3: 0, -1: 0, 0: 0, 1: 1, 5: 5, 6: 5, 99: 5} {
		if got := clampCtx(in); got != want {
			t.Fatalf("clampCtx(%d) = %d, want %d", in, got, want)
		}
	}
}

// ---------------------------------------------------------------- fast_tree

func treeNames(t *testing.T, s string) []string {
	t.Helper()
	m := mustJSON(t, s)
	var entries []treeEntry
	b, _ := json.Marshal(m["entries"])
	if err := json.Unmarshal(b, &entries); err != nil {
		t.Fatalf("entries not parseable: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, filepath.Base(e.Path))
	}
	return names
}

func mkTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, d := range []string{"sub/deep", ".git", "node_modules", "pkg_cache"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{
		"root.txt", filepath.Join("sub", "mid.txt"),
		filepath.Join("sub", "deep", "leaf.txt"),
		filepath.Join(".git", "hidden.txt"),
		filepath.Join("node_modules", "dep.js"),
		filepath.Join("pkg_cache", "c.bin"),
	} {
		if err := os.WriteFile(filepath.Join(root, f), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestFastTree(t *testing.T) {
	root := mkTree(t)

	t.Run("dirs-first order, alphabetical, skips", func(t *testing.T) {
		m := mustJSON(t, fastTree(root, 10, 5000, ""))
		if m["success"] != true || m["truncated"] != false {
			t.Fatalf("envelope = %v", m)
		}
		got := treeNames(t, jmap(m))
		// depth 1: dirs sub (skips: .git, node_modules, pkg_cache via the
		// *_cache rule), file root.txt; depth 2: deep, mid.txt;
		// depth 3: leaf.txt
		want := []string{"sub", "root.txt", "deep", "mid.txt", "leaf.txt"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("order = %v, want %v", got, want)
		}
		for i, e := range m["entries"].([]any) {
			ent := e.(map[string]any)
			if _, ok := ent["mtime"].(float64); !ok {
				t.Fatalf("entry %d missing mtime: %v", i, ent)
			}
		}
	})

	t.Run("depth budget", func(t *testing.T) {
		got := treeNames(t, fastTree(root, 1, 5000, ""))
		want := []string{"sub", "root.txt"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("depth 1 = %v, want %v", got, want)
		}
		got = treeNames(t, fastTree(root, 2, 5000, ""))
		want = []string{"sub", "root.txt", "deep", "mid.txt"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("depth 2 = %v, want %v", got, want)
		}
	})

	t.Run("depth and entry clamps", func(t *testing.T) {
		got := treeNames(t, fastTree(root, 0, 0, "")) // clamps to 1/1
		if len(got) != 1 {
			t.Fatalf("clamped tree = %v", got)
		}
		m := mustJSON(t, fastTree(root, 99, 99, ""))
		if m["truncated"] != false {
			t.Fatalf("99/99 must not truncate on this tree: %v", m)
		}
	})

	t.Run("entry budget truncates with hint", func(t *testing.T) {
		m := mustJSON(t, fastTree(root, 10, 3, ""))
		if m["total"] != float64(3) || m["truncated"] != true {
			t.Fatalf("envelope = %v", m)
		}
		if hint, _ := m["hint"].(string); !strings.Contains(hint, "max_entries=3") {
			t.Fatalf("hint = %q", hint)
		}
	})

	t.Run("pattern filters names but still descends", func(t *testing.T) {
		got := treeNames(t, fastTree(root, 10, 5000, "*.txt"))
		want := []string{"root.txt", "mid.txt", "leaf.txt"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("pattern tree = %v, want %v", got, want)
		}
	})

	t.Run("missing dir errors", func(t *testing.T) {
		m := mustJSON(t, fastTree(filepath.Join(root, "nope"), 3, 500, ""))
		if m["success"] != false {
			t.Fatalf("envelope = %v", m)
		}
	})
}

// ------------------------------------------------------------ batch_search

func TestBatchSearchValidation(t *testing.T) {
	mk := func(ops ...map[string]any) json.RawMessage {
		b, _ := json.Marshal(map[string]any{"ops": ops})
		return b
	}
	one := map[string]any{"pattern": "x", "path": t.TempDir()}

	if _, isErr, unknown := dispatchTool("batch_search", mk(one)); unknown || isErr {
		t.Fatalf("single valid op: isErr=%v unknown=%v", isErr, unknown)
	}
	_, isErr, _ := dispatchTool("batch_search", mk(one, map[string]any{"pattern": "", "path": "/tmp"}))
	if !isErr {
		t.Fatalf("empty pattern op must reject the batch")
	}
	_, isErr, _ = dispatchTool("batch_search", mk(one, map[string]any{"pattern": "x", "path": ""}))
	if !isErr {
		t.Fatalf("empty path op must reject the batch")
	}
	many := make([]map[string]any, 17)
	for i := range many {
		many[i] = one
	}
	text, isErr, _ := dispatchTool("batch_search", mk(many...))
	if !isErr || !strings.Contains(text, "split") {
		t.Fatalf("17 ops must be rejected: %v", text)
	}
	text, isErr, _ = dispatchTool("batch_search", mk(one))
	if isErr || !strings.Contains(text, `"success":true`) {
		t.Fatalf("valid batch envelope = %v", text)
	}
}

// ------------------------------------------------- batch_read op clamping

func TestBatchReadOpClamp(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f.txt")
	var sb strings.Builder
	for i := 1; i <= 10; i++ {
		sb.WriteString(fmt.Sprintf("row%d\n", i))
	}
	if err := os.WriteFile(p, []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	zero, big := 0, 99999
	one := 1

	// offset=0 used to reach render() as 0 and panic via lines[-1]
	r := mustJSON(t, batchRead([]readOp{{Path: p, Offset: &zero}}))
	res := r["results"].([]any)[0].(map[string]any)
	if res["success"] != true {
		t.Fatalf("offset=0 envelope = %v", res)
	}
	if c, _ := res["content"].(string); !strings.HasPrefix(c, "1\trow1") {
		t.Fatalf("offset=0 content = %q", c)
	}

	// limit clamps to [1, readDefaultLimit] like fastRead
	r = mustJSON(t, batchRead([]readOp{{Path: p, Limit: &big}}))
	res = r["results"].([]any)[0].(map[string]any)
	if c, _ := res["content"].(string); strings.Count(c, "\n") != 9 {
		t.Fatalf("limit=99999 must clamp to 1000 lines, got %d rows", strings.Count(c, "\n")+1)
	}
	r = mustJSON(t, batchRead([]readOp{{Path: p, Limit: &one}}))
	res = r["results"].([]any)[0].(map[string]any)
	if c, _ := res["content"].(string); c != "1\trow1" {
		t.Fatalf("limit=1 content = %q", c)
	}
}

// ------------------------------------------------------- limitedWriter cap

func TestLimitedWriterCapped(t *testing.T) {
	var buf bytes.Buffer
	lw := &limitedWriter{w: &buf, max: 8}
	if _, err := lw.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if lw.capped {
		t.Fatalf("capped set on under-limit write")
	}
	if _, err := lw.Write([]byte("defghi")); err != nil {
		t.Fatal(err)
	}
	if !lw.capped {
		t.Fatalf("capped not set when bytes were dropped")
	}
	if buf.Len() != 8 {
		t.Fatalf("buffer len = %d, want 8", buf.Len())
	}
}

// -------------------------------------------- warm_exec argument hardening

func TestWarmExecRejectsNewlineCwd(t *testing.T) {
	for _, cwd := range []string{"bad\ncwd", "bad\rcwd"} {
		m := mustJSON(t, warmExec("echo hi", cwd, 5, false))
		if m["success"] != false ||
			m["error"] != "cwd must not contain newline characters" {
			t.Fatalf("cwd %q envelope = %v", cwd, m)
		}
	}
	m := mustJSON(t, batchExec([]string{"echo hi"}, "bad\ncwd", 5))
	if m["success"] != false ||
		m["error"] != "cwd must not contain newline characters" {
		t.Fatalf("batch_exec newline cwd envelope = %v", m)
	}
}

// ------------------------------------------- warm_exec write-wedge (SIGSTOP)

// TestWarmExecWriteWedge: stop the shell, then send a frame far larger than
// the 64KB pipe buffer. The write must not deadlock the server: the call
// returns rc 124 within the timeout, and the wedged writer goroutine exits
// once the tree is killed (buffered result channel -> no leak).
func TestWarmExecWriteWedge(t *testing.T) {
	ws := &warmShell{}
	defer func() {
		ws.mu.Lock()
		ws.killLocked()
		ws.mu.Unlock()
	}()
	if out, rc, _ := ws.run("echo warm", "", 10*time.Second); rc != 0 {
		t.Fatalf("warmup rc=%d out=%q", rc, out)
	}
	pid := ws.cmd.Process.Pid
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP: %v", err)
	}
	defer syscall.Kill(pid, syscall.SIGCONT) // best effort cleanup
	before := runtime.NumGoroutine()
	start := time.Now()
	out, rc, _ := ws.run("cat <<'EOF'\n"+strings.Repeat("x", 256<<10)+
		"\nEOF", "", 2*time.Second)
	elapsed := time.Since(start)
	if rc != 124 {
		t.Fatalf("rc=%d, want 124 (write wedge must time out); out=%q", rc, out)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("wedge took %v — server-wide deadlock", elapsed)
	}
	if !strings.Contains(out, "timed out") {
		t.Fatalf("out missing timeout notice: %q", out)
	}
	// after the kill the shell respawns and serves again
	out, rc, _ = ws.run("echo back", "", 10*time.Second)
	if rc != 0 || strings.TrimSpace(out) != "back" {
		t.Fatalf("post-wedge rc=%d out=%q", rc, out)
	}
	// wedged writer goroutines must drain, not accumulate
	waitFor(t, "goroutines to settle", 5*time.Second, func() bool {
		return runtime.NumGoroutine() <= before+2
	})
}

// ------------------------------------------------------ reader 1MB line cap

func TestWarmShellReaderLineCap(t *testing.T) {
	ws := &warmShell{}
	defer func() {
		ws.mu.Lock()
		ws.killLocked()
		ws.mu.Unlock()
	}()
	// one single 3MB line, then a normal line proving the shell is sane
	out, rc, _ := ws.run("printf 'huge'; head -c 3000000 /dev/zero | tr '\\0' 'y'; "+
		"printf '\\nmarker\\n'", "", 15*time.Second)
	if rc != 0 {
		t.Fatalf("rc=%d out len=%d", rc, len(out))
	}
	if !strings.Contains(out, "marker") {
		t.Fatalf("line after the capped line lost: len=%d", len(out))
	}
	// the capped line keeps AT MOST maxShellLine of the 3MB run: locate the
	// run of 'y's and bound its length (the first 1MB legitimately is 'y')
	ys := 0
	for _, b := range []byte(out) {
		if b == 'y' {
			ys++
		}
	}
	if ys > maxShellLine {
		t.Fatalf("%d 'y' bytes materialized, cap is %d", ys, maxShellLine)
	}
	if len(out) > maxShellLine+len("huge")+len("marker")+64 {
		t.Fatalf("out len %d far above the 1MB cap", len(out))
	}
}

// ------------------------------------------------------ fast_tree hardening

func TestFastTreeInvalidPattern(t *testing.T) {
	root := mkTree(t)
	m := mustJSON(t, fastTree(root, 3, 500, "["))
	if m["success"] != false || m["error"] != "invalid pattern: [" {
		t.Fatalf("envelope = %v", m)
	}
}

func TestFastTreeSymlinkLstat(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "realdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "f.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("realdir", filepath.Join(root, "linkdir")); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}
	if err := os.Symlink("gone", filepath.Join(root, "dangling")); err != nil {
		t.Fatal(err)
	}
	m := mustJSON(t, fastTree(root, 10, 500, ""))
	if m["success"] != true {
		t.Fatalf("envelope = %v", m)
	}
	var ents []map[string]any
	b, _ := json.Marshal(m["entries"])
	if err := json.Unmarshal(b, &ents); err != nil {
		t.Fatal(err)
	}
	byName := map[string]map[string]any{}
	var names []string
	for _, e := range ents {
		byName[filepath.Base(e["path"].(string))] = e
		names = append(names, filepath.Base(e["path"].(string)))
	}
	// every entry must always carry is_symlink
	for _, e := range ents {
		if _, ok := e["is_symlink"].(bool); !ok {
			t.Fatalf("entry missing is_symlink: %v", e)
		}
	}
	ld := byName["linkdir"]
	if ld == nil || ld["is_symlink"] != true || ld["is_dir"] != false {
		t.Fatalf("linkdir entry = %v", ld)
	}
	if dg := byName["dangling"]; dg == nil || dg["is_symlink"] != true {
		t.Fatalf("dangling link not listed: %v", dg)
	}
	// symlinked dir must NOT be descended into: nothing from inside realdir
	// appears under the link, and realdir's own children are not duplicated
	if byName["f.txt"] == nil || byName["realdir"] == nil {
		t.Fatalf("missing base entries: %v", names)
	}
}

func TestFastTreeExactFitNotTruncated(t *testing.T) {
	root := t.TempDir()
	for _, n := range []string{"a.txt", "b.txt"} {
		if err := os.WriteFile(filepath.Join(root, n), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	m := mustJSON(t, fastTree(root, 1, 2, ""))
	if m["total"] != float64(2) || m["truncated"] != false {
		t.Fatalf("exact fit must not truncate: %v", m)
	}
	m = mustJSON(t, fastTree(root, 1, 1, ""))
	if m["truncated"] != true {
		t.Fatalf("omitted entry must truncate: %v", m)
	}
}

// ------------------------------------------------------- searchWalk glob

func TestSearchWalkInvalidGlob(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte("needle\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	m := mustJSON(t, fastSearch("needle", dir, "[", true, 10, 0, 0))
	if useSearch && rg() != "" {
		// rg validates -g itself and errors — either way it must not be
		// a silent empty listing
		if m["success"] != false {
			t.Fatalf("invalid glob envelope = %v", m)
		}
	} else {
		if m["success"] != false || m["error"] != "invalid pattern: [" {
			t.Fatalf("walk invalid glob envelope = %v", m)
		}
	}
}
