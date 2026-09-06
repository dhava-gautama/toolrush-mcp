package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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
		_, hits := parse(t, collectHits("a/b.txt:3:hello world\n", "rg", 10, 0))
		if len(hits) != 1 || hits[0].Path != "a/b.txt" ||
			hits[0].Line != 3 || hits[0].Content != "hello world" {
			t.Fatalf("hits = %+v", hits)
		}
	})

	t.Run("colon in path (non-greedy fix)", func(t *testing.T) {
		_, hits := parse(t, collectHits("co:lon/f.txt:1:needle\n", "rg", 10, 0))
		if len(hits) != 1 {
			t.Fatalf("hits = %+v", hits)
		}
		if hits[0].Path != "co:lon/f.txt" || hits[0].Line != 1 ||
			hits[0].Content != "needle" {
			t.Fatalf("hit = %+v", hits[0])
		}
	})

	t.Run("garbage lines skipped from hits", func(t *testing.T) {
		m, hits := parse(t, collectHits("not a hit\nx/y.txt:2:real\n", "rg", 10, 0))
		if len(hits) != 1 || hits[0].Path != "x/y.txt" {
			t.Fatalf("hits = %+v", hits)
		}
		// known quirk: garbage rows still count toward total_hits and
		// therefore flip the truncated flag.
		if m["total_hits"] != float64(2) {
			t.Fatalf("total_hits = %v", m["total_hits"])
		}
	})

	t.Run("offset and limit windowing", func(t *testing.T) {
		out := "f:1:a\nf:2:b\nf:3:c\nf:4:d\n"
		m, hits := parse(t, collectHits(out, "rg", 2, 1))
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
		m, hits := parse(t, collectHits("f:1:a\nf:2:b\n", "rg", 1, 0))
		if len(hits) != 1 || m["truncated"] != true {
			t.Fatalf("hits = %+v truncated = %v", hits, m["truncated"])
		}
	})

	t.Run("empty output", func(t *testing.T) {
		m, hits := parse(t, collectHits("", "rg", 10, 0))
		if len(hits) != 0 || m["total_hits"] != float64(0) {
			t.Fatalf("m = %v", m)
		}
	})

	t.Run("overlong content clamped", func(t *testing.T) {
		long := strings.Repeat("z", 2500)
		_, hits := parse(t, collectHits("f:1:"+long+"\n", "rg", 10, 0))
		if !strings.HasSuffix(hits[0].Content, "... [truncated]") {
			t.Fatalf("content not clamped (len %d)", len(hits[0].Content))
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
	f.Add("a/b.txt:3:hello\n")
	f.Add("co:lon/f.txt:1:needle\n")
	f.Add("garbage line\nf:2:x\n\n")
	f.Add("")
	f.Add(":::\n1:2:3:4:5\n")
	f.Fuzz(func(t *testing.T, data string) {
		limit := 1 + len(data)%37
		offset := len(data) % 11
		out := collectHits(data, "fuzz", limit, offset)
		var m map[string]any
		if err := json.Unmarshal([]byte(out), &m); err != nil {
			t.Fatalf("collectHits emitted invalid JSON: %v\n%q", err, out)
		}
	})
}
