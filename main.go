// ToolRush-MCP — Go port of toolrush_mcp.py (itself a port of ToolRush v2,
// github.com/OnlyTerp/toolrush, Hermes Agent plugin) to a static binary.
//
// Same wire contract, same tools, same envelopes as the Python reference:
//
//	fast_read  batch_read  fast_search  batch_search  fast_tree
//	warm_exec  batch_exec  doctor
//
// What Go buys (measured in bench_go.py, evidence in README):
//   - per-request goroutines: a 120s warm_exec no longer head-of-line blocks
//     fast_read behind it (the Python server is a serial loop)
//   - ~1ms startup vs ~68ms interpreter boot (paid per session)
//   - GIL-free CPU: decode/render parallelize when they need to
//   - static binary, no Python runtime dependency
//
// Kill-switches (fail-closed): TOOLRUSH_FASTLANE/SEARCH/PERSIST/PARALLEL=0
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"
)

// version is injected at release time via -X main.version=...; a dev build
// falls back to the module version from runtime/debug.ReadBuildInfo (set by
// `go install ...@vX.Y.Z`), else stays "dev".
var version = "dev"

func init() {
	if version == "dev" {
		if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
			version = bi.Main.Version
		}
	}
}

var (
	useFastlane = os.Getenv("TOOLRUSH_FASTLANE") != "0"
	useSearch   = os.Getenv("TOOLRUSH_SEARCH") != "0"
	usePersist  = os.Getenv("TOOLRUSH_PERSIST") != "0"
	useParallel = os.Getenv("TOOLRUSH_PARALLEL") != "0"
)

const (
	maxLine            = 2000
	maxReadBytes       = 100_000
	maxOut             = 8 << 20
	readDefaultLimit   = 1000
	searchTimeout      = 60 * time.Second
	execDefaultTimeout = 120
	batchMax           = 16
	linesCacheMax      = 64
	largeFile          = 64 << 20
	cacheFileMax       = 4 << 20
	walkMaxFile        = 16 << 20
)

var imageSuffixes = map[string]bool{
	".png": true, ".jpg": true, ".jpeg": true, ".gif": true,
	".webp": true, ".bmp": true, ".ico": true,
}
var skipSuffixes = map[string]bool{
	".png": true, ".jpg": true, ".jpeg": true, ".gif": true, ".webp": true,
	".bmp": true, ".ico": true, ".pyc": true, ".pyo": true,
}

// ---------------------------------------------------------------- stats

var statsMu sync.Mutex
var stats = map[string]int{
	"fast_read": 0, "batch_read": 0, "fast_search": 0, "batch_search": 0,
	"fast_tree": 0, "warm_exec": 0,
	"batch_exec": 0, "cache_hits": 0, "shell_respawns": 0,
}

func bump(key string) {
	statsMu.Lock()
	stats[key]++
	statsMu.Unlock()
}

// ---------------------------------------------------------------- helpers

// jmap marshals without HTML escaping (parity with Python ensure_ascii=False:
// raw UTF-8, no < > & mangling).
func jmap(m map[string]any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(m)
	return strings.TrimRight(buf.String(), "\n")
}

func jerr(msg string) string {
	return jmap(map[string]any{"success": false, "error": msg})
}

func resolvePath(p string) string {
	if !filepath.IsAbs(p) {
		cwd, err := os.Getwd()
		if err == nil {
			p = filepath.Join(cwd, p)
		}
	}
	if r, err := filepath.EvalSymlinks(p); err == nil {
		p = r
	}
	return p
}

func shq(p string) string {
	return "'" + strings.ReplaceAll(p, "'", "'\\''") + "'"
}

// ---------------------------------------------------------------- fast_read

type linesEntry struct {
	mtimeNs int64
	size    int64
	lines   []string
}

var linesMu sync.Mutex
var linesCache = map[string]linesEntry{}

func splitLines(b []byte) []string {
	// BOM strip like Python utf-8-sig
	b = bytes.TrimPrefix(b, []byte("\xef\xbb\xbf"))
	s := string(b)
	if strings.Contains(s, "\r\n") {
		s = strings.ReplaceAll(s, "\r\n", "\n")
	}
	return strings.Split(strings.TrimRight(s, "\n"), "\n")
}

// decodeStore validates + caches; returns (lines, errMsg). Main-thread-only
// CPU work in Python; here it is just fast.
func decodeStore(rp string, st os.FileInfo, raw []byte) ([]string, string) {
	head := raw
	if len(head) > 8000 {
		head = head[:8000]
	}
	if bytes.IndexByte(head, 0) >= 0 {
		return nil, "binary file (NUL byte in first 8000 bytes)"
	}
	if !utf8.Valid(raw) {
		return nil, "binary file (not UTF-8 decodable)"
	}
	lines := splitLines(raw)
	// Byte budget: serving the read is fine, but caching a huge file
	// multiplies RSS (lines + raw string copies). Skip the store.
	if st.Size() <= cacheFileMax {
		linesMu.Lock()
		if len(linesCache) >= linesCacheMax {
			for k := range linesCache { // evict an arbitrary (oldest-ish) entry
				delete(linesCache, k)
				break
			}
		}
		linesCache[rp] = linesEntry{st.ModTime().UnixNano(), st.Size(), lines}
		linesMu.Unlock()
	}
	return lines, ""
}

func getLines(rp string, st os.FileInfo) ([]string, string) {
	linesMu.Lock()
	hit, ok := linesCache[rp]
	linesMu.Unlock()
	if ok && hit.mtimeNs == st.ModTime().UnixNano() && hit.size == st.Size() {
		bump("cache_hits")
		return hit.lines, ""
	}
	raw, err := os.ReadFile(rp)
	if err != nil {
		return nil, fmt.Sprintf("%T: %v", err, err)
	}
	return decodeStore(rp, st, raw)
}

func render(lines []string, size int64, offset, limit int) string {
	total := len(lines)
	// Kernel pseudo-files (/proc et al.) stat as 0 bytes but hold content —
	// only claim emptiness when the read itself produced nothing.
	if size == 0 && (total == 0 || (total == 1 && lines[0] == "")) {
		return jmap(map[string]any{
			"success": true, "content": "", "total_lines": 0,
			"file_size": 0, "hint": "File is empty (0 bytes)."})
	}
	if offset < 0 { // tail mode
		offset = total + offset + 1
		if offset < 1 {
			offset = 1
		}
	}
	if offset < 1 { // defensive: callers clamp 0→1, render must never index -1
		offset = 1
	}
	if offset > total {
		return jmap(map[string]any{
			"success": true, "content": "", "total_lines": total,
			"file_size": size,
			"hint": fmt.Sprintf("Note: offset %d is beyond the end of the file "+
				"(%d lines total). Retry with offset <= %d.", offset, total, total)})
	}
	end := offset + limit - 1
	if end < offset || end > total { // defensive: clamps bound this already
		end = total
	}
	var sb strings.Builder
	nbytes := 0
	clipped := false
	last := offset - 1
	for i := offset; i <= end && i <= total; i++ {
		line := lines[i-1]
		if len(line) > maxLine {
			line = line[:maxLine] + "... [truncated]"
		}
		row := strconv.Itoa(i) + "\t" + line
		nbytes += len(row) + 1
		if nbytes > maxReadBytes {
			clipped = true
			end = i - 1
			break
		}
		if last >= offset {
			sb.WriteByte('\n')
		}
		sb.WriteString(row)
		last = i
	}
	truncated := total > end
	d := map[string]any{
		"success": true, "content": sb.String(), "total_lines": total,
		"file_size": size, "truncated": truncated,
		"is_binary": false, "is_image": false,
	}
	if truncated || clipped {
		d["hint"] = fmt.Sprintf("Use offset=%d to continue reading "+
			"(showing %d-%d of %d lines)", end+1, offset, end, total)
	}
	return jmap(d)
}

func streamRead(rp string, st os.FileInfo, offset, limit int) string {
	if offset < 1 {
		return jerr("large file (>64MB): tail mode unsupported, use a positive offset")
	}
	f, err := os.Open(rp)
	if err != nil {
		return jerr(fmt.Sprintf("%T: %v", err, err))
	}
	defer f.Close()
	r := bufio.NewReader(f)
	skip := offset - 1
	var sb strings.Builder
	shown := 0
	lineNo := 0
	for {
		line, err := r.ReadString('\n')
		if line != "" {
			lineNo++
			if lineNo > skip && shown < limit {
				line = strings.TrimRight(line, "\n")
				line = strings.TrimSuffix(line, "\r")
				if len(line) > maxLine {
					line = line[:maxLine] + "... [truncated]"
				}
				if shown > 0 {
					sb.WriteByte('\n')
				}
				sb.WriteString(strconv.Itoa(lineNo) + "\t" + line)
				shown++
				if shown == limit {
					break // got the window — don't scan to EOF
				}
			}
		}
		if err != nil {
			break
		}
	}
	return jmap(map[string]any{
		"success": true, "content": sb.String(), "total_lines": nil,
		"file_size": st.Size(), "truncated": shown == limit,
		"is_binary": false, "is_image": false,
		"hint": fmt.Sprintf("large file streamed without line count; "+
			"offset=%d continues", offset+shown)})
}

func fastRead(path string, offset, limit int) string {
	if !useFastlane {
		return jerr("TOOLRUSH_FASTLANE=0 — lane disabled; use the harness's native read")
	}
	if limit < 1 {
		limit = 1
	}
	if limit > readDefaultLimit {
		limit = readDefaultLimit
	}
	if offset == 0 { // page 1; negatives stay tail mode
		offset = 1
	}
	rp := resolvePath(path)
	st, err := os.Stat(rp)
	if err != nil || !st.Mode().IsRegular() {
		return jerr(fmt.Sprintf("File not found: %s", path))
	}
	if imageSuffixes[strings.ToLower(filepath.Ext(rp))] {
		return jerr("image file — use the harness's vision-capable reader")
	}
	bump("fast_read")
	if st.Size() > largeFile {
		return streamRead(rp, st, offset, limit)
	}
	lines, errMsg := getLines(rp, st)
	if errMsg != "" {
		return jerr(errMsg)
	}
	return render(lines, st.Size(), offset, limit)
}

// probe: ("", missInfo) cold-but-probed; (rendered, nil) cache hit;
// ("", nil) defer to fastRead.
type missInfo struct {
	rp string
	st os.FileInfo
}

func probe(path string, offset, limit int) (string, *missInfo) {
	rp := resolvePath(path)
	st, err := os.Stat(rp)
	if err != nil || !st.Mode().IsRegular() ||
		imageSuffixes[strings.ToLower(filepath.Ext(rp))] || st.Size() > largeFile {
		return "", nil
	}
	linesMu.Lock()
	hit, ok := linesCache[rp]
	linesMu.Unlock()
	if !ok || hit.mtimeNs != st.ModTime().UnixNano() || hit.size != st.Size() {
		return "", &missInfo{rp, st}
	}
	bump("cache_hits")
	return render(hit.lines, st.Size(), offset, limit), nil
}

// ---------------------------------------------------------------- batch_read

type readOp struct {
	Path   string `json:"path"`
	Offset *int   `json:"offset"`
	Limit  *int   `json:"limit"`
}

func (o readOp) off() int {
	// 0 means "page 1" exactly like fastRead; negatives stay tail mode.
	// Without the 0→1 clamp, render() indexes lines[-1] and panics.
	if o.Offset == nil || *o.Offset == 0 {
		return 1
	}
	return *o.Offset
}
func (o readOp) lim() int {
	l := readDefaultLimit
	if o.Limit != nil {
		l = *o.Limit
	}
	if l < 1 {
		l = 1
	}
	if l > readDefaultLimit {
		l = readDefaultLimit
	}
	return l
}

func batchRead(ops []readOp) string {
	if !useParallel {
		return jerr("TOOLRUSH_PARALLEL=0 — lane disabled; issue individual reads")
	}
	if len(ops) == 0 {
		return jerr("ops must be a non-empty list of {path, offset?, limit?}")
	}
	if len(ops) > batchMax {
		return jerr(fmt.Sprintf("batch too large: %d > %d — split it", len(ops), batchMax))
	}
	for i, op := range ops {
		if op.Path == "" {
			return jerr(fmt.Sprintf("op %d invalid: each op needs a string 'path'", i))
		}
	}
	bump("batch_read")
	// Serial by design (measured: threads lose on every filesystem here —
	// reads are page-cache fast, dispatch overhead pure loss). The win is
	// ONE MCP call instead of N. Results embedded raw (no double encode).
	results := make([]json.RawMessage, len(ops))
	misses := []int{}
	infos := make([]*missInfo, len(ops))
	for i, op := range ops {
		hit, mi := probe(op.Path, op.off(), op.lim())
		if hit != "" {
			results[i] = json.RawMessage(hit)
		} else {
			misses = append(misses, i)
			infos[i] = mi
		}
	}
	for _, i := range misses {
		op := ops[i]
		if mi := infos[i]; mi != nil { // cold-but-probed: no re-stat
			lines, errMsg := getLines(mi.rp, mi.st)
			bump("fast_read")
			if errMsg != "" {
				results[i] = json.RawMessage(jerr(errMsg))
			} else {
				results[i] = json.RawMessage(render(lines, mi.st.Size(), op.off(), op.lim()))
			}
		} else {
			results[i] = json.RawMessage(fastRead(op.Path, op.off(), op.lim()))
		}
	}
	out, _ := json.Marshal(map[string]any{"success": true, "results": results})
	return string(out)
}

// -------------------------------------------------------------- fast_search

var rgOnce sync.Once
var rgPath string

func rg() string {
	rgOnce.Do(func() {
		if p, err := exec.LookPath("rg"); err == nil {
			rgPath = p
			return
		}
		home, _ := os.UserHomeDir()
		alt := filepath.Join(home, ".kimi-code", "bin", "rg")
		if st, err := os.Stat(alt); err == nil && !st.IsDir() {
			rgPath = alt
		}
	})
	return rgPath
}

func clampLimitOffset(limit, offset int) (int, int) {
	if limit < 1 {
		limit = 1
	}
	if limit > 2000 {
		limit = 2000
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

// clampCtx bounds the fast_search context param: 0..5, default 0. At 0 the
// envelopes stay byte-identical to pre-context builds (no "context" field).
func clampCtx(c int) int {
	if c < 0 {
		return 0
	}
	if c > 5 {
		return 5
	}
	return c
}

func searchRg(pattern, path, fileGlob string, caseSensitive bool, limit, offset, ctx int) string {
	// --json: rg emits a message stream (begin/match/context/end) instead of
	// text rows, so paths, line numbers and content arrive in separate fields
	// — no more "path:line:content" regex ambiguity (colons/dashes/CRs in
	// content, "-N-" gutter fabrication, context rows re-parsed as matches).
	args := []string{"--json", "--line-number", "--no-heading", "--with-filename",
		"--color", "never", "--encoding", "utf-8"}
	if !caseSensitive {
		args = append(args, "-i")
	}
	if fileGlob != "" {
		args = append(args, "-g", fileGlob)
	}
	if ctx > 0 {
		// -C n (clamped to 5 by the caller) makes rg emit context messages
		// around each match; at 0 no context messages exist at all.
		args = append(args, "-C", strconv.Itoa(ctx))
	}
	args = append(args, "-e", pattern, "--", resolvePath(path))
	rctx, cancel := context.WithTimeout(context.Background(), searchTimeout)
	defer cancel()
	cmd := exec.CommandContext(rctx, rg(), args...)
	var stdout, stderr bytes.Buffer
	lw := &limitedWriter{w: &stdout, max: maxOut}
	cmd.Stdout = lw
	cmd.Stderr = &stderr
	err := cmd.Run()
	if rctx.Err() == context.DeadlineExceeded {
		return jerr(fmt.Sprintf("rg timed out after %ds", int(searchTimeout.Seconds())))
	}
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == 1 {
			// no hits — valid
		} else {
			msg := stderr.String()
			if len(msg) > 500 {
				msg = msg[:500]
			}
			return jerr("rg error: " + msg)
		}
	}
	return collectHitsJSON(stdout.String(), "rg", limit, offset, ctx, lw.capped)
}

// limitedWriter caps total bytes written; capped reports whether any byte
// was dropped, i.e. the consumer only saw a prefix of the stream.
type limitedWriter struct {
	w      *bytes.Buffer
	max    int
	capped bool
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	remain := l.max - l.w.Len()
	if remain > 0 {
		if len(p) > remain {
			l.w.Write(p[:remain])
			l.capped = true
		} else {
			l.w.Write(p)
		}
	} else if len(p) > 0 {
		l.capped = true
	}
	return len(p), nil
}

// searchHit is one match row; Context is the optional gutter ("N-line" for
// rows before the match, "N+line" for rows after), omitted when empty.
type searchHit struct {
	Path    string `json:"path"`
	Line    int    `json:"line"`
	Content string `json:"content"`
	Context string `json:"context,omitempty"`
}

// envelopeHits renders the search envelope. capped=true means rg's 8MB
// stdout cap was hit, so total_hits is a lower bound.
func envelopeHits(engine string, hits []searchHit, total, offset int, truncated, capped bool) string {
	return jmap(map[string]any{
		"success": true, "engine": engine, "hits": hits,
		"total_hits": total, "shown": len(hits),
		"truncated": truncated || total > offset+len(hits),
		"capped":    capped,
	})
}

// ctxLine is one parsed context message awaiting attachment.
type ctxLine struct {
	no   int
	line string
}

// collectHitsJSON parses rg's --json message stream. Match messages carry
// path/line_number/lines.text; context messages carry line_number/lines.text.
// Grouping per the pinned contract: context rows seen before any match are
// before-context of the FOLLOWING match; rows after a match are after-context
// of the PRECEDING match (rg prints shared rows once, positioned after the
// first match). Only match messages count toward total_hits; limit/offset
// window matches exactly like the pre-JSON lane.
func collectHitsJSON(out, engine string, limit, offset, ctx int, capped bool) string {
	hits := []searchHit{}
	total := 0
	var pending []ctxLine // before-context rows awaiting their match
	var last *searchHit   // last kept hit — after-context target
	for _, raw := range strings.Split(out, "\n") {
		if raw == "" {
			continue
		}
		// fresh struct per line: Unmarshal into a reused struct leaks
		// stale fields from the previous message (type/data inheritance)
		var msg struct {
			Type string `json:"type"`
			Data struct {
				Path struct {
					Text string `json:"text"`
				} `json:"path"`
				Lines struct {
					Text string `json:"text"`
				} `json:"lines"`
				LineNumber int `json:"line_number"`
			} `json:"data"`
		}
		if json.Unmarshal([]byte(raw), &msg) != nil {
			continue // truncated cap tail or stray — capped flag covers it
		}
		switch msg.Type {
		case "begin": // new file: grouping state is per-file
			pending = nil
			last = nil
		case "match":
			total++
			kept := total > offset && len(hits) < limit
			line := stripOneCRLF(msg.Data.Lines.Text)
			if !kept {
				pending = nil
				last = nil
				continue
			}
			h := searchHit{
				Path:    msg.Data.Path.Text,
				Line:    msg.Data.LineNumber,
				Content: clampLine(line),
			}
			if len(pending) > 0 {
				rows := make([]string, 0, len(pending))
				for _, p := range pending {
					rows = append(rows, strconv.Itoa(p.no)+"-"+clampLine(p.line))
				}
				h.Context = strings.Join(rows, "\n")
				pending = nil
			}
			hits = append(hits, h)
			last = &hits[len(hits)-1]
		case "context":
			line := stripOneCRLF(msg.Data.Lines.Text)
			if last != nil {
				last.Context = joinCtx(last.Context,
					strconv.Itoa(msg.Data.LineNumber)+"+"+clampLine(line))
			} else {
				pending = append(pending, ctxLine{msg.Data.LineNumber, line})
			}
		}
	}
	return envelopeHits(engine, hits, total, offset, false, capped)
}

// stripOneCRLF removes exactly one trailing "\n" and then one trailing "\r"
// (rg's lines.text keeps the raw line terminator; CRLF content must not
// leak a stray CR into the envelope).
func stripOneCRLF(s string) string {
	s = strings.TrimSuffix(s, "\n")
	s = strings.TrimSuffix(s, "\r")
	return s
}

func clampLine(s string) string {
	if len(s) > maxLine {
		return s[:maxLine] + "... [truncated]"
	}
	return s
}

func joinCtx(existing, row string) string {
	if existing == "" {
		return row
	}
	return existing + "\n" + row
}

func searchWalk(pattern, path, fileGlob string, caseSensitive bool, limit, offset, ctx int) string {
	pat := pattern
	if !caseSensitive {
		pat = "(?i)" + pat
	}
	rx, err := regexp.Compile(pat)
	if err != nil {
		return jerr(fmt.Sprintf("invalid regex: %v", err))
	}
	if fileGlob != "" {
		if _, err := filepath.Match(fileGlob, ""); err != nil {
			return jerr("invalid pattern: " + fileGlob)
		}
	}
	root := resolvePath(path)
	hits := []searchHit{}
	total := 0
	deadline := time.Now().Add(searchTimeout)
	timedOut := false
	_ = filepath.WalkDir(root, func(fp string, d os.DirEntry, err error) error {
		if time.Now().After(deadline) {
			timedOut = true
			return fmt.Errorf("walk deadline exceeded after %ds",
				int(searchTimeout.Seconds()))
		}
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if fileGlob != "" {
			if ok, _ := filepath.Match(fileGlob, name); !ok {
				return nil
			}
		}
		if skipSuffixes[strings.ToLower(filepath.Ext(name))] {
			return nil
		}
		if fi, err := d.Info(); err != nil || fi.Size() > walkMaxFile {
			return nil // unreadable or too large to walk
		}
		raw, err := os.ReadFile(fp)
		if err != nil {
			return nil
		}
		head := raw
		if len(head) > 8000 {
			head = head[:8000]
		}
		if bytes.IndexByte(head, 0) >= 0 || !utf8.Valid(raw) {
			return nil
		}
		fileLines := splitLines(raw)
		for i, line := range fileLines {
			if !rx.MatchString(line) {
				continue
			}
			total++
			if total <= offset || len(hits) >= limit {
				continue
			}
			content := line
			if len(content) > maxLine {
				content = content[:maxLine] + "... [truncated]"
			}
			h := searchHit{Path: fp, Line: i + 1, Content: content}
			if ctx > 0 {
				// same gutter format as the rg engine: "N-line" before,
				// "N+line" after, clamped like match lines
				var gutter []string
				lo := i - ctx
				if lo < 0 {
					lo = 0
				}
				hi := i + ctx + 1
				if hi > len(fileLines) {
					hi = len(fileLines)
				}
				for j := lo; j < i; j++ {
					gutter = append(gutter, ctxRow(j+1, fileLines[j], "-"))
				}
				for j := i + 1; j < hi; j++ {
					gutter = append(gutter, ctxRow(j+1, fileLines[j], "+"))
				}
				if len(gutter) > 0 {
					h.Context = strings.Join(gutter, "\n")
				}
			}
			hits = append(hits, h)
		}
		return nil
	})
	return envelopeHits("walk", hits, total, offset, timedOut, false)
}

// ctxRow renders one context-gutter row, clamped like match lines.
func ctxRow(no int, line, sep string) string {
	if len(line) > maxLine {
		line = line[:maxLine] + "... [truncated]"
	}
	return strconv.Itoa(no) + sep + line
}

func fastSearch(pattern, path, fileGlob string, caseSensitive bool, limit, offset, ctx int) string {
	if pattern == "" {
		return jerr("pattern must be a non-empty string (ripgrep regex syntax)")
	}
	limit, offset = clampLimitOffset(limit, offset)
	ctx = clampCtx(ctx)
	bump("fast_search")
	if useSearch && rg() != "" {
		return searchRg(pattern, path, fileGlob, caseSensitive, limit, offset, ctx)
	}
	return searchWalk(pattern, path, fileGlob, caseSensitive, limit, offset, ctx)
}

// ------------------------------------------------------------- batch_search

type searchOp struct {
	Pattern       string `json:"pattern"`
	Path          string `json:"path"`
	FileGlob      string `json:"file_glob"`
	CaseSensitive *bool  `json:"case_sensitive"`
	Context       *int   `json:"context"`
	Limit         *int   `json:"limit"`
	Offset        *int   `json:"offset"`
}

func (o searchOp) run() string {
	cs := true
	if o.CaseSensitive != nil {
		cs = *o.CaseSensitive
	}
	lim, off := 100, 0
	if o.Limit != nil {
		lim = *o.Limit
	}
	if o.Offset != nil {
		off = *o.Offset
	}
	ctx := 0
	if o.Context != nil {
		ctx = *o.Context
	}
	return fastSearch(o.Pattern, o.Path, o.FileGlob, cs, lim, off, ctx)
}

func batchSearch(ops []searchOp) string {
	if !useParallel {
		return jerr("TOOLRUSH_PARALLEL=0 — lane disabled; issue individual searches")
	}
	if len(ops) == 0 {
		return jerr("ops must be a non-empty list of {pattern, path, file_glob?, " +
			"case_sensitive?, context?, limit?, offset?}")
	}
	if len(ops) > batchMax {
		return jerr(fmt.Sprintf("batch too large: %d > %d — split it", len(ops), batchMax))
	}
	for i, op := range ops {
		if op.Pattern == "" {
			return jerr(fmt.Sprintf("op %d invalid: each op needs a non-empty string 'pattern'", i))
		}
		if op.Path == "" {
			return jerr(fmt.Sprintf("op %d invalid: each op needs a non-empty string 'path'", i))
		}
	}
	bump("batch_search")
	// The opposite of batch_read's serial-by-design: each op spawns its own
	// rg process and searches are process/IO-bound, so fan out — the batch
	// wall time approaches the slowest op, not the sum. Results embedded raw
	// (no double encode), input order preserved, per-op failures isolated
	// (that op's result is an error envelope, the rest still succeed).
	results := make([]string, len(ops))
	var wg sync.WaitGroup
	for i, op := range ops {
		wg.Add(1)
		go func(i int, op searchOp) {
			defer wg.Done()
			results[i] = op.run()
		}(i, op)
	}
	wg.Wait()
	raw := make([]json.RawMessage, len(results))
	for i, r := range results {
		raw[i] = json.RawMessage(r)
	}
	out, _ := json.Marshal(map[string]any{"success": true, "results": raw})
	return string(out)
}

// ---------------------------------------------------------------- fast_tree

var treeSkipNames = map[string]bool{
	".git": true, "node_modules": true, "__pycache__": true, ".venv": true,
	"target": true, "dist": true, "build": true,
}

// treeSkip is the shared fast_tree skip list: exact names plus anything
// ending in "_cache" (".pytest_cache", "my_cache", ...).
func treeSkip(name string) bool {
	return treeSkipNames[name] || strings.HasSuffix(name, "_cache")
}

type treeEntry struct {
	Path      string `json:"path"`
	IsDir     bool   `json:"is_dir"`
	IsSymlink bool   `json:"is_symlink"`
	Size      int64  `json:"size"`
	Mtime     int64  `json:"mtime"` // unix seconds
}

func fastTree(path string, maxDepth, maxEntries int, pattern string) string {
	if !useFastlane {
		return jerr("TOOLRUSH_FASTLANE=0 — lane disabled; use the harness's native listing")
	}
	if pattern != "" {
		if _, err := filepath.Match(pattern, ""); err != nil {
			return jerr("invalid pattern: " + pattern)
		}
	}
	root := resolvePath(path)
	st, err := os.Stat(root)
	if err != nil || !st.IsDir() {
		return jerr(fmt.Sprintf("Directory not found: %s", path))
	}
	if maxDepth < 1 {
		maxDepth = 1
	}
	if maxDepth > 10 {
		maxDepth = 10
	}
	if maxEntries < 1 {
		maxEntries = 1
	}
	if maxEntries > 5000 {
		maxEntries = 5000
	}
	bump("fast_tree")
	entries := []treeEntry{}
	truncated := false
	var walk func(dir string, depth int)
	walk = func(dir string, depth int) {
		ents, err := os.ReadDir(dir) // sorted by name
		if err != nil {
			return
		}
		// deterministic dirs-first, alphabetical within the directory
		dirs := []os.DirEntry{}
		ordered := make([]os.DirEntry, 0, len(ents))
		for _, e := range ents {
			if e.IsDir() {
				dirs = append(dirs, e)
			} else {
				ordered = append(ordered, e)
			}
		}
		ordered = append(dirs, ordered...)
		// pass 1: list this directory's entries (depth budget already
		// enforced by the descent guard — we only get here within budget).
		// Truncation is flagged only when an entry that WOULD be listed is
		// omitted due to the budget — an exact fit is not truncated.
		for _, e := range ordered {
			if truncated {
				return
			}
			name := e.Name()
			if treeSkip(name) {
				continue
			}
			if pattern != "" {
				if ok, _ := filepath.Match(pattern, name); !ok {
					continue // filtered out, but dirs are still descended into
				}
			}
			if len(entries) >= maxEntries {
				truncated = true
				return
			}
			fp := filepath.Join(dir, name)
			// Lstat semantics (parity with the Python reference): a symlink
			// reports is_dir:false/is_symlink:true and the LINK's own
			// size/mtime; dangling links are listed, never descended into.
			fi, err := os.Lstat(fp)
			if err != nil {
				continue
			}
			entries = append(entries, treeEntry{
				Path:      fp,
				IsDir:     fi.IsDir(),
				IsSymlink: fi.Mode()&os.ModeSymlink != 0,
				Size:      fi.Size(),
				Mtime:     fi.ModTime().Unix(),
			})
		}
		// pass 2: descend into child dirs (children of the root are depth 1,
		// so a dir listed at `depth` is descended only when depth < maxDepth)
		if truncated || depth >= maxDepth {
			return
		}
		for _, e := range dirs {
			if treeSkip(e.Name()) {
				continue
			}
			walk(filepath.Join(dir, e.Name()), depth+1)
			if truncated {
				return
			}
		}
	}
	walk(root, 1)
	d := map[string]any{
		"success": true, "entries": entries,
		"total": len(entries), "truncated": truncated,
	}
	if truncated {
		d["hint"] = fmt.Sprintf("max_entries=%d reached — narrow with pattern= "+
			"or a more specific path", maxEntries)
	}
	return jmap(d)
}

// --------------------------------------------------------------- warm_exec

const outTruncNotice = "\n[output truncated at 8MB]"

// warmShell is ONE persistent bash. mu serializes run() (structural fields:
// cmd/stdin/lines/cwd); alive is an atomic so doctor can peek liveness
// without blocking behind a running command.
type warmShell struct {
	mu    sync.Mutex
	cmd   *exec.Cmd
	stdin io.WriteCloser
	lines chan *string // nil = shell died
	cwd   string
	alive atomic.Bool
}

// maxShellLine caps materialization of a single reader line at 1MB: a
// runaway background job printing one gigantic line must not balloon RSS
// before the 8MB call-cap can drop it. The line keeps being consumed (pipe
// flow) but the excess is discarded.
const maxShellLine = 1 << 20

func newLineChan() chan *string { return make(chan *string, 65536) }

func (s *warmShell) spawnLocked(cwd string) {
	if cwd == "" {
		cwd, _ = os.Getwd()
	}
	if s.stdin != nil { // drop the old shell's stdin before replacing it
		_ = s.stdin.Close()
		s.stdin = nil
	}
	s.cwd = cwd
	cmd := exec.Command("bash", "--noprofile", "--norc")
	cmd.Dir = cwd
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		s.alive.Store(false)
		return
	}
	pr, pw, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		s.alive.Store(false)
		return
	}
	cmd.Stdout = pw
	cmd.Stderr = pw
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = pr.Close()
		_ = pw.Close()
		s.alive.Store(false)
		return
	}
	_ = pw.Close() // reader owns the pipe now
	s.cmd = cmd
	s.stdin = stdin
	s.lines = newLineChan()
	s.alive.Store(true)
	ch := s.lines
	go func() {
		// Sole reaper: after EOF the shell is gone — close the pipe and
		// Wait() to reap the zombie, then report the death. No one else
		// may Wait (double-Wait is an error).
		r := bufio.NewReaderSize(pr, 64<<10)
		// readLine consumes exactly one line. Long lines are ReadSliced in
		// fragments; materialization stops at maxShellLine but consumption
		// continues to the newline so the pipe never backs up.
		readLine := func() (string, bool) {
			var sb strings.Builder
			over := false
			for {
				frag, err := r.ReadSlice('\n')
				if len(frag) > 0 && !over {
					if sb.Len()+len(frag) > maxShellLine {
						if keep := maxShellLine - sb.Len(); keep > 0 {
							sb.Write(frag[:keep])
						}
						over = true
					} else {
						sb.Write(frag)
					}
				}
				if err == nil {
					return sb.String(), true
				}
				if err == bufio.ErrBufferFull {
					continue // line continues past the buffer
				}
				return sb.String(), sb.Len() > 0 // EOF: emit partial, then done
			}
		}
		for {
			line, ok := readLine()
			if ok {
				l := strings.TrimRight(line, "\n")
				ch <- &l
			}
			if !ok {
				_ = pr.Close()
				_ = cmd.Wait()
				s.alive.Store(false)
				ch <- nil
				return
			}
		}
	}()
}

func (s *warmShell) killLocked() {
	if s.alive.Load() && s.cmd != nil && s.cmd.Process != nil {
		_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL) // whole tree
		_ = s.cmd.Process.Kill()
	}
	s.alive.Store(false)
	// No Wait here: the reader goroutine of this very shell reaps it once
	// the pipe hits EOF. killLocked only signals.
}

func (s *warmShell) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.killLocked()
	cwd, _ := os.Getwd()
	s.spawnLocked(cwd)
}

// run executes one command. rc: 124 timeout, -1 shell died. Never retries.
// The third return reports that output hit the 8MB cap (notice appended).
func (s *warmShell) run(command, cwd string, timeout time.Duration) (string, int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.alive.Load() {
		s.spawnLocked(cwd)
		bump("shell_respawns")
	}
	if !s.alive.Load() {
		return "", -1, false
	}
	// drain strays from backgrounded jobs of earlier calls; a nil sentinel
	// means the shell died between calls — respawn BEFORE writing the frame
	// instead of misreporting "shell died mid-command".
	for {
		select {
		case lp := <-s.lines:
			if lp == nil {
				s.spawnLocked(cwd)
				bump("shell_respawns")
				if !s.alive.Load() {
					return "", -1, false
				}
			}
		default:
			goto drained
		}
	}
drained:
	if cwd != "" {
		if abs, err := filepath.Abs(cwd); err == nil && abs != s.cwd {
			command = "cd " + shq(cwd) + " && { " + command + "\n}"
		}
	}
	mid := randHex(6)
	begin := "TRB" + mid
	endP := "TRE" + mid
	// Markers lead with \n so unterminated output can never glue them onto
	// the last line (the begin marker's leading \n also terminates any stray
	// unterminated line from a previous background job). END carries rc AND
	// $PWD: real cwd read back from the shell, so `cd` inside a command can
	// never desync the tracker.
	frame := "printf '\\n%s\\n' '" + begin + "'; { " + command + "\n}; _rc=$?; " +
		"printf '\\n%s:%d:%s\\n' '" + endP + "' $_rc \"$PWD\"\n"
	// The frame write must not hold the shell hostage: a stopped (SIGSTOP)
	// shell with a frame bigger than the 64KB pipe buffer would block
	// forever, deadlocking every lane behind s.mu. Write in a goroutine and
	// race it against the call's remaining deadline; on timeout kill the
	// tree — the blocked writer then unblocks with EPIPE and exits (the
	// channel is buffered, so no goroutine leaks).
	deadline := time.Now().Add(timeout)
	writeErr := make(chan error, 1)
	go func() {
		_, err := io.WriteString(s.stdin, frame)
		writeErr <- err
	}()
	wt := time.NewTimer(time.Until(deadline))
	select {
	case err := <-writeErr:
		wt.Stop()
		if err != nil {
			s.alive.Store(false)
			return "", -1, false
		}
	case <-wt.C:
		s.killLocked()
		return fmt.Sprintf("\n[timed out after %ds — "+
			"command tree killed, shell will respawn]", int(timeout.Seconds())), 124, false
	}
	var out strings.Builder
	nbytes := 0
	truncated := false
	rc := -1
	begun := false // nothing is output before the begin marker line
	endMarker := endP + ":"
	for {
		remain := time.Until(deadline)
		if remain <= 0 {
			s.killLocked()
			out.WriteString(fmt.Sprintf("\n[timed out after %ds — "+
				"command tree killed, shell will respawn]", int(timeout.Seconds())))
			return out.String(), 124, truncated
		}
		timer := time.NewTimer(remain)
		select {
		case lp := <-s.lines:
			timer.Stop()
			if lp == nil { // shell died mid-command; do NOT retry
				s.alive.Store(false)
				return out.String(), -1, truncated
			}
			line := *lp
			if !begun {
				// pre-begin state: the begin marker's leading \n surfaces
				// as an empty line — discard it (and any other empties) so
				// it never counts as command output.
				if line == "" {
					continue
				}
				if line == begin {
					begun = true
					continue
				}
				// stray terminated line from an earlier background job:
				// pass it through as output
			}
			if strings.HasPrefix(line, endMarker) {
				parts := strings.SplitN(line, ":", 3)
				if len(parts) == 3 {
					if v, err := strconv.Atoi(parts[1]); err == nil {
						rc = v
					}
					if parts[2] != "" {
						s.cwd = parts[2]
					}
				}
				return out.String(), rc, truncated
			}
			// Cap accounting reserves room for the notice so the final
			// stdout stays within maxOut even after appending it.
			if truncated || nbytes+len(line)+1 > maxOut-len(outTruncNotice) {
				if !truncated {
					out.WriteString(outTruncNotice)
					nbytes += len(outTruncNotice)
					truncated = true
				}
				continue // bounded parser memory: drop, keep consuming
			}
			nbytes += len(line) + 1
			out.WriteString(line)
			out.WriteByte('\n')
		case <-timer.C:
			s.killLocked()
			out.WriteString(fmt.Sprintf("\n[timed out after %ds — "+
				"command tree killed, shell will respawn]", int(timeout.Seconds())))
			return out.String(), 124, truncated
		}
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

var shell = &warmShell{}

func warmExec(command, cwd string, timeoutSec int, reset bool) string {
	if strings.TrimSpace(command) == "" {
		return jerr("command must be a non-empty string")
	}
	if strings.ContainsAny(cwd, "\n\r") {
		// a newline in $PWD would split the END marker and corrupt cwd
		// tracking (verified wrong-directory execution)
		return jerr("cwd must not contain newline characters")
	}
	if timeoutSec < 1 {
		timeoutSec = 1
	}
	if timeoutSec > 3600 {
		timeoutSec = 3600
	}
	bump("warm_exec")
	if !usePersist {
		// negative control: spawn-per-call. Combined stdout+stderr capped
		// at maxOut with the same notice as the warm path.
		ctx, cancel := context.WithTimeout(context.Background(),
			time.Duration(timeoutSec)*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "bash", "--noprofile", "--norc", "-c", command)
		if cwd != "" {
			cmd.Dir = cwd
		}
		var so, se bytes.Buffer
		perStream := (maxOut - len(outTruncNotice)) / 2
		cmd.Stdout = &limitedWriter{w: &so, max: perStream}
		cmd.Stderr = &limitedWriter{w: &se, max: perStream}
		err := cmd.Run()
		if ctx.Err() == context.DeadlineExceeded {
			return jmap(map[string]any{"success": false, "mode": "spawn",
				"stdout": "", "exit_code": 124, "truncated": false,
				"error": fmt.Sprintf("timed out after %ds", timeoutSec)})
		}
		rc := 0
		if ee, ok := err.(*exec.ExitError); ok {
			rc = ee.ExitCode()
		} else if err != nil {
			rc = -1
		}
		truncated := so.Len() >= perStream || se.Len() >= perStream
		stdout := so.String() + se.String()
		if truncated {
			stdout += outTruncNotice
		}
		return jmap(map[string]any{"success": rc == 0, "mode": "spawn",
			"stdout": stdout, "exit_code": rc, "truncated": truncated})
	}
	if reset {
		shell.reset()
	}
	out, rc, truncated := shell.run(command, cwd, time.Duration(timeoutSec)*time.Second)
	d := map[string]any{"success": rc == 0, "mode": "persist",
		"stdout": strings.TrimRight(out, "\n"), "exit_code": rc,
		"truncated": truncated}
	if rc == 124 {
		d["error"] = fmt.Sprintf("timed out after %ds", timeoutSec)
	} else if rc == -1 {
		d["error"] = "shell died mid-command (not retried); next call respawns"
	}
	return jmap(d)
}

// --------------------------------------------------------------- batch_exec

func batchExec(commands []string, cwd string, timeoutSec int) string {
	if len(commands) == 0 {
		return jerr("commands must be a non-empty list of strings")
	}
	if len(commands) > batchMax {
		return jerr(fmt.Sprintf("batch too large: %d > %d — split it", len(commands), batchMax))
	}
	if strings.ContainsAny(cwd, "\n\r") {
		return jerr("cwd must not contain newline characters")
	}
	for i, c := range commands {
		if strings.TrimSpace(c) == "" {
			return jerr(fmt.Sprintf("command %d invalid: must be a non-empty string", i))
		}
	}
	bump("batch_exec")
	results := make([]json.RawMessage, len(commands))
	fresh := false
	allOK := true
	for i, c := range commands {
		// cwd applies to the FIRST command only: subsequent commands
		// inherit the shell's state (cd/export flow between commands).
		cwdArg := ""
		if i == 0 {
			cwdArg = cwd
		}
		r := warmExec(c, cwdArg, timeoutSec, false)
		if fresh {
			var m map[string]any
			if json.Unmarshal([]byte(r), &m) == nil {
				m["note"] = "ran on a fresh shell — prior command killed the warm one"
				r = jmap(m)
			}
		}
		results[i] = json.RawMessage(r)
		var m map[string]any
		if json.Unmarshal([]byte(r), &m) == nil {
			if rc, ok := m["exit_code"].(float64); !ok || int(rc) != 0 {
				allOK = false
			}
			if rc, ok := m["exit_code"].(float64); ok && (int(rc) == 124 || int(rc) == -1) {
				fresh = true
			}
		}
	}
	out, _ := json.Marshal(map[string]any{"success": allOK, "results": results})
	return string(out)
}

// ------------------------------------------------------------------- doctor

var rgVerOnce sync.Once
var rgVer string

func doctor() string {
	rgP := rg()
	if rgP != "" {
		rgVerOnce.Do(func() {
			out, err := exec.Command(rgP, "--version").Output()
			if err == nil {
				rgVer = strings.SplitN(string(out), "\n", 2)[0]
			} else {
				rgVer = "unreadable"
			}
		})
	}
	statsMu.Lock()
	snap := make(map[string]int, len(stats))
	for k, v := range stats {
		snap[k] = v
	}
	statsMu.Unlock()
	// atomic alive: doctor never blocks behind a long-running command
	alive := shell.alive.Load()
	return jmap(map[string]any{
		"success": true, "version": version,
		"go":       strings.TrimPrefix(runtime.Version(), "go"),
		"platform": runtime.GOOS,
		"kill_switches": map[string]bool{
			"TOOLRUSH_FASTLANE": useFastlane, "TOOLRUSH_SEARCH": useSearch,
			"TOOLRUSH_PERSIST": usePersist, "TOOLRUSH_PARALLEL": useParallel},
		"rg": rgP, "rg_version": rgVer,
		"warm_shell_alive": alive,
		"stats":            snap,
	})
}

// ------------------------------------------------------------ MCP transport

var toolsList json.RawMessage

func init() {
	desc := func(s string) string { return s }
	tools := []map[string]any{
		{"name": "fast_read",
			"description": desc("Fast in-process file read with line-number gutter " +
				"(<lineno>\\t<content>), offset/limit paging, negative " +
				"offset = tail, binary sniff, BOM strip. Read-only."),
			"inputSchema": map[string]any{"type": "object",
				"properties": map[string]any{
					"path":   map[string]any{"type": "string"},
					"offset": map[string]any{"type": "integer", "default": 1},
					"limit":  map[string]any{"type": "integer", "default": 1000}},
				"required": []string{"path"}}},
		{"name": "batch_read",
			"description": desc("Read 1-16 files in ONE call (serial by design — " +
				"measured faster than pooling on page-cache-fast storage). " +
				"Input order preserved; the whole batch is validated before " +
				"anything runs. Each op: {path, offset?, limit?}."),
			"inputSchema": map[string]any{"type": "object",
				"properties": map[string]any{
					"ops": map[string]any{"type": "array", "items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"path":   map[string]any{"type": "string"},
							"offset": map[string]any{"type": "integer"},
							"limit":  map[string]any{"type": "integer"}},
						"required": []string{"path"}}}},
				"required": []string{"ops"}}},
		{"name": "fast_search",
			"description": desc("Content search via direct ripgrep transport " +
				"(respects .gitignore, real regex grammar, --json parsing). " +
				"Falls back to a pure walk when rg is unavailable. context=N " +
				"(0-5) adds N lines of gutter around each hit: \"L-line\" " +
				"before, \"L+line\" after. Envelope always carries \"capped\": " +
				"true means total_hits is a lower bound (8MB output cap hit)."),
			"inputSchema": map[string]any{"type": "object",
				"properties": map[string]any{
					"pattern":        map[string]any{"type": "string"},
					"path":           map[string]any{"type": "string"},
					"file_glob":      map[string]any{"type": "string"},
					"case_sensitive": map[string]any{"type": "boolean", "default": true},
					"context":        map[string]any{"type": "integer", "default": 0},
					"limit":          map[string]any{"type": "integer", "default": 100},
					"offset":         map[string]any{"type": "integer", "default": 0}},
				"required": []string{"pattern", "path"}}},
		{"name": "batch_search",
			"description": desc("Run 1-16 content searches in ONE call, fanned out " +
				"across goroutines — each op spawns its own rg process, so " +
				"the batch wall time approaches the slowest op, not the sum " +
				"(unlike batch_read, serial by design). Input order " +
				"preserved; the whole batch is validated before anything " +
				"runs; per-op failures are isolated (that op's result is an " +
				"error envelope). Each op envelope carries \"capped\": true " +
				"means that op's total_hits is a lower bound. Each op: " +
				"{pattern, path, file_glob?, case_sensitive?, context?, " +
				"limit?, offset?}."),
			"inputSchema": map[string]any{"type": "object",
				"properties": map[string]any{
					"ops": map[string]any{"type": "array", "items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"pattern":        map[string]any{"type": "string"},
							"path":           map[string]any{"type": "string"},
							"file_glob":      map[string]any{"type": "string"},
							"case_sensitive": map[string]any{"type": "boolean"},
							"context":        map[string]any{"type": "integer"},
							"limit":          map[string]any{"type": "integer"},
							"offset":         map[string]any{"type": "integer"}},
						"required": []string{"pattern", "path"}}}},
				"required": []string{"ops"}}},
		{"name": "fast_tree",
			"description": desc("Budgeted directory listing: deterministic " +
				"dirs-first, alphabetical order; depth (1-10, default 3) and " +
				"entry (1-5000, default 500) budgets; optional basename " +
				"pattern filter (invalid patterns error; dirs matching the " +
				"filter are still descended). Entries always carry " +
				"is_symlink and use lstat semantics: a symlink reports its " +
				"own size/mtime, dangling links are listed, and symlinked " +
				"dirs are never descended into. Skips .git, node_modules, " +
				"__pycache__, .venv, target, dist, build and *_cache. No " +
				".gitignore parsing — use pattern= to narrow."),
			"inputSchema": map[string]any{"type": "object",
				"properties": map[string]any{
					"path":        map[string]any{"type": "string"},
					"max_depth":   map[string]any{"type": "integer", "default": 3},
					"max_entries": map[string]any{"type": "integer", "default": 500},
					"pattern":     map[string]any{"type": "string"}},
				"required": []string{"path"}}},
		{"name": "warm_exec",
			"description": desc("Run a shell command on ONE persistent bash: cwd, " +
				"exports and shell state survive across calls (unlike " +
				"per-call spawns). Same privileges as the harness " +
				"terminal. Timeout kills the whole command tree; " +
				"commands are never retried. reset=true restarts the " +
				"shell fresh."),
			"inputSchema": map[string]any{"type": "object",
				"properties": map[string]any{
					"command": map[string]any{"type": "string"},
					"cwd":     map[string]any{"type": "string"},
					"timeout": map[string]any{"type": "integer", "default": 120},
					"reset":   map[string]any{"type": "boolean", "default": false}},
				"required": []string{"command"}}},
		{"name": "batch_exec",
			"description": desc("Run 1-16 shell commands through ONE call on the warm " +
				"shell, sequentially — cd/export state flows between " +
				"commands. Per-command stdout+exit_code; a timed-out " +
				"command's tree is killed and later commands run on a " +
				"fresh shell (flagged). Prefer this over N separate " +
				"exec calls."),
			"inputSchema": map[string]any{"type": "object",
				"properties": map[string]any{
					"commands": map[string]any{"type": "array",
						"items": map[string]any{"type": "string"}},
					"cwd":     map[string]any{"type": "string"},
					"timeout": map[string]any{"type": "integer", "default": 120}},
				"required": []string{"commands"}}},
		{"name": "doctor",
			"description": desc("Report ToolRush lane status: versions, kill-switch " +
				"state, rg availability, warm-shell liveness, counters."),
			"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}}},
	}
	b, _ := json.Marshal(map[string]any{"tools": tools})
	toolsList = b
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   any             `json:"error,omitempty"`
}

var stdoutMu sync.Mutex

func reply(id json.RawMessage, result any, rpcErr any) {
	resp := rpcResponse{JSONRPC: "2.0", ID: id, Result: result, Error: rpcErr}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(resp); err != nil {
		return
	}
	stdoutMu.Lock()
	_, _ = os.Stdout.Write(buf.Bytes()) // one locked Write: never interleaved
	stdoutMu.Unlock()
}

func rpcError(code int, msg string) map[string]any {
	return map[string]any{"code": code, "message": msg}
}

func toolResult(text string, isErr bool) map[string]any {
	return map[string]any{
		"content": []map[string]string{{"type": "text", "text": text}},
		"isError": isErr,
	}
}

func handle(req rpcRequest) {
	defer func() {
		if r := recover(); r != nil {
			reply(req.ID, nil, rpcError(-32603, fmt.Sprintf("%v", r)))
		}
	}()
	switch req.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &p)
		pv := p.ProtocolVersion
		if pv == "" {
			pv = "2024-11-05"
		}
		reply(req.ID, map[string]any{
			"protocolVersion": pv,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": "toolrush", "version": version},
		}, nil)
	case "ping":
		reply(req.ID, map[string]any{}, nil)
	case "tools/list":
		var raw map[string]json.RawMessage
		_ = json.Unmarshal(toolsList, &raw)
		reply(req.ID, raw, nil)
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		_ = json.Unmarshal(req.Params, &p)
		text, isErr, unknown := dispatchTool(p.Name, p.Arguments)
		if unknown {
			reply(req.ID, nil, rpcError(-32602, "unknown tool: "+p.Name))
			return
		}
		reply(req.ID, toolResult(text, isErr), nil)
	default:
		reply(req.ID, nil, rpcError(-32601, "method not found: "+req.Method))
	}
}

// envIsError derives MCP isError from the parsed tool envelope rather than
// sniffing its first bytes: true when success==false or error is non-empty.
// Envelopes without a success field and no error (none today) stay false.
func envIsError(r string) bool {
	var m struct {
		Success *bool  `json:"success"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal([]byte(r), &m); err != nil {
		return true
	}
	return (m.Success != nil && !*m.Success) || m.Error != ""
}

// dispatchTool returns (envelopeText, isError, unknownTool).
func dispatchTool(name string, args json.RawMessage) (string, bool, bool) {
	switch name {
	case "fast_read":
		var a struct {
			Path   string `json:"path"`
			Offset *int   `json:"offset"`
			Limit  *int   `json:"limit"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return jerr("invalid arguments: " + err.Error()), true, false
		}
		if a.Path == "" {
			return jerr("path must be a non-empty string"), true, false
		}
		off, lim := 1, readDefaultLimit
		if a.Offset != nil {
			off = *a.Offset
		}
		if a.Limit != nil {
			lim = *a.Limit
		}
		r := fastRead(a.Path, off, lim)
		return r, envIsError(r), false
	case "batch_read":
		var a struct {
			Ops []readOp `json:"ops"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return jerr("invalid arguments: " + err.Error()), true, false
		}
		r := batchRead(a.Ops)
		return r, envIsError(r), false
	case "fast_search":
		var a struct {
			Pattern       string `json:"pattern"`
			Path          string `json:"path"`
			FileGlob      string `json:"file_glob"`
			CaseSensitive *bool  `json:"case_sensitive"`
			Context       *int   `json:"context"`
			Limit         *int   `json:"limit"`
			Offset        *int   `json:"offset"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return jerr("invalid arguments: " + err.Error()), true, false
		}
		cs := true
		if a.CaseSensitive != nil {
			cs = *a.CaseSensitive
		}
		lim, off := 100, 0
		if a.Limit != nil {
			lim = *a.Limit
		}
		if a.Offset != nil {
			off = *a.Offset
		}
		ctx := 0
		if a.Context != nil {
			ctx = *a.Context
		}
		r := fastSearch(a.Pattern, a.Path, a.FileGlob, cs, lim, off, ctx)
		return r, envIsError(r), false
	case "batch_search":
		var a struct {
			Ops []searchOp `json:"ops"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return jerr("invalid arguments: " + err.Error()), true, false
		}
		r := batchSearch(a.Ops)
		return r, envIsError(r), false
	case "fast_tree":
		var a struct {
			Path       string `json:"path"`
			MaxDepth   *int   `json:"max_depth"`
			MaxEntries *int   `json:"max_entries"`
			Pattern    string `json:"pattern"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return jerr("invalid arguments: " + err.Error()), true, false
		}
		if a.Path == "" {
			return jerr("path must be a non-empty string"), true, false
		}
		depth, entries := 3, 500
		if a.MaxDepth != nil {
			depth = *a.MaxDepth
		}
		if a.MaxEntries != nil {
			entries = *a.MaxEntries
		}
		r := fastTree(a.Path, depth, entries, a.Pattern)
		return r, envIsError(r), false
	case "warm_exec":
		var a struct {
			Command string `json:"command"`
			Cwd     string `json:"cwd"`
			Timeout *int   `json:"timeout"`
			Reset   bool   `json:"reset"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return jerr("invalid arguments: " + err.Error()), true, false
		}
		to := execDefaultTimeout
		if a.Timeout != nil {
			to = *a.Timeout
		}
		r := warmExec(a.Command, a.Cwd, to, a.Reset)
		return r, envIsError(r), false
	case "batch_exec":
		var a struct {
			Commands []string `json:"commands"`
			Cwd      string   `json:"cwd"`
			Timeout  *int     `json:"timeout"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return jerr("invalid arguments: " + err.Error()), true, false
		}
		to := execDefaultTimeout
		if a.Timeout != nil {
			to = *a.Timeout
		}
		r := batchExec(a.Commands, a.Cwd, to)
		return r, envIsError(r), false
	case "doctor":
		return doctor(), false, false
	}
	return "", false, true
}

func main() {
	fmt.Fprintf(os.Stderr, "[toolrush] toolrush-mcp %s ready (pid %d)\n",
		version, os.Getpid())
	r := bufio.NewReader(os.Stdin)
	var wg sync.WaitGroup // in-flight handlers; drained before exit
	for {
		line, err := r.ReadBytes('\n')
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) > 0 {
			var req rpcRequest
			if json.Unmarshal(trimmed, &req) != nil {
				reply(nil, nil, rpcError(-32700, "parse error"))
			} else if len(req.ID) == 0 || string(req.ID) == "null" {
				// notification — no reply
			} else if req.Method == "initialize" {
				handle(req) // synchronous: its reply must precede all others
			} else {
				// per-request goroutine: a long warm_exec never
				// head-of-line blocks fast_read behind it
				wg.Add(1)
				go func() {
					defer wg.Done()
					handle(req)
				}()
			}
		}
		if err != nil {
			// stdin closed (pipe-driven use, harness shutdown): wait for
			// in-flight handlers so their replies are not lost on exit
			wg.Wait()
			return
		}
	}
}
