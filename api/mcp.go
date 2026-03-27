package handler

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// ── constants ─────────────────────────────────────────────────────────────────

const (
	serverName = "rose"
	serverVer  = "7.0.0"

	ghBase     = "https://api.github.com"
	ghVersion  = "2022-11-28"
	brevoBase  = "https://api.brevo.com/v3"
	calBase    = "https://api.cal.com/v2"
	calVersion = "2024-08-13"
	tgBase     = "https://api.telegram.org/bot"
	renderBase = "https://api.render.com/v1"
	liBase     = "https://api.linkedin.com"
)

// env returns the param value from the tool call, falling back to an environment variable.
// This allows Vercel env vars to act as default credentials so Claude need not supply them.
func env(p map[string]any, key, envVar string) string {
	if v, ok := p[key].(string); ok && v != "" {
		return v
	}
	return os.Getenv(envVar)
}

var calEventTypes = map[int]int{15: 4513949, 30: 4513947}

// ── Telegram in-memory state ──────────────────────────────────────────────────
// tgKnownUsers accumulates chat IDs seen via tg.updates, enabling tg.broadcast.
// tgAutoReplies stores keyword→response rules for webhook-triggered auto-replies.
// Both reset on Vercel cold start; persist externally via db.set if needed.

type tgAutoReply struct {
	Response  string
	MatchType string // "exact" | "contains"
}

var (
	tgKnownUsers  = map[string]bool{}
	tgUsersMu     sync.RWMutex
	tgAutoReplies = map[string]tgAutoReply{}
	tgAutoReplyMu sync.RWMutex
)

// titleCase returns the string with the first letter uppercased.
func titleCase(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// stripHTMLTags removes HTML tags from a string for token-efficient output.
func stripHTMLTags(s string) string {
	var b strings.Builder
	inTag := false
	for _, r := range s {
		if r == '<' {
			inTag = true
			continue
		}
		if r == '>' {
			inTag = false
			b.WriteRune(' ')
			continue
		}
		if !inTag {
			b.WriteRune(r)
		}
	}
	// Collapse multiple spaces/newlines
	result := b.String()
	for strings.Contains(result, "  ") {
		result = strings.ReplaceAll(result, "  ", " ")
	}
	return strings.TrimSpace(result)
}

// ── MCP wire types ────────────────────────────────────────────────────────────

type MCPRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type MCPResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id,omitempty"`
	Result  any       `json:"result,omitempty"`
	Error   *MCPError `json:"error,omitempty"`
}

type MCPError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type TextContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type ToolResult struct {
	Content []TextContent `json:"content"`
}

type ToolDef struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// ── result helpers ────────────────────────────────────────────────────────────

func textResult(v any) ToolResult {
	b, _ := json.MarshalIndent(v, "", "  ")
	return ToolResult{Content: []TextContent{{Type: "text", Text: string(b)}}}
}

func errResult(msg string) ToolResult {
	return textResult(map[string]any{"ok": false, "error": msg})
}

// ── param helpers ─────────────────────────────────────────────────────────────

func str(m map[string]any, k, def string) string {
	if v, ok := m[k].(string); ok && v != "" {
		return v
	}
	return def
}

func num(m map[string]any, k string, def float64) float64 {
	switch v := m[k].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	}
	return def
}

func boolVal(m map[string]any, k string, def bool) bool {
	if v, ok := m[k].(bool); ok {
		return v
	}
	return def
}

func strSlice(m map[string]any, k string) []string {
	raw, ok := m[k].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func strMap(m map[string]any, k string) map[string]string {
	raw, ok := m[k].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(raw))
	for ck, cv := range raw {
		out[ck] = fmt.Sprintf("%v", cv)
	}
	return out
}

func anyMap(m map[string]any, k string) map[string]any {
	if v, ok := m[k].(map[string]any); ok {
		return v
	}
	return nil
}

// ── generic HTTP client ───────────────────────────────────────────────────────

func httpDo(method, rawURL string, headers map[string]string, body map[string]any, bodyType string) (map[string]any, error) {
	var bodyReader io.Reader
	contentType := ""

	if body != nil {
		switch bodyType {
		case "form":
			p := url.Values{}
			for k, v := range body {
				p.Set(k, fmt.Sprintf("%v", v))
			}
			bodyReader = strings.NewReader(p.Encode())
			contentType = "application/x-www-form-urlencoded"
		case "raw":
			raw, _ := body["raw"]
			bodyReader = strings.NewReader(fmt.Sprintf("%v", raw))
			contentType = "text/plain"
		default:
			b, _ := json.Marshal(body)
			bodyReader = bytes.NewReader(b)
			contentType = "application/json"
		}
	}

	req, err := http.NewRequest(method, rawURL, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		parsed = string(raw)
	}
	return map[string]any{
		"status": resp.StatusCode,
		"ok":     resp.StatusCode >= 200 && resp.StatusCode < 300,
		"body":   parsed,
	}, nil
}

// ── GitHub client ─────────────────────────────────────────────────────────────

func ghDo(method, path, token string, body map[string]any) (map[string]any, error) {
	if token == "" {
		token = os.Getenv("GITHUB_TOKEN")
	}
	var bodyReader io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, ghBase+path, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-GitHub-Api-Version", ghVersion)
	req.Header.Set("Accept", "application/vnd.github+json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var parsed any
	_ = json.Unmarshal(raw, &parsed)
	return map[string]any{"status": resp.StatusCode, "ok": resp.StatusCode >= 200 && resp.StatusCode < 300, "body": parsed}, nil
}

// ghGetFileSHA fetches the current blob SHA for a file path.
// Returns ("", nil) if the file does not exist (new file).
// This is called internally by github.file.write and github.file.delete —
// Claude never needs to make a separate SHA-fetching call.
func ghGetFileSHA(token, owner, repo, path, branch string) (string, error) {
	p := fmt.Sprintf("/repos/%s/%s/contents/%s", owner, repo, path)
	if branch != "" {
		p += "?ref=" + url.QueryEscape(branch)
	}
	res, err := ghDo("GET", p, token, nil)
	if err != nil {
		return "", err
	}
	status, _ := res["status"].(int)
	if status == 404 {
		return "", nil // new file
	}
	if b, ok := res["body"].(map[string]any); ok {
		if sha, ok := b["sha"].(string); ok {
			return sha, nil
		}
	}
	return "", nil
}

// ── Cal.com v2 client (direct) ────────────────────────────────────────────────
// Auth: Authorization: Bearer <cal_api_key>
// Header: cal-api-version: 2024-08-13 (slots use 2024-09-04)

func calRequest(apiKey, path, method string, body any, version string) (map[string]any, error) {
	if apiKey == "" {
		apiKey = os.Getenv("CAL_API_KEY")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("cal_api_key is required (or set CAL_API_KEY env var)")
	}
	if version == "" {
		version = calVersion
	}
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, calBase+path, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("cal-api-version", version)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var parsed any
	_ = json.Unmarshal(raw, &parsed)
	return map[string]any{"status": resp.StatusCode, "ok": resp.StatusCode >= 200 && resp.StatusCode < 300, "body": parsed}, nil
}

// ── Brevo client ──────────────────────────────────────────────────────────────

func brevoDo(method, path, apiKey string, body any) (map[string]any, error) {
	if apiKey == "" {
		apiKey = os.Getenv("BREVO_API_KEY")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("brevo_api_key is required (or set BREVO_API_KEY env var)")
	}
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, brevoBase+path, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("api-key", apiKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var parsed any
	_ = json.Unmarshal(raw, &parsed)
	return map[string]any{"status": resp.StatusCode, "ok": resp.StatusCode >= 200 && resp.StatusCode < 300, "body": parsed}, nil
}

// ── shared schema fragments ───────────────────────────────────────────────────

var calKeyProp = map[string]any{"type": "string", "description": "Cal.com API key"}

var httpBodySchema = map[string]any{
	"type": "object",
	"properties": map[string]any{
		"url":       map[string]any{"type": "string", "format": "uri"},
		"headers":   map[string]any{"type": "object", "description": "Extra request headers"},
		"body":      map[string]any{"type": "object"},
		"body_type": map[string]any{"type": "string", "enum": []string{"json", "form", "raw"}, "default": "json"},
	},
	"required": []string{"url"},
}

// ghProps builds the standard {github_token, owner, repo} property block with optional extras.
func ghProps(extra map[string]any) map[string]any {
	base := map[string]any{
		"github_token": map[string]any{"type": "string", "description": "GitHub PAT"},
		"owner":        map[string]any{"type": "string", "description": "Repo owner (user or org)"},
		"repo":         map[string]any{"type": "string"},
	}
	for k, v := range extra {
		base[k] = v
	}
	return base
}

// ── tool registry ─────────────────────────────────────────────────────────────

type HandlerFunc func(params map[string]any) (ToolResult, error)

type Tool struct {
	Def     ToolDef
	Handler HandlerFunc
}

func buildTools() []Tool {
	return []Tool{

		// ════════════════════════════════════════════════════════════════════
		// HTTP — generic, pass Authorization in headers as needed
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name:        "GET",
				Description: "HTTP GET [url]. Use headers for auth.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"url":     map[string]any{"type": "string", "format": "uri"},
						"headers": map[string]any{"type": "object"},
					},
					"required": []string{"url"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := httpDo("GET", str(p, "url", ""), strMap(p, "headers"), nil, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{Name: "POST", Description: "HTTP POST [url] with json/form/raw body.", InputSchema: httpBodySchema},
			Handler: func(p map[string]any) (ToolResult, error) {
				body, _ := p["body"].(map[string]any)
				res, err := httpDo("POST", str(p, "url", ""), strMap(p, "headers"), body, str(p, "body_type", "json"))
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{Name: "PUT", Description: "HTTP PUT [url].", InputSchema: httpBodySchema},
			Handler: func(p map[string]any) (ToolResult, error) {
				body, _ := p["body"].(map[string]any)
				res, err := httpDo("PUT", str(p, "url", ""), strMap(p, "headers"), body, str(p, "body_type", "json"))
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{Name: "PATCH", Description: "HTTP PATCH [url].", InputSchema: httpBodySchema},
			Handler: func(p map[string]any) (ToolResult, error) {
				body, _ := p["body"].(map[string]any)
				res, err := httpDo("PATCH", str(p, "url", ""), strMap(p, "headers"), body, str(p, "body_type", "json"))
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "DELETE",
				Description: "HTTP DELETE [url].",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"url":     map[string]any{"type": "string", "format": "uri"},
						"headers": map[string]any{"type": "object"},
					},
					"required": []string{"url"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := httpDo("DELETE", str(p, "url", ""), strMap(p, "headers"), nil, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "HEAD",
				Description: "HTTP HEAD [url] — returns status + headers only.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"url":     map[string]any{"type": "string", "format": "uri"},
						"headers": map[string]any{"type": "object"},
					},
					"required": []string{"url"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				req, err := http.NewRequest("HEAD", str(p, "url", ""), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				for k, v := range strMap(p, "headers") {
					req.Header.Set(k, v)
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return errResult(err.Error()), nil
				}
				defer resp.Body.Close()
				hdrs := map[string]string{}
				for k, vs := range resp.Header {
					hdrs[k] = strings.Join(vs, ", ")
				}
				return textResult(map[string]any{"status": resp.StatusCode, "ok": resp.StatusCode < 300, "headers": hdrs}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// GitHub ── Repos
		// All file tools auto-fetch SHAs internally. Claude never needs a
		// separate GET-for-SHA call. Responses trimmed to essentials only.
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name:        "github.repo.create",
				Description: "Create a new GitHub repo. Returns {ok, name, url, clone_url, default_branch}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"github_token": map[string]any{"type": "string", "description": "GitHub PAT"},
						"name":         map[string]any{"type": "string"},
						"description":  map[string]any{"type": "string"},
						"private":      map[string]any{"type": "boolean", "default": false},
						"auto_init":    map[string]any{"type": "boolean", "default": true, "description": "Commit a README so repo is non-empty"},
						"org":          map[string]any{"type": "string", "description": "Org login (omit for personal repo)"},
					},
					"required": []string{"github_token", "name"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{
					"name":      str(p, "name", ""),
					"private":   boolVal(p, "private", false),
					"auto_init": boolVal(p, "auto_init", true),
				}
				if d := str(p, "description", ""); d != "" {
					body["description"] = d
				}
				path := "/user/repos"
				if org := str(p, "org", ""); org != "" {
					path = "/orgs/" + org + "/repos"
				}
				res, err := ghDo("POST", path, str(p, "github_token", ""), body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["name"] = b["name"]
					out["url"] = b["html_url"]
					out["clone_url"] = b["clone_url"]
					out["default_branch"] = b["default_branch"]
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name: "github.repo.find",
				Description: "Fetch repo metadata AND its top-level file listing in one call. " +
					"Always call this first when you need to know what files exist. " +
					"Returns {name, url, default_branch, private, files[{name,path,type,size}]}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": ghProps(map[string]any{
						"branch": map[string]any{"type": "string", "description": "Branch ref (omit for default)"},
					}),
					"required": []string{"github_token", "owner", "repo"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := str(p, "github_token", "")
				owner := str(p, "owner", "")
				repo := str(p, "repo", "")

				repoRes, err := ghDo("GET", "/repos/"+owner+"/"+repo, token, nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": repoRes["ok"]}
				var defaultBranch string
				if b, ok := repoRes["body"].(map[string]any); ok {
					defaultBranch, _ = b["default_branch"].(string)
					out["name"] = b["name"]
					out["url"] = b["html_url"]
					out["default_branch"] = defaultBranch
					out["private"] = b["private"]
					out["description"] = b["description"]
					out["language"] = b["language"]
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
						return textResult(out), nil
					}
				}

				branch := str(p, "branch", defaultBranch)
				treePath := fmt.Sprintf("/repos/%s/%s/contents", owner, repo)
				if branch != "" {
					treePath += "?ref=" + url.QueryEscape(branch)
				}
				treeRes, err := ghDo("GET", treePath, token, nil)
				if err == nil {
					if arr, ok := treeRes["body"].([]any); ok {
						var files []map[string]any
						for _, item := range arr {
							if f, ok := item.(map[string]any); ok {
								files = append(files, map[string]any{
									"name": f["name"],
									"path": f["path"],
									"type": f["type"],
									"size": f["size"],
								})
							}
						}
						out["files"] = files
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "github.repo.settings",
				Description: "Update repo settings. Pass only fields to change.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": ghProps(map[string]any{
						"description":    map[string]any{"type": "string"},
						"homepage":       map[string]any{"type": "string"},
						"private":        map[string]any{"type": "boolean"},
						"has_issues":     map[string]any{"type": "boolean"},
						"has_wiki":       map[string]any{"type": "boolean"},
						"has_projects":   map[string]any{"type": "boolean"},
						"default_branch": map[string]any{"type": "string"},
						"archived":       map[string]any{"type": "boolean"},
					}),
					"required": []string{"github_token", "owner", "repo"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				fields := []string{"description", "homepage", "private", "has_issues", "has_wiki", "has_projects", "default_branch", "archived"}
				body := map[string]any{}
				for _, f := range fields {
					if v, ok := p[f]; ok {
						body[f] = v
					}
				}
				res, err := ghDo("PATCH", "/repos/"+str(p, "owner", "")+"/"+str(p, "repo", ""), str(p, "github_token", ""), body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["name"] = b["name"]
					out["url"] = b["html_url"]
					out["private"] = b["private"]
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// GitHub ── Files  (SHA-free from Claude's perspective)
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "github.file.write",
				Description: "Create OR update a file. Fetches SHA automatically — no separate call needed. " +
					"content is plain UTF-8 text (tool base64-encodes it). " +
					"Returns {ok, path, sha, commit_sha, commit_url}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": ghProps(map[string]any{
						"path":    map[string]any{"type": "string", "description": "File path in repo e.g. src/main.go"},
						"content": map[string]any{"type": "string", "description": "Plain text (not base64)"},
						"message": map[string]any{"type": "string", "description": "Commit message"},
						"branch":  map[string]any{"type": "string"},
					}),
					"required": []string{"github_token", "owner", "repo", "path", "content"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := str(p, "github_token", "")
				owner := str(p, "owner", "")
				repo := str(p, "repo", "")
				filePath := str(p, "path", "")
				branch := str(p, "branch", "")

				sha, err := ghGetFileSHA(token, owner, repo, filePath, branch)
				if err != nil {
					return errResult("sha lookup: " + err.Error()), nil
				}

				body := map[string]any{
					"message": str(p, "message", "update "+filePath),
					"content": base64.StdEncoding.EncodeToString([]byte(str(p, "content", ""))),
				}
				if sha != "" {
					body["sha"] = sha
				}
				if branch != "" {
					body["branch"] = branch
				}

				apiPath := fmt.Sprintf("/repos/%s/%s/contents/%s", owner, repo, filePath)
				res, err := ghDo("PUT", apiPath, token, body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					if content, ok := b["content"].(map[string]any); ok {
						out["path"] = content["path"]
						out["sha"] = content["sha"]
					}
					if commit, ok := b["commit"].(map[string]any); ok {
						out["commit_sha"] = commit["sha"]
						out["commit_url"] = commit["html_url"]
					}
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "github.file.read",
				Description: "Read a file. Returns {ok, path, content (decoded), sha, size}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": ghProps(map[string]any{
						"path":   map[string]any{"type": "string"},
						"branch": map[string]any{"type": "string"},
					}),
					"required": []string{"github_token", "owner", "repo", "path"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := str(p, "github_token", "")
				owner := str(p, "owner", "")
				repo := str(p, "repo", "")
				filePath := str(p, "path", "")
				branch := str(p, "branch", "")

				apiPath := fmt.Sprintf("/repos/%s/%s/contents/%s", owner, repo, filePath)
				if branch != "" {
					apiPath += "?ref=" + url.QueryEscape(branch)
				}
				res, err := ghDo("GET", apiPath, token, nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["path"] = b["path"]
					out["sha"] = b["sha"]
					out["size"] = b["size"]
					if enc, ok := b["content"].(string); ok {
						decoded, decErr := base64.StdEncoding.DecodeString(strings.ReplaceAll(enc, "\n", ""))
						if decErr == nil {
							out["content"] = string(decoded)
						} else {
							out["content_base64"] = enc
						}
					}
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "github.file.delete",
				Description: "Delete a file. Fetches SHA automatically.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": ghProps(map[string]any{
						"path":    map[string]any{"type": "string"},
						"message": map[string]any{"type": "string"},
						"branch":  map[string]any{"type": "string"},
					}),
					"required": []string{"github_token", "owner", "repo", "path"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := str(p, "github_token", "")
				owner := str(p, "owner", "")
				repo := str(p, "repo", "")
				filePath := str(p, "path", "")
				branch := str(p, "branch", "")

				sha, err := ghGetFileSHA(token, owner, repo, filePath, branch)
				if err != nil {
					return errResult("sha lookup: " + err.Error()), nil
				}
				if sha == "" {
					return errResult("file not found: " + filePath), nil
				}

				body := map[string]any{"message": str(p, "message", "delete "+filePath), "sha": sha}
				if branch != "" {
					body["branch"] = branch
				}

				apiPath := fmt.Sprintf("/repos/%s/%s/contents/%s", owner, repo, filePath)
				res, err := ghDo("DELETE", apiPath, token, body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					if msg, ok := b["message"].(string); ok && !res["ok"].(bool) {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "github.file.search",
				Description: "Search for files/code across GitHub. Returns [{path, repo, url}]. Supports all GitHub search qualifiers: repo:owner/repo language:go filename:main etc.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"github_token": map[string]any{"type": "string"},
						"query":        map[string]any{"type": "string", "description": "e.g. 'MyClass repo:owner/repo language:go'"},
					},
					"required": []string{"github_token", "query"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				q := url.QueryEscape(str(p, "query", ""))
				res, err := ghDo("GET", "/search/code?q="+q+"&per_page=20", str(p, "github_token", ""), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["total_count"] = b["total_count"]
					if items, ok := b["items"].([]any); ok {
						var files []map[string]any
						for _, item := range items {
							if f, ok := item.(map[string]any); ok {
								repoName := ""
								if r, ok := f["repository"].(map[string]any); ok {
									repoName, _ = r["full_name"].(string)
								}
								files = append(files, map[string]any{
									"path": f["path"],
									"repo": repoName,
									"url":  f["html_url"],
								})
							}
						}
						out["files"] = files
					}
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// GitHub ── User
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name:        "github.user.status",
				Description: "Set authenticated user's status. Pass empty emoji+message to clear.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"github_token": map[string]any{"type": "string"},
						"emoji":        map[string]any{"type": "string", "description": "GitHub emoji slug e.g. :pizza:"},
						"message":      map[string]any{"type": "string"},
						"busy":         map[string]any{"type": "boolean", "default": false, "description": "Mark as limited availability"},
						"expires_at":   map[string]any{"type": "string", "description": "ISO 8601 expiry"},
					},
					"required": []string{"github_token"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				statusBody := map[string]any{
					"emoji":                str(p, "emoji", ""),
					"message":              str(p, "message", ""),
					"limited_availability": boolVal(p, "busy", false),
				}
				if exp := str(p, "expires_at", ""); exp != "" {
					statusBody["expires_at"] = exp
				}
				// GitHub status update uses GraphQL or PATCH /user — use undocumented PATCH for REST
				res, err := ghDo("PATCH", "/user", str(p, "github_token", ""), map[string]any{
					"status": statusBody,
				})
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					if s, ok := b["status"].(map[string]any); ok {
						out["emoji"] = s["emoji"]
						out["message"] = s["message"]
					}
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// GitHub ── Actions & Workflows
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name:        "github.actions.workflows",
				Description: "List workflows in a repo. Returns [{id, name, state, path}].",
				InputSchema: map[string]any{
					"type":       "object",
					"properties": ghProps(nil),
					"required":   []string{"github_token", "owner", "repo"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := ghDo("GET", "/repos/"+str(p, "owner", "")+"/"+str(p, "repo", "")+"/actions/workflows", str(p, "github_token", ""), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["total"] = b["total_count"]
					if wfs, ok := b["workflows"].([]any); ok {
						var list []map[string]any
						for _, w := range wfs {
							if wf, ok := w.(map[string]any); ok {
								list = append(list, map[string]any{"id": wf["id"], "name": wf["name"], "state": wf["state"], "path": wf["path"]})
							}
						}
						out["workflows"] = list
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "github.actions.dispatch",
				Description: "Trigger a workflow via workflow_dispatch. workflow_id can be numeric ID or filename (e.g. deploy.yml).",
				InputSchema: map[string]any{
					"type": "object",
					"properties": ghProps(map[string]any{
						"workflow_id": map[string]any{"type": "string", "description": "Workflow ID or filename"},
						"ref":         map[string]any{"type": "string", "description": "Branch or tag name"},
						"inputs":      map[string]any{"type": "object", "description": "Workflow input key/values"},
					}),
					"required": []string{"github_token", "owner", "repo", "workflow_id", "ref"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{"ref": str(p, "ref", "main")}
				if inputs := anyMap(p, "inputs"); inputs != nil {
					body["inputs"] = inputs
				}
				path := fmt.Sprintf("/repos/%s/%s/actions/workflows/%s/dispatches", str(p, "owner", ""), str(p, "repo", ""), str(p, "workflow_id", ""))
				res, err := ghDo("POST", path, str(p, "github_token", ""), body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{"ok": res["ok"], "status": res["status"]}), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "github.actions.runs",
				Description: "List recent workflow runs. Returns [{id, name, status, conclusion, url, created_at}].",
				InputSchema: map[string]any{
					"type": "object",
					"properties": ghProps(map[string]any{
						"workflow_id": map[string]any{"type": "string", "description": "Filter by workflow ID/filename (omit for all)"},
						"status":      map[string]any{"type": "string", "enum": []string{"queued", "in_progress", "completed", "waiting"}},
					}),
					"required": []string{"github_token", "owner", "repo"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				base := "/repos/" + str(p, "owner", "") + "/" + str(p, "repo", "") + "/actions"
				if wf := str(p, "workflow_id", ""); wf != "" {
					base += "/workflows/" + wf
				}
				base += "/runs?per_page=20"
				if s := str(p, "status", ""); s != "" {
					base += "&status=" + s
				}
				res, err := ghDo("GET", base, str(p, "github_token", ""), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					if runs, ok := b["workflow_runs"].([]any); ok {
						var list []map[string]any
						for _, r := range runs {
							if run, ok := r.(map[string]any); ok {
								list = append(list, map[string]any{
									"id": run["id"], "name": run["name"],
									"status": run["status"], "conclusion": run["conclusion"],
									"url": run["html_url"], "created_at": run["created_at"],
								})
							}
						}
						out["runs"] = list
					}
				}
				return textResult(out), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// GitHub ── Webhooks
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name:        "github.webhook.create",
				Description: "Add a webhook to a repo. events e.g. [\"push\",\"pull_request\"]. Returns {ok, id, events}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": ghProps(map[string]any{
						"url":          map[string]any{"type": "string", "format": "uri", "description": "Payload endpoint URL"},
						"events":       map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "default": []string{"push"}},
						"secret":       map[string]any{"type": "string"},
						"content_type": map[string]any{"type": "string", "enum": []string{"json", "form"}, "default": "json"},
						"active":       map[string]any{"type": "boolean", "default": true},
					}),
					"required": []string{"github_token", "owner", "repo", "url"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				cfg := map[string]any{"url": str(p, "url", ""), "content_type": str(p, "content_type", "json"), "insecure_ssl": "0"}
				if s := str(p, "secret", ""); s != "" {
					cfg["secret"] = s
				}
				events := strSlice(p, "events")
				if len(events) == 0 {
					events = []string{"push"}
				}
				body := map[string]any{"name": "web", "active": boolVal(p, "active", true), "events": events, "config": cfg}
				res, err := ghDo("POST", "/repos/"+str(p, "owner", "")+"/"+str(p, "repo", "")+"/hooks", str(p, "github_token", ""), body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["id"] = b["id"]
					out["events"] = b["events"]
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "github.webhook.list",
				Description: "List webhooks for a repo. Returns [{id, url, events, active}].",
				InputSchema: map[string]any{
					"type":       "object",
					"properties": ghProps(nil),
					"required":   []string{"github_token", "owner", "repo"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := ghDo("GET", "/repos/"+str(p, "owner", "")+"/"+str(p, "repo", "")+"/hooks", str(p, "github_token", ""), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if arr, ok := res["body"].([]any); ok {
					var list []map[string]any
					for _, h := range arr {
						if hook, ok := h.(map[string]any); ok {
							cfg, _ := hook["config"].(map[string]any)
							hookURL := ""
							if cfg != nil {
								hookURL, _ = cfg["url"].(string)
							}
							list = append(list, map[string]any{"id": hook["id"], "url": hookURL, "events": hook["events"], "active": hook["active"]})
						}
					}
					out["webhooks"] = list
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "github.webhook.delete",
				Description: "Delete a repo webhook by ID.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": ghProps(map[string]any{
						"hook_id": map[string]any{"type": "number"},
					}),
					"required": []string{"github_token", "owner", "repo", "hook_id"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				id := int(num(p, "hook_id", 0))
				res, err := ghDo("DELETE", fmt.Sprintf("/repos/%s/%s/hooks/%d", str(p, "owner", ""), str(p, "repo", ""), id), str(p, "github_token", ""), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{"ok": res["ok"], "status": res["status"]}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Brevo ── Email
		// Free plan: 300 emails/day. brevo.mail.bulk sends 1000 in ONE call.
		// Auth: api-key header (set automatically from brevo_api_key param).
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "brevo.mail.send",
				Description: "Send a single transactional email. " +
					"Prefer brevo.mail.bulk when sending to multiple recipients — it's one API call for up to 1000.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"brevo_api_key":  map[string]any{"type": "string"},
						"sender_email":   map[string]any{"type": "string", "format": "email"},
						"sender_name":    map[string]any{"type": "string"},
						"to_email":       map[string]any{"type": "string", "format": "email"},
						"to_name":        map[string]any{"type": "string"},
						"subject":        map[string]any{"type": "string"},
						"html_content":   map[string]any{"type": "string", "description": "HTML body"},
						"text_content":   map[string]any{"type": "string"},
						"template_id":    map[string]any{"type": "number", "description": "Brevo template ID (alternative to html_content)"},
						"params":         map[string]any{"type": "object", "description": "Template variables e.g. {\"name\":\"Alice\"}"},
						"reply_to_email": map[string]any{"type": "string", "format": "email"},
					},
					"required": []string{"brevo_api_key", "sender_email", "to_email", "subject"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{
					"sender":  map[string]any{"email": str(p, "sender_email", ""), "name": str(p, "sender_name", "")},
					"to":      []map[string]any{{"email": str(p, "to_email", ""), "name": str(p, "to_name", "")}},
					"subject": str(p, "subject", ""),
				}
				if h := str(p, "html_content", ""); h != "" {
					body["htmlContent"] = h
				}
				if t := str(p, "text_content", ""); t != "" {
					body["textContent"] = t
				}
				if tid := num(p, "template_id", 0); tid > 0 {
					body["templateId"] = int(tid)
				}
				if params := anyMap(p, "params"); params != nil {
					body["params"] = params
				}
				if r := str(p, "reply_to_email", ""); r != "" {
					body["replyTo"] = map[string]any{"email": r}
				}
				res, err := brevoDo("POST", "/smtp/email", str(p, "brevo_api_key", ""), body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["messageId"] = b["messageId"]
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name: "brevo.mail.bulk",
				Description: "Send up to 1000 personalized emails in ONE API call. " +
					"Each entry in message_versions targets one or more recipients with optional overrides. " +
					"Much more token-efficient than looping brevo.mail.send. " +
					"Free plan: 300 emails/day total across all sends. " +
					"Returns {ok, batchId}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"brevo_api_key": map[string]any{"type": "string"},
						"sender_email":  map[string]any{"type": "string", "format": "email", "description": "Default sender email"},
						"sender_name":   map[string]any{"type": "string"},
						"subject":       map[string]any{"type": "string", "description": "Default subject (overridable per version)"},
						"html_content":  map[string]any{"type": "string", "description": "Default HTML (overridable per version)"},
						"template_id":   map[string]any{"type": "number", "description": "Brevo template ID (alternative to html_content)"},
						"message_versions": map[string]any{
							"type":        "array",
							"description": "Per-recipient batches. Each: {to:[{email,name}], subject?, htmlContent?, params?}. No outer `to` param allowed.",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"to":          map[string]any{"type": "array", "items": map[string]any{"type": "object", "properties": map[string]any{"email": map[string]any{"type": "string"}, "name": map[string]any{"type": "string"}}}},
									"subject":     map[string]any{"type": "string"},
									"htmlContent": map[string]any{"type": "string"},
									"params":      map[string]any{"type": "object"},
								},
								"required": []string{"to"},
							},
						},
					},
					"required": []string{"brevo_api_key", "sender_email", "subject", "message_versions"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{
					"sender":  map[string]any{"email": str(p, "sender_email", ""), "name": str(p, "sender_name", "")},
					"subject": str(p, "subject", ""),
				}
				if h := str(p, "html_content", ""); h != "" {
					body["htmlContent"] = h
				}
				if tid := num(p, "template_id", 0); tid > 0 {
					body["templateId"] = int(tid)
				}
				if mv, ok := p["message_versions"].([]any); ok {
					body["messageVersions"] = mv
				}
				res, err := brevoDo("POST", "/smtp/email", str(p, "brevo_api_key", ""), body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["batchId"] = b["batchId"]
					out["messageId"] = b["messageId"]
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Brevo ── Contacts
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "brevo.contact.create",
				Description: "Create or upsert a contact. Attribute keys MUST be UPPERCASE (e.g. FNAME, LNAME). " +
					"list_ids adds contact to those mailing lists.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"brevo_api_key":  map[string]any{"type": "string"},
						"email":          map[string]any{"type": "string", "format": "email"},
						"attributes":     map[string]any{"type": "object", "description": "UPPERCASE keys e.g. {\"FNAME\":\"Ali\",\"LNAME\":\"Hassan\"}"},
						"list_ids":       map[string]any{"type": "array", "items": map[string]any{"type": "number"}},
						"update_enabled": map[string]any{"type": "boolean", "default": true, "description": "Upsert if already exists"},
					},
					"required": []string{"brevo_api_key", "email"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{"email": str(p, "email", ""), "updateEnabled": boolVal(p, "update_enabled", true)}
				if attrs := anyMap(p, "attributes"); attrs != nil {
					body["attributes"] = attrs
				}
				if raw, ok := p["list_ids"].([]any); ok {
					body["listIds"] = raw
				}
				res, err := brevoDo("POST", "/contacts", str(p, "brevo_api_key", ""), body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["id"] = b["id"]
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "brevo.contact.get",
				Description: "Get a contact by email address or numeric ID.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"brevo_api_key": map[string]any{"type": "string"},
						"identifier":    map[string]any{"type": "string", "description": "Email or contact ID"},
					},
					"required": []string{"brevo_api_key", "identifier"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := brevoDo("GET", "/contacts/"+url.PathEscape(str(p, "identifier", "")), str(p, "brevo_api_key", ""), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "brevo.contact.list",
				Description: "List contacts, optionally filtered to a specific list. Returns {total, contacts[{id,email,attributes}]}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"brevo_api_key": map[string]any{"type": "string"},
						"list_id":       map[string]any{"type": "number", "description": "Filter by mailing list (omit for all contacts)"},
						"limit":         map[string]any{"type": "number", "default": 50, "description": "Max 500"},
						"offset":        map[string]any{"type": "number", "default": 0},
					},
					"required": []string{"brevo_api_key"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				limit := int(num(p, "limit", 50))
				offset := int(num(p, "offset", 0))
				path := fmt.Sprintf("/contacts?limit=%d&offset=%d", limit, offset)
				if lid := num(p, "list_id", 0); lid > 0 {
					path = fmt.Sprintf("/contacts/lists/%d/contacts?limit=%d&offset=%d", int(lid), limit, offset)
				}
				res, err := brevoDo("GET", path, str(p, "brevo_api_key", ""), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["total"] = b["count"]
					if contacts, ok := b["contacts"].([]any); ok {
						var list []map[string]any
						for _, c := range contacts {
							if contact, ok := c.(map[string]any); ok {
								list = append(list, map[string]any{"id": contact["id"], "email": contact["email"], "attributes": contact["attributes"]})
							}
						}
						out["contacts"] = list
					}
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Brevo ── Mailing Lists
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name:        "brevo.list.create",
				Description: "Create a mailing list. Returns {ok, id}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"brevo_api_key": map[string]any{"type": "string"},
						"name":          map[string]any{"type": "string"},
						"folder_id":     map[string]any{"type": "number", "description": "Folder ID (omit to use default folder)"},
					},
					"required": []string{"brevo_api_key", "name"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{"name": str(p, "name", "")}
				if fid := num(p, "folder_id", 0); fid > 0 {
					body["folderId"] = int(fid)
				}
				res, err := brevoDo("POST", "/contacts/lists", str(p, "brevo_api_key", ""), body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["id"] = b["id"]
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "brevo.list.get",
				Description: "List all mailing lists. Returns [{id, name, totalSubscribers}].",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"brevo_api_key": map[string]any{"type": "string"},
						"limit":         map[string]any{"type": "number", "default": 50},
						"offset":        map[string]any{"type": "number", "default": 0},
					},
					"required": []string{"brevo_api_key"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				limit := int(num(p, "limit", 50))
				offset := int(num(p, "offset", 0))
				res, err := brevoDo("GET", fmt.Sprintf("/contacts/lists?limit=%d&offset=%d", limit, offset), str(p, "brevo_api_key", ""), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["total"] = b["count"]
					if lists, ok := b["lists"].([]any); ok {
						var result []map[string]any
						for _, l := range lists {
							if lst, ok := l.(map[string]any); ok {
								result = append(result, map[string]any{"id": lst["id"], "name": lst["name"], "totalSubscribers": lst["totalSubscribers"]})
							}
						}
						out["lists"] = result
					}
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "brevo.list.add_contacts",
				Description: "Add existing contacts to a mailing list by email or ID.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"brevo_api_key": map[string]any{"type": "string"},
						"list_id":       map[string]any{"type": "number"},
						"emails":        map[string]any{"type": "array", "items": map[string]any{"type": "string", "format": "email"}, "description": "Up to 150 emails per call"},
					},
					"required": []string{"brevo_api_key", "list_id", "emails"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				lid := int(num(p, "list_id", 0))
				emails := strSlice(p, "emails")
				if len(emails) == 0 {
					return errResult("emails array is required"), nil
				}
				body := map[string]any{"emails": emails}
				res, err := brevoDo("POST", fmt.Sprintf("/contacts/lists/%d/contacts/add", lid), str(p, "brevo_api_key", ""), body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Brevo ── Senders
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name:        "brevo.sender.create",
				Description: "Add a new email sender identity. The email domain must be verified in Brevo. Returns {ok, id}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"brevo_api_key": map[string]any{"type": "string"},
						"name":          map[string]any{"type": "string", "description": "Display name e.g. Support Team"},
						"email":         map[string]any{"type": "string", "format": "email"},
					},
					"required": []string{"brevo_api_key", "name", "email"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{"name": str(p, "name", ""), "email": str(p, "email", "")}
				res, err := brevoDo("POST", "/senders", str(p, "brevo_api_key", ""), body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["id"] = b["id"]
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "brevo.sender.list",
				Description: "List all senders in the account. Returns [{id, name, email, active}].",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"brevo_api_key": map[string]any{"type": "string"},
					},
					"required": []string{"brevo_api_key"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := brevoDo("GET", "/senders", str(p, "brevo_api_key", ""), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					if senders, ok := b["senders"].([]any); ok {
						var list []map[string]any
						for _, s := range senders {
							if sender, ok := s.(map[string]any); ok {
								list = append(list, map[string]any{"id": sender["id"], "name": sender["name"], "email": sender["email"], "active": sender["active"]})
							}
						}
						out["senders"] = list
					}
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Cal.com ── Scheduling
		// Pass cal_api_key with every call.
		// Durations: 15 min (4513949) | 30 min (4513947)
		// Default timezone: Africa/Cairo
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name:        "cal.slots",
				Description: "Get available booking slots for a date range.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"cal_api_key":      calKeyProp,
						"start_date":       map[string]any{"type": "string", "description": "YYYY-MM-DD"},
						"end_date":         map[string]any{"type": "string", "description": "YYYY-MM-DD"},
						"duration_minutes": map[string]any{"type": "number", "enum": []int{15, 30}, "default": 15},
					},
					"required": []string{"cal_api_key", "start_date", "end_date"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				dur := int(num(p, "duration_minutes", 15))
				etid, ok := calEventTypes[dur]
				if !ok {
					etid = calEventTypes[15]
				}
				path := fmt.Sprintf("/slots?start=%s&end=%s&eventTypeId=%d", str(p, "start_date", ""), str(p, "end_date", ""), etid)
				res, err := calRequest(str(p, "cal_api_key", ""), path, "GET", nil, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal.bookings",
				Description: "List bookings filtered by status: upcoming | past | cancelled.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"cal_api_key": calKeyProp,
						"status":      map[string]any{"type": "string", "enum": []string{"upcoming", "past", "cancelled"}},
					},
					"required": []string{"cal_api_key"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				path := "/bookings"
				if s := str(p, "status", ""); s != "" {
					path += "?status=" + s
				}
				res, err := calRequest(str(p, "cal_api_key", ""), path, "GET", nil, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal.booking",
				Description: "Get a single booking by UID.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"cal_api_key": calKeyProp,
						"uid":         map[string]any{"type": "string"},
					},
					"required": []string{"cal_api_key", "uid"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := calRequest(str(p, "cal_api_key", ""), "/bookings/"+str(p, "uid", ""), "GET", nil, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal.create",
				Description: "Create a booking at an exact start time. Use cal.slots first to get valid times.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"cal_api_key":      calKeyProp,
						"start":            map[string]any{"type": "string", "description": "ISO 8601 e.g. 2026-04-01T10:00:00Z"},
						"duration_minutes": map[string]any{"type": "number", "enum": []int{15, 30}, "default": 15},
						"attendee_name":    map[string]any{"type": "string"},
						"attendee_email":   map[string]any{"type": "string", "format": "email"},
						"timezone":         map[string]any{"type": "string", "default": "Africa/Cairo"},
						"guests":           map[string]any{"type": "array", "items": map[string]any{"type": "string", "format": "email"}},
					},
					"required": []string{"cal_api_key", "start", "attendee_name", "attendee_email"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				dur := int(num(p, "duration_minutes", 15))
				etid, ok := calEventTypes[dur]
				if !ok {
					etid = calEventTypes[15]
				}
				body := map[string]any{
					"eventTypeId": etid,
					"start":       str(p, "start", ""),
					"attendee":    map[string]any{"name": str(p, "attendee_name", ""), "email": str(p, "attendee_email", ""), "timeZone": str(p, "timezone", "Africa/Cairo")},
				}
				if guests := strSlice(p, "guests"); len(guests) > 0 {
					body["guests"] = guests
				}
				res, err := calRequest(str(p, "cal_api_key", ""), "/bookings", "POST", body, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name: "cal.book",
				Description: "One-shot booking: finds first available slot on date, creates booking, auto-confirms. " +
					"Use when the user wants 'any available time' on a date. Returns {ok, uid, slot, booking, confirmation}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"cal_api_key":      calKeyProp,
						"name":             map[string]any{"type": "string"},
						"email":            map[string]any{"type": "string", "format": "email"},
						"date":             map[string]any{"type": "string", "description": "YYYY-MM-DD"},
						"duration_minutes": map[string]any{"type": "number", "enum": []int{15, 30}, "default": 15},
						"timezone":         map[string]any{"type": "string", "default": "Africa/Cairo"},
						"guests":           map[string]any{"type": "array", "items": map[string]any{"type": "string", "format": "email"}},
					},
					"required": []string{"cal_api_key", "name", "email", "date"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				dur := int(num(p, "duration_minutes", 15))
				etid, ok := calEventTypes[dur]
				if !ok {
					etid = calEventTypes[15]
				}
				apiKey := str(p, "cal_api_key", "")
				date := str(p, "date", "")

				slotsRes, err := calRequest(apiKey, fmt.Sprintf("/slots?start=%s&end=%s&eventTypeId=%d", date, date, etid), "GET", nil, "")
				if err != nil {
					return errResult(err.Error()), nil
				}

				firstSlot := ""
				if body, ok := slotsRes["body"].(map[string]any); ok {
					if data, ok := body["data"].(map[string]any); ok {
						if slots, ok := data["slots"].(map[string]any); ok {
							for _, dayRaw := range slots {
								if daySlots, ok := dayRaw.([]any); ok && len(daySlots) > 0 {
									if slotObj, ok := daySlots[0].(map[string]any); ok {
										firstSlot, _ = slotObj["time"].(string)
									}
								}
								if firstSlot != "" {
									break
								}
							}
						}
					}
				}
				if firstSlot == "" {
					return textResult(map[string]any{"ok": false, "error": "no available slots on " + date, "slots": slotsRes["body"]}), nil
				}

				bookBody := map[string]any{
					"eventTypeId": etid, "start": firstSlot,
					"attendee": map[string]any{"name": str(p, "name", ""), "email": str(p, "email", ""), "timeZone": str(p, "timezone", "Africa/Cairo")},
				}
				if guests := strSlice(p, "guests"); len(guests) > 0 {
					bookBody["guests"] = guests
				}

				booking, err := calRequest(apiKey, "/bookings", "POST", bookBody, "")
				if err != nil {
					return errResult(err.Error()), nil
				}

				uid := ""
				if bBody, ok := booking["body"].(map[string]any); ok {
					if data, ok := bBody["data"].(map[string]any); ok {
						uid, _ = data["uid"].(string)
					}
					if uid == "" {
						uid, _ = bBody["uid"].(string)
					}
				}
				if uid == "" {
					return textResult(map[string]any{"ok": false, "error": "booking failed", "response": booking}), nil
				}

				confirm, err := calRequest(apiKey, "/bookings/"+uid+"/confirm", "POST", map[string]any{"confirmed": true}, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{
					"ok": true, "uid": uid, "slot": firstSlot,
					"attendee": map[string]any{"name": str(p, "name", ""), "email": str(p, "email", "")},
					"booking": booking["body"], "confirmation": confirm["body"],
				}), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal.confirm",
				Description: "Confirm (accept) a pending booking.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"cal_api_key": calKeyProp,
						"uid":         map[string]any{"type": "string"},
					},
					"required": []string{"cal_api_key", "uid"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := calRequest(str(p, "cal_api_key", ""), "/bookings/"+str(p, "uid", "")+"/confirm", "POST", map[string]any{"confirmed": true}, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal.reject",
				Description: "Reject a pending booking.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"cal_api_key": calKeyProp,
						"uid":         map[string]any{"type": "string"},
						"reason":      map[string]any{"type": "string"},
					},
					"required": []string{"cal_api_key", "uid"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{"confirmed": false}
				if r := str(p, "reason", ""); r != "" {
					body["reason"] = r
				}
				res, err := calRequest(str(p, "cal_api_key", ""), "/bookings/"+str(p, "uid", "")+"/confirm", "POST", body, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal.cancel",
				Description: "Cancel a booking.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"cal_api_key": calKeyProp,
						"uid":         map[string]any{"type": "string"},
						"reason":      map[string]any{"type": "string"},
					},
					"required": []string{"cal_api_key", "uid"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{}
				if r := str(p, "reason", ""); r != "" {
					body["reason"] = r
				}
				res, err := calRequest(str(p, "cal_api_key", ""), "/bookings/"+str(p, "uid", "")+"/cancel", "POST", body, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal.reschedule",
				Description: "Reschedule a booking to a new start time. Use cal.slots to find a valid time.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"cal_api_key": calKeyProp,
						"uid":         map[string]any{"type": "string"},
						"new_start":   map[string]any{"type": "string", "description": "ISO 8601"},
						"reason":      map[string]any{"type": "string"},
					},
					"required": []string{"cal_api_key", "uid", "new_start"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{"start": str(p, "new_start", "")}
				if r := str(p, "reason", ""); r != "" {
					body["reason"] = r
				}
				res, err := calRequest(str(p, "cal_api_key", ""), "/bookings/"+str(p, "uid", "")+"/reschedule", "POST", body, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal.event_types",
				Description: "List all Cal.com event types for this account.",
				InputSchema: map[string]any{
					"type":       "object",
					"properties": map[string]any{"cal_api_key": calKeyProp},
					"required":   []string{"cal_api_key"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := calRequest(str(p, "cal_api_key", ""), "/event-types", "GET", nil, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Reddit ── Auth
		// Script-app flow: server-side password grant, no browser redirect.
		// Rate limit: 600 req / 10 min. Back-pressure is automatic.
		// Required scopes for full mod: read, modposts, modmail, modconfig,
		//   modflair, modlog, modwiki, privatemessages, submit, identity
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "reddit.auth",
				Description: "Get a Reddit OAuth2 bearer token using script-app password flow. " +
					"Returns {ok, token}. Pass token to all other reddit.* tools. " +
					"Tokens expire after 1 hour; call again to refresh.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"client_id":     map[string]any{"type": "string", "description": "Reddit app client_id"},
						"client_secret": map[string]any{"type": "string"},
						"username":      map[string]any{"type": "string", "description": "Mod account username"},
						"password":      map[string]any{"type": "string"},
						"user_agent":    map[string]any{"type": "string", "description": "e.g. bot:mymod:v1 (by /u/you)"},
					},
					"required": []string{"client_id", "client_secret", "username", "password"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				ua := str(p, "user_agent", "rose-mcp/6.0")
				token, err := redditToken(
					str(p, "client_id", ""), str(p, "client_secret", ""),
					str(p, "username", ""), str(p, "password", ""), ua,
				)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{"ok": true, "token": token}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Reddit ── Posts / Queue
		// Claude receives truncated posts to minimize tokens.
		// Description capped at 300 chars; body stripped to first 500 chars.
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "reddit.posts.fetch",
				Description: "Fetch up to 1000 latest posts from a subreddit queue (new/hot/top/mod queue/reports). " +
					"Returns slim objects: [{id, name, title, author, score, url, flair, " +
					"selftext_preview(500ch), num_reports, created_utc}]. " +
					"Use this to feed Claude for moderation decisions.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reddit_token": map[string]any{"type": "string"},
						"user_agent":   map[string]any{"type": "string", "default": "rose-mcp/6.0"},
						"subreddit":    map[string]any{"type": "string", "description": "e.g. pics"},
						"feed":         map[string]any{"type": "string", "enum": []string{"new", "hot", "top", "mod", "reports", "spam", "unmoderated"}, "default": "new"},
						"limit":        map[string]any{"type": "number", "default": 100, "description": "1-1000; fetched in pages of 100"},
						"after":        map[string]any{"type": "string", "description": "Pagination cursor (name of last post)"},
					},
					"required": []string{"reddit_token", "subreddit"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := str(p, "reddit_token", "")
				ua := str(p, "user_agent", "rose-mcp/6.0")
				sub := str(p, "subreddit", "")
				feed := str(p, "feed", "new")
				totalWant := int(num(p, "limit", 100))
				if totalWant > 1000 {
					totalWant = 1000
				}

				var path string
				switch feed {
				case "mod":
					path = fmt.Sprintf("/r/%s/about/modqueue", sub)
				case "reports":
					path = fmt.Sprintf("/r/%s/about/reports", sub)
				case "spam":
					path = fmt.Sprintf("/r/%s/about/spam", sub)
				case "unmoderated":
					path = fmt.Sprintf("/r/%s/about/unmoderated", sub)
				default:
					path = fmt.Sprintf("/r/%s/%s", sub, feed)
				}

				var allPosts []map[string]any
				after := str(p, "after", "")
				for len(allPosts) < totalWant {
					batch := 100
					if totalWant-len(allPosts) < batch {
						batch = totalWant - len(allPosts)
					}
					q := url.Values{}
					q.Set("limit", fmt.Sprintf("%d", batch))
					q.Set("raw_json", "1")
					if after != "" {
						q.Set("after", after)
					}
					res, err := redditDo("GET", path+"?"+q.Encode(), token, ua, nil)
					if err != nil {
						return errResult(err.Error()), nil
					}
					body, _ := res["body"].(map[string]any)
					data, _ := body["data"].(map[string]any)
					children, _ := data["children"].([]any)
					if len(children) == 0 {
						break
					}
					for _, c := range children {
						child, _ := c.(map[string]any)
						d, _ := child["data"].(map[string]any)
						if d == nil {
							continue
						}
						selftext, _ := d["selftext"].(string)
						if len(selftext) > 500 {
							selftext = selftext[:500] + "…"
						}
						title, _ := d["title"].(string)
						if len(title) > 200 {
							title = title[:200]
						}
						allPosts = append(allPosts, map[string]any{
							"id":               d["id"],
							"name":             d["name"], // fullname e.g. t3_xyz
							"title":            title,
							"author":           d["author"],
							"score":            d["score"],
							"url":              d["url"],
							"permalink":        d["permalink"],
							"flair":            d["link_flair_text"],
							"selftext_preview": selftext,
							"num_reports":      d["num_reports"],
							"created_utc":      d["created_utc"],
							"locked":           d["locked"],
							"removed":          d["removed"],
						})
					}
					after, _ = data["after"].(string)
					if after == "" {
						break
					}
				}
				return textResult(map[string]any{
					"ok":    true,
					"count": len(allPosts),
					"after": after,
					"posts": allPosts,
				}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Reddit ── Bulk moderation actions
		// Claude produces [{id, action}] — server fans out with rate limiting.
		// action: "remove" | "approve" | "spam" | "lock" | "unlock"
		// removal_reason is the reason UUID (from reddit.mod.reasons).
		// Returns {ok, done, errors[], rate_remaining}.
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "reddit.posts.act",
				Description: "Execute bulk moderation actions on posts/comments. " +
					"Pass actions=[{id, action, removal_reason?}]. " +
					"id is the post/comment fullname (e.g. t3_abc or t1_xyz). " +
					"action: remove | approve | spam | lock | unlock. " +
					"Server rate-limits automatically. Returns {ok, done, errors[]}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reddit_token": map[string]any{"type": "string"},
						"user_agent":   map[string]any{"type": "string", "default": "rose-mcp/6.0"},
						"subreddit":    map[string]any{"type": "string"},
						"actions": map[string]any{
							"type":        "array",
							"description": "Max 500 per call. Each: {id:fullname, action, removal_reason?}",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"id":             map[string]any{"type": "string"},
									"action":         map[string]any{"type": "string", "enum": []string{"remove", "approve", "spam", "lock", "unlock"}},
									"removal_reason": map[string]any{"type": "string", "description": "Reason UUID from reddit.mod.reasons"},
								},
								"required": []string{"id", "action"},
							},
						},
					},
					"required": []string{"reddit_token", "subreddit", "actions"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := str(p, "reddit_token", "")
				ua := str(p, "user_agent", "rose-mcp/6.0")
				rawActions, _ := p["actions"].([]any)

				done := 0
				var errs []map[string]any

				for _, a := range rawActions {
					act, _ := a.(map[string]any)
					if act == nil {
						continue
					}
					id := str(act, "id", "")
					action := str(act, "action", "")
					if id == "" || action == "" {
						continue
					}

					var endpoint string
					form := url.Values{}
					form.Set("id", id)

					switch action {
					case "remove":
						endpoint = "/api/remove"
						form.Set("spam", "false")
						if rr := str(act, "removal_reason", ""); rr != "" {
							form.Set("reason_id", rr)
						}
					case "spam":
						endpoint = "/api/remove"
						form.Set("spam", "true")
					case "approve":
						endpoint = "/api/approve"
					case "lock":
						endpoint = "/api/lock"
					case "unlock":
						endpoint = "/api/unlock"
					default:
						errs = append(errs, map[string]any{"id": id, "error": "unknown action: " + action})
						continue
					}

					res, err := redditDo("POST", endpoint, token, ua, form)
					if err != nil {
						errs = append(errs, map[string]any{"id": id, "error": err.Error()})
						continue
					}
					if !res["ok"].(bool) {
						errs = append(errs, map[string]any{"id": id, "status": res["status"]})
					} else {
						done++
					}
				}

				return textResult(map[string]any{
					"ok":             len(errs) == 0,
					"done":           done,
					"errors":         errs,
					"rate_remaining": redditRL.remaining,
				}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Reddit ── Modmail
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "reddit.modmail.list",
				Description: "Fetch modmail conversations sorted by creation date. " +
					"Returns [{id, subject, author, state, created_utc, body_preview}]. " +
					"Filters: state (all/new/inprogress/archived/default/notifications/join_requests/mod), " +
					"after cursor for pagination.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reddit_token": map[string]any{"type": "string"},
						"user_agent":   map[string]any{"type": "string", "default": "rose-mcp/6.0"},
						"subreddit":    map[string]any{"type": "string"},
						"state":        map[string]any{"type": "string", "default": "all", "enum": []string{"all", "new", "inprogress", "archived", "default", "notifications", "join_requests", "mod"}},
						"limit":        map[string]any{"type": "number", "default": 25, "description": "Max 100"},
						"sort":         map[string]any{"type": "string", "enum": []string{"recent", "mod", "user", "unread"}, "default": "recent"},
						"after":        map[string]any{"type": "string"},
					},
					"required": []string{"reddit_token", "subreddit"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := str(p, "reddit_token", "")
				ua := str(p, "user_agent", "rose-mcp/6.0")
				sub := str(p, "subreddit", "")
				limit := int(num(p, "limit", 25))
				if limit > 100 {
					limit = 100
				}

				q := url.Values{}
				q.Set("entity", sub)
				q.Set("state", str(p, "state", "all"))
				q.Set("sort", str(p, "sort", "recent"))
				q.Set("limit", fmt.Sprintf("%d", limit))
				if after := str(p, "after", ""); after != "" {
					q.Set("after", after)
				}

				res, err := redditDo("GET", "/api/mod/conversations?"+q.Encode(), token, ua, nil)
				if err != nil {
					return errResult(err.Error()), nil
				}

				body, _ := res["body"].(map[string]any)
				convsRaw, _ := body["conversations"].(map[string]any)
				var convs []map[string]any
				for id, cv := range convsRaw {
					c, _ := cv.(map[string]any)
					if c == nil {
						continue
					}
					preview := ""
					if msgs, ok := body["messages"].(map[string]any); ok {
						// grab first message body
						for _, mv := range msgs {
							m, _ := mv.(map[string]any)
							if body, ok := m["body"].(string); ok {
								if len(body) > 300 {
									body = body[:300] + "…"
								}
								preview = body
								break
							}
						}
					}
					subject, _ := c["subject"].(string)
					state := ""
					if s, ok := c["state"].(float64); ok {
						state = fmt.Sprintf("%g", s)
					}
					convs = append(convs, map[string]any{
						"id":           id,
						"subject":      subject,
						"state":        state,
						"created_utc":  c["lastUpdated"],
						"body_preview": preview,
					})
				}
				return textResult(map[string]any{
					"ok":            res["ok"],
					"count":         len(convs),
					"conversations": convs,
					"after":         body["after"],
				}), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "reddit.modmail.reply",
				Description: "Reply to a modmail conversation or send a new modmail to a user.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reddit_token":     map[string]any{"type": "string"},
						"user_agent":       map[string]any{"type": "string", "default": "rose-mcp/6.0"},
						"conversation_id":  map[string]any{"type": "string", "description": "Existing conversation ID (omit to start new)"},
						"subreddit":        map[string]any{"type": "string", "description": "Required when starting new conversation"},
						"subject":          map[string]any{"type": "string", "description": "Required for new conversation"},
						"to":               map[string]any{"type": "string", "description": "Username (required for new conversation)"},
						"body":             map[string]any{"type": "string", "description": "Markdown body"},
						"internal":         map[string]any{"type": "boolean", "default": false, "description": "Internal mod note (not visible to user)"},
					},
					"required": []string{"reddit_token", "body"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := str(p, "reddit_token", "")
				ua := str(p, "user_agent", "rose-mcp/6.0")
				convID := str(p, "conversation_id", "")

				if convID != "" {
					// Reply to existing
					body := map[string]any{
						"body":           str(p, "body", ""),
						"isAuthorHidden": false,
						"isInternal":     boolVal(p, "internal", false),
					}
					b, _ := json.Marshal(body)
					req, _ := http.NewRequest("POST", "https://oauth.reddit.com/api/mod/conversations/"+convID, bytes.NewReader(b))
					req.Header.Set("Authorization", "Bearer "+token)
					req.Header.Set("User-Agent", ua)
					req.Header.Set("Content-Type", "application/json")
					resp, err := http.DefaultClient.Do(req)
					if err != nil {
						return errResult(err.Error()), nil
					}
					defer resp.Body.Close()
					raw, _ := io.ReadAll(resp.Body)
					var parsed any
					_ = json.Unmarshal(raw, &parsed)
					return textResult(map[string]any{"ok": resp.StatusCode < 300, "status": resp.StatusCode, "body": parsed}), nil
				}

				// New conversation
				form := url.Values{}
				form.Set("srName", str(p, "subreddit", ""))
				form.Set("subject", str(p, "subject", ""))
				form.Set("body", str(p, "body", ""))
				form.Set("to", str(p, "to", ""))
				form.Set("isAuthorHidden", "false")
				res, err := redditDo("POST", "/api/mod/conversations", token, ua, form)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{"ok": res["ok"], "status": res["status"]}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Reddit ── User management (bulk ban / mute)
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "reddit.users.ban",
				Description: "Bulk ban or unban users from a subreddit. " +
					"Pass users=[{username, duration?, reason?, note?, message?}]. " +
					"duration=0 means permanent. Returns {ok, done, errors[]}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reddit_token": map[string]any{"type": "string"},
						"user_agent":   map[string]any{"type": "string", "default": "rose-mcp/6.0"},
						"subreddit":    map[string]any{"type": "string"},
						"unban":        map[string]any{"type": "boolean", "default": false},
						"users": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"username": map[string]any{"type": "string"},
									"duration": map[string]any{"type": "number", "description": "Days 1-999 or 0=permanent"},
									"reason":   map[string]any{"type": "string"},
									"note":     map[string]any{"type": "string", "description": "Internal mod note"},
									"message":  map[string]any{"type": "string", "description": "Message sent to user"},
								},
								"required": []string{"username"},
							},
						},
					},
					"required": []string{"reddit_token", "subreddit", "users"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := str(p, "reddit_token", "")
				ua := str(p, "user_agent", "rose-mcp/6.0")
				sub := str(p, "subreddit", "")
				unban := boolVal(p, "unban", false)
				rawUsers, _ := p["users"].([]any)

				action := "banned"
				if unban {
					action = "unbanned"
				}
				endpoint := fmt.Sprintf("/r/%s/api/friend", sub)
				if unban {
					endpoint = fmt.Sprintf("/r/%s/api/unfriend", sub)
				}

				done := 0
				var errs []map[string]any

				for _, u := range rawUsers {
					user, _ := u.(map[string]any)
					if user == nil {
						continue
					}
					username := str(user, "username", "")
					if username == "" {
						continue
					}

					form := url.Values{}
					form.Set("name", username)
					form.Set("type", action)
					if dur := num(user, "duration", 0); dur > 0 {
						form.Set("duration", fmt.Sprintf("%d", int(dur)))
					}
					if r := str(user, "reason", ""); r != "" {
						form.Set("reason", r)
					}
					if n := str(user, "note", ""); n != "" {
						form.Set("note", n)
					}
					if m := str(user, "message", ""); m != "" {
						form.Set("ban_message", m)
					}

					res, err := redditDo("POST", endpoint, token, ua, form)
					if err != nil {
						errs = append(errs, map[string]any{"user": username, "error": err.Error()})
						continue
					}
					if !res["ok"].(bool) {
						errs = append(errs, map[string]any{"user": username, "status": res["status"]})
					} else {
						done++
					}
				}
				return textResult(map[string]any{"ok": len(errs) == 0, "done": done, "errors": errs, "rate_remaining": redditRL.remaining}), nil
			},
		},
		{
			Def: ToolDef{
				Name: "reddit.users.mute",
				Description: "Bulk mute or unmute users from modmail. Mute prevents sending modmail. " +
					"Pass users=[{username, note?}]. Returns {ok, done, errors[]}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reddit_token": map[string]any{"type": "string"},
						"user_agent":   map[string]any{"type": "string", "default": "rose-mcp/6.0"},
						"subreddit":    map[string]any{"type": "string"},
						"unmute":       map[string]any{"type": "boolean", "default": false},
						"users": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"username": map[string]any{"type": "string"},
									"note":     map[string]any{"type": "string"},
								},
								"required": []string{"username"},
							},
						},
					},
					"required": []string{"reddit_token", "subreddit", "users"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := str(p, "reddit_token", "")
				ua := str(p, "user_agent", "rose-mcp/6.0")
				sub := str(p, "subreddit", "")
				unmute := boolVal(p, "unmute", false)
				rawUsers, _ := p["users"].([]any)
				action := "muted"
				endpoint := fmt.Sprintf("/r/%s/api/friend", sub)
				if unmute {
					action = "unmuted"
					endpoint = fmt.Sprintf("/r/%s/api/unfriend", sub)
				}

				done := 0
				var errs []map[string]any
				for _, u := range rawUsers {
					user, _ := u.(map[string]any)
					if user == nil {
						continue
					}
					username := str(user, "username", "")
					if username == "" {
						continue
					}
					form := url.Values{}
					form.Set("name", username)
					form.Set("type", action)
					if n := str(user, "note", ""); n != "" {
						form.Set("note", n)
					}
					res, err := redditDo("POST", endpoint, token, ua, form)
					if err != nil {
						errs = append(errs, map[string]any{"user": username, "error": err.Error()})
						continue
					}
					if !res["ok"].(bool) {
						errs = append(errs, map[string]any{"user": username, "status": res["status"]})
					} else {
						done++
					}
				}
				return textResult(map[string]any{"ok": len(errs) == 0, "done": done, "errors": errs}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Reddit ── Moderator monitoring
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "reddit.mod.activity",
				Description: "Fetch moderator action log for a subreddit. " +
					"Filter by mod username or action type. " +
					"Returns [{mod, action, target_fullname, target_title, created_utc}]. " +
					"Use to audit mod activity and detect inactivity.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reddit_token": map[string]any{"type": "string"},
						"user_agent":   map[string]any{"type": "string", "default": "rose-mcp/6.0"},
						"subreddit":    map[string]any{"type": "string"},
						"mod":          map[string]any{"type": "string", "description": "Filter by moderator username (omit for all)"},
						"action":       map[string]any{"type": "string", "description": "e.g. removelink, approvecomment, banuser"},
						"limit":        map[string]any{"type": "number", "default": 100, "description": "Max 500"},
					},
					"required": []string{"reddit_token", "subreddit"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := str(p, "reddit_token", "")
				ua := str(p, "user_agent", "rose-mcp/6.0")
				sub := str(p, "subreddit", "")
				limit := int(num(p, "limit", 100))
				if limit > 500 {
					limit = 500
				}

				q := url.Values{}
				q.Set("limit", fmt.Sprintf("%d", limit))
				q.Set("raw_json", "1")
				if m := str(p, "mod", ""); m != "" {
					q.Set("mod", m)
				}
				if a := str(p, "action", ""); a != "" {
					q.Set("type", a)
				}

				res, err := redditDo("GET", fmt.Sprintf("/r/%s/about/log?%s", sub, q.Encode()), token, ua, nil)
				if err != nil {
					return errResult(err.Error()), nil
				}

				body, _ := res["body"].(map[string]any)
				data, _ := body["data"].(map[string]any)
				children, _ := data["children"].([]any)

				var entries []map[string]any
				for _, c := range children {
					child, _ := c.(map[string]any)
					d, _ := child["data"].(map[string]any)
					if d == nil {
						continue
					}
					entries = append(entries, map[string]any{
						"mod":             d["mod"],
						"action":          d["action"],
						"target_fullname": d["target_fullname"],
						"target_title":    d["target_title"],
						"target_author":   d["target_author"],
						"created_utc":     d["created_utc"],
					})
				}
				return textResult(map[string]any{"ok": res["ok"], "count": len(entries), "entries": entries}), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "reddit.mod.reasons",
				Description: "List removal reason UUIDs for a subreddit. Pass reason UUID to reddit.posts.act removal_reason field.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reddit_token": map[string]any{"type": "string"},
						"user_agent":   map[string]any{"type": "string", "default": "rose-mcp/6.0"},
						"subreddit":    map[string]any{"type": "string"},
					},
					"required": []string{"reddit_token", "subreddit"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := str(p, "reddit_token", "")
				ua := str(p, "user_agent", "rose-mcp/6.0")
				sub := str(p, "subreddit", "")
				res, err := redditDo("GET", fmt.Sprintf("/api/v1/%s/removal_reasons", sub), token, ua, nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				// Response: { order:[], data:{uuid:{title,message}} }
				body, _ := res["body"].(map[string]any)
				data, _ := body["data"].(map[string]any)
				order, _ := body["order"].([]any)
				var reasons []map[string]any
				for _, id := range order {
					uuid, _ := id.(string)
					r, _ := data[uuid].(map[string]any)
					if r == nil {
						continue
					}
					reasons = append(reasons, map[string]any{
						"id":      uuid,
						"title":   r["title"],
						"message": r["message"],
					})
				}
				return textResult(map[string]any{"ok": res["ok"], "reasons": reasons}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Reddit ── Submit posts / comments
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "reddit.submit",
				Description: "Submit a post or comment. " +
					"kind=link posts a URL, kind=self posts markdown text, kind=image posts an image URL. " +
					"To reply to a post/comment set parent_fullname to the fullname (t3_/t1_). " +
					"Returns {ok, url, id}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reddit_token":    map[string]any{"type": "string"},
						"user_agent":      map[string]any{"type": "string", "default": "rose-mcp/6.0"},
						"subreddit":       map[string]any{"type": "string", "description": "Required for new posts"},
						"kind":            map[string]any{"type": "string", "enum": []string{"self", "link", "image", "comment"}, "default": "self"},
						"title":           map[string]any{"type": "string", "description": "Post title (required for new posts)"},
						"text":            map[string]any{"type": "string", "description": "Markdown body (for self posts and comments)"},
						"url":             map[string]any{"type": "string", "description": "URL (for link/image posts)"},
						"parent_fullname": map[string]any{"type": "string", "description": "t3_abc to comment on post, t1_xyz to reply to comment"},
						"flair_id":        map[string]any{"type": "string"},
						"flair_text":      map[string]any{"type": "string"},
						"nsfw":            map[string]any{"type": "boolean", "default": false},
						"spoiler":         map[string]any{"type": "boolean", "default": false},
					},
					"required": []string{"reddit_token"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := str(p, "reddit_token", "")
				ua := str(p, "user_agent", "rose-mcp/6.0")
				kind := str(p, "kind", "self")

				form := url.Values{}
				form.Set("api_type", "json")
				form.Set("raw_json", "1")

				if kind == "comment" || str(p, "parent_fullname", "") != "" {
					form.Set("thing_id", str(p, "parent_fullname", ""))
					form.Set("text", str(p, "text", ""))
					res, err := redditDo("POST", "/api/comment", token, ua, form)
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": res["ok"], "body": res["body"]}), nil
				}

				form.Set("sr", str(p, "subreddit", ""))
				form.Set("kind", kind)
				form.Set("title", str(p, "title", ""))
				if kind == "self" {
					form.Set("text", str(p, "text", ""))
				} else {
					form.Set("url", str(p, "url", ""))
				}
				if fid := str(p, "flair_id", ""); fid != "" {
					form.Set("flair_id", fid)
				}
				if ft := str(p, "flair_text", ""); ft != "" {
					form.Set("flair_text", ft)
				}
				if boolVal(p, "nsfw", false) {
					form.Set("nsfw", "true")
				}
				if boolVal(p, "spoiler", false) {
					form.Set("spoiler", "true")
				}

				res, err := redditDo("POST", "/api/submit", token, ua, form)
				if err != nil {
					return errResult(err.Error()), nil
				}

				// Extract url + name from nested json.data
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					if jd, ok := b["json"].(map[string]any); ok {
						if d, ok := jd["data"].(map[string]any); ok {
							out["url"] = d["url"]
							out["id"] = d["id"]
							out["name"] = d["name"]
						}
						if errs, ok := jd["errors"].([]any); ok && len(errs) > 0 {
							out["errors"] = errs
							out["ok"] = false
						}
					}
				}
				return textResult(out), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Upstash Redis ── HTTP REST, pipeline-first
		// Auth: Authorization: Bearer <token>
		// Pipeline: one HTTP call for N commands = minimal reads on free plan.
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "db.get",
				Description: "Get one or many keys from Upstash Redis. " +
					"Single key returns value directly. Multiple keys use pipeline (1 HTTP call). " +
					"Returns {ok, results:{key:value}}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"upstash_url":   map[string]any{"type": "string", "description": "UPSTASH_REDIS_REST_URL"},
						"upstash_token": map[string]any{"type": "string", "description": "UPSTASH_REDIS_REST_TOKEN"},
						"keys":          map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "One or more keys"},
					},
					"required": []string{"upstash_url", "upstash_token", "keys"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				restURL := str(p, "upstash_url", "")
				token := str(p, "upstash_token", "")
				keys := strSlice(p, "keys")
				if len(keys) == 0 {
					return errResult("keys is required"), nil
				}
				if len(keys) == 1 {
					val, err := upstashCmd(restURL, token, "GET", keys[0])
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": true, "results": map[string]any{keys[0]: val}}), nil
				}
				// Pipeline multiple GETs
				cmds := make([][]string, len(keys))
				for i, k := range keys {
					cmds[i] = []string{"GET", k}
				}
				results, err := upstashPipeline(restURL, token, cmds)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := make(map[string]any, len(keys))
				for i, k := range keys {
					if i < len(results) {
						out[k] = results[i]
					}
				}
				return textResult(map[string]any{"ok": true, "results": out}), nil
			},
		},
		{
			Def: ToolDef{
				Name: "db.set",
				Description: "Set one or many key/value pairs. Uses pipeline for multiple keys (1 HTTP call). " +
					"Supports optional TTL per key. Returns {ok, results[]}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"upstash_url":   map[string]any{"type": "string"},
						"upstash_token": map[string]any{"type": "string"},
						"pairs": map[string]any{
							"type":        "array",
							"description": "Each: {key, value, ttl_seconds?}",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"key":         map[string]any{"type": "string"},
									"value":       map[string]any{"type": "string", "description": "Store objects as JSON strings"},
									"ttl_seconds": map[string]any{"type": "number"},
								},
								"required": []string{"key", "value"},
							},
						},
					},
					"required": []string{"upstash_url", "upstash_token", "pairs"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				restURL := str(p, "upstash_url", "")
				token := str(p, "upstash_token", "")
				rawPairs, _ := p["pairs"].([]any)
				if len(rawPairs) == 0 {
					return errResult("pairs is required"), nil
				}

				var cmds [][]string
				for _, rp := range rawPairs {
					pair, _ := rp.(map[string]any)
					if pair == nil {
						continue
					}
					k := str(pair, "key", "")
					v := str(pair, "value", "")
					if k == "" {
						continue
					}
					ttl := num(pair, "ttl_seconds", 0)
					if ttl > 0 {
						cmds = append(cmds, []string{"SETEX", k, fmt.Sprintf("%d", int(ttl)), v})
					} else {
						cmds = append(cmds, []string{"SET", k, v})
					}
				}
				if len(cmds) == 0 {
					return errResult("no valid pairs"), nil
				}

				results, err := upstashPipeline(restURL, token, cmds)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{"ok": true, "results": results}), nil
			},
		},
		{
			Def: ToolDef{
				Name: "db.del",
				Description: "Delete one or many keys in one pipeline call.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"upstash_url":   map[string]any{"type": "string"},
						"upstash_token": map[string]any{"type": "string"},
						"keys":          map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					},
					"required": []string{"upstash_url", "upstash_token", "keys"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				keys := strSlice(p, "keys")
				if len(keys) == 0 {
					return errResult("keys is required"), nil
				}
				// Single DEL command accepts multiple keys
				args := append([]string{"DEL"}, keys...)
				val, err := upstashCmd(str(p, "upstash_url", ""), str(p, "upstash_token", ""), args...)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{"ok": true, "deleted": val}), nil
			},
		},
		{
			Def: ToolDef{
				Name: "db.scan",
				Description: "Scan keys by pattern without blocking. Follows cursors server-side — returns all matching keys in one MCP result. " +
					"NEVER use KEYS * (blocks server). Use pattern e.g. 'user:*' or 'session:*'. " +
					"Returns {ok, keys[], count}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"upstash_url":   map[string]any{"type": "string"},
						"upstash_token": map[string]any{"type": "string"},
						"pattern":       map[string]any{"type": "string", "default": "*", "description": "Glob pattern e.g. user:*"},
						"count":         map[string]any{"type": "number", "default": 100, "description": "Hint per cursor page (not a hard limit)"},
					},
					"required": []string{"upstash_url", "upstash_token"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				keys, err := upstashScanAll(
					str(p, "upstash_url", ""),
					str(p, "upstash_token", ""),
					str(p, "pattern", "*"),
					int(num(p, "count", 100)),
				)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{"ok": true, "count": len(keys), "keys": keys}), nil
			},
		},
		{
			Def: ToolDef{
				Name: "db.pipeline",
				Description: "Execute arbitrary Redis commands in one HTTP call. " +
					"commands=[[\"SET\",\"k\",\"v\"],[\"GET\",\"k\"],[\"EXPIRE\",\"k\",\"60\"]]. " +
					"Use this for complex operations: HSET, ZADD, LPUSH, INCR, etc. " +
					"Returns {ok, results[]} in same order.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"upstash_url":   map[string]any{"type": "string"},
						"upstash_token": map[string]any{"type": "string"},
						"commands": map[string]any{
							"type":        "array",
							"description": "Array of Redis command arrays. All args must be strings.",
							"items":       map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
						},
					},
					"required": []string{"upstash_url", "upstash_token", "commands"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				restURL := str(p, "upstash_url", "")
				token := str(p, "upstash_token", "")
				rawCmds, _ := p["commands"].([]any)
				if len(rawCmds) == 0 {
					return errResult("commands is required"), nil
				}

				var cmds [][]string
				for _, rc := range rawCmds {
					arr, _ := rc.([]any)
					if arr == nil {
						continue
					}
					var cmd []string
					for _, a := range arr {
						cmd = append(cmd, fmt.Sprintf("%v", a))
					}
					cmds = append(cmds, cmd)
				}

				results, err := upstashPipeline(restURL, token, cmds)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{"ok": true, "results": results}), nil
			},
		},
		{
			Def: ToolDef{
				Name: "db.hash",
				Description: "Hash operations: HSET (set multiple fields), HGET (get one field), HGETALL (get all fields), HDEL (delete fields). " +
					"Hashes are ideal for storing structured objects. Returns {ok, result}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"upstash_url":   map[string]any{"type": "string"},
						"upstash_token": map[string]any{"type": "string"},
						"op":            map[string]any{"type": "string", "enum": []string{"hset", "hget", "hgetall", "hdel", "hmget"}, "description": "Operation"},
						"key":           map[string]any{"type": "string", "description": "Hash key"},
						"fields":        map[string]any{"type": "object", "description": "For hset: {field:value,...}"},
						"field":         map[string]any{"type": "string", "description": "For hget/hdel"},
						"field_names":   map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "For hmget/hdel multiple"},
					},
					"required": []string{"upstash_url", "upstash_token", "op", "key"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				restURL := str(p, "upstash_url", "")
				token := str(p, "upstash_token", "")
				op := str(p, "op", "")
				key := str(p, "key", "")

				switch op {
				case "hset":
					fields := anyMap(p, "fields")
					if len(fields) == 0 {
						return errResult("fields is required for hset"), nil
					}
					args := []string{"HSET", key}
					for f, v := range fields {
						args = append(args, f, fmt.Sprintf("%v", v))
					}
					val, err := upstashCmd(restURL, token, args...)
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": true, "result": val}), nil

				case "hget":
					val, err := upstashCmd(restURL, token, "HGET", key, str(p, "field", ""))
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": true, "result": val}), nil

				case "hgetall":
					val, err := upstashCmd(restURL, token, "HGETALL", key)
					if err != nil {
						return errResult(err.Error()), nil
					}
					// Convert flat array [k,v,k,v] to map
					if arr, ok := val.([]any); ok && len(arr)%2 == 0 {
						m := make(map[string]any, len(arr)/2)
						for i := 0; i < len(arr)-1; i += 2 {
							k, _ := arr[i].(string)
							m[k] = arr[i+1]
						}
						return textResult(map[string]any{"ok": true, "result": m}), nil
					}
					return textResult(map[string]any{"ok": true, "result": val}), nil

				case "hmget":
					fields := strSlice(p, "field_names")
					args := append([]string{"HMGET", key}, fields...)
					val, err := upstashCmd(restURL, token, args...)
					if err != nil {
						return errResult(err.Error()), nil
					}
					// Zip field names with values
					if arr, ok := val.([]any); ok {
						m := make(map[string]any, len(fields))
						for i, f := range fields {
							if i < len(arr) {
								m[f] = arr[i]
							}
						}
						return textResult(map[string]any{"ok": true, "result": m}), nil
					}
					return textResult(map[string]any{"ok": true, "result": val}), nil

				case "hdel":
					fields := strSlice(p, "field_names")
					if f := str(p, "field", ""); f != "" {
						fields = append([]string{f}, fields...)
					}
					args := append([]string{"HDEL", key}, fields...)
					val, err := upstashCmd(restURL, token, args...)
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": true, "deleted": val}), nil

				default:
					return errResult("unknown op: " + op), nil
				}
			},
		},
		{
			Def: ToolDef{
				Name: "db.list",
				Description: "List operations: push (LPUSH/RPUSH), pop (LPOP/RPOP), range (LRANGE), length (LLEN). " +
					"Lists are ordered sequences. Returns {ok, result}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"upstash_url":   map[string]any{"type": "string"},
						"upstash_token": map[string]any{"type": "string"},
						"op":            map[string]any{"type": "string", "enum": []string{"lpush", "rpush", "lpop", "rpop", "lrange", "llen"}, "description": "Operation"},
						"key":           map[string]any{"type": "string"},
						"values":        map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "For push operations"},
						"start":         map[string]any{"type": "number", "default": 0, "description": "For lrange"},
						"stop":          map[string]any{"type": "number", "default": -1, "description": "For lrange (-1=end)"},
						"count":         map[string]any{"type": "number", "default": 1, "description": "For pop"},
					},
					"required": []string{"upstash_url", "upstash_token", "op", "key"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				restURL := str(p, "upstash_url", "")
				token := str(p, "upstash_token", "")
				op := str(p, "op", "")
				key := str(p, "key", "")

				switch op {
				case "lpush", "rpush":
					vals := strSlice(p, "values")
					args := append([]string{strings.ToUpper(op), key}, vals...)
					val, err := upstashCmd(restURL, token, args...)
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": true, "length": val}), nil
				case "lpop", "rpop":
					count := int(num(p, "count", 1))
					val, err := upstashCmd(restURL, token, strings.ToUpper(op), key, fmt.Sprintf("%d", count))
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": true, "result": val}), nil
				case "lrange":
					start := int(num(p, "start", 0))
					stop := int(num(p, "stop", -1))
					val, err := upstashCmd(restURL, token, "LRANGE", key, fmt.Sprintf("%d", start), fmt.Sprintf("%d", stop))
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": true, "result": val}), nil
				case "llen":
					val, err := upstashCmd(restURL, token, "LLEN", key)
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": true, "length": val}), nil
				default:
					return errResult("unknown op: " + op), nil
				}
			},
		},
		{
			Def: ToolDef{
				Name: "db.ttl",
				Description: "Get TTL for a key (seconds), or set/remove expiry. Returns {ok, ttl} (-1=no expiry, -2=not found).",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"upstash_url":   map[string]any{"type": "string"},
						"upstash_token": map[string]any{"type": "string"},
						"key":           map[string]any{"type": "string"},
						"set_ttl":       map[string]any{"type": "number", "description": "Set expiry in seconds (0=remove expiry)"},
					},
					"required": []string{"upstash_url", "upstash_token", "key"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				restURL := str(p, "upstash_url", "")
				token := str(p, "upstash_token", "")
				key := str(p, "key", "")

				if ttl, ok := p["set_ttl"].(float64); ok {
					if ttl == 0 {
						val, err := upstashCmd(restURL, token, "PERSIST", key)
						if err != nil {
							return errResult(err.Error()), nil
						}
						return textResult(map[string]any{"ok": true, "result": val}), nil
					}
					val, err := upstashCmd(restURL, token, "EXPIRE", key, fmt.Sprintf("%d", int(ttl)))
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": true, "result": val}), nil
				}
				val, err := upstashCmd(restURL, token, "TTL", key)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{"ok": true, "ttl": val}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Telegram ── Bot API
		// Token stored in TELEGRAM_BOT_TOKEN env var (no need to pass it).
		// All tools accept optional telegram_token to override env var.
		// Auto-replies are stored in-memory in tgAutoReplies map.
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name:        "tg.send",
				Description: "Send a text message to a Telegram chat. chat_id can be a numeric ID or @username. Supports Markdown/HTML parse_mode.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"telegram_token": map[string]any{"type": "string", "description": "Bot token (omit if TELEGRAM_BOT_TOKEN env set)"},
						"chat_id":        map[string]any{"type": "string", "description": "Numeric chat ID or @username"},
						"text":           map[string]any{"type": "string"},
						"parse_mode":     map[string]any{"type": "string", "enum": []string{"Markdown", "MarkdownV2", "HTML"}, "default": "Markdown"},
						"reply_to":       map[string]any{"type": "number", "description": "message_id to reply to"},
						"silent":         map[string]any{"type": "boolean", "default": false},
					},
					"required": []string{"chat_id", "text"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{
					"chat_id":    str(p, "chat_id", ""),
					"text":       str(p, "text", ""),
					"parse_mode": str(p, "parse_mode", "Markdown"),
				}
				if rt := num(p, "reply_to", 0); rt > 0 {
					body["reply_parameters"] = map[string]any{"message_id": int(rt)}
				}
				if boolVal(p, "silent", false) {
					body["disable_notification"] = true
				}
				res, err := tgDo(env(p, "telegram_token", "TELEGRAM_BOT_TOKEN"), "sendMessage", body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					if result, ok := b["result"].(map[string]any); ok {
						out["message_id"] = result["message_id"]
						out["chat_id"] = result["chat"].(map[string]any)["id"]
					}
					if desc, ok := b["description"].(string); ok {
						out["error"] = desc
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name: "tg.broadcast",
				Description: "Send a message to multiple chats or all known users. " +
					"Pass chat_ids=[...] for specific targets, or all_users=true to send to everyone in tg.users list. " +
					"Returns {ok, sent, failed[]}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"telegram_token": map[string]any{"type": "string"},
						"chat_ids":       map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
						"all_users":      map[string]any{"type": "boolean", "default": false},
						"text":           map[string]any{"type": "string"},
						"parse_mode":     map[string]any{"type": "string", "default": "Markdown"},
					},
					"required": []string{"text"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "telegram_token", "TELEGRAM_BOT_TOKEN")
				targets := strSlice(p, "chat_ids")

				// Fetch from known users store if all_users=true
				if boolVal(p, "all_users", false) {
					tgUsersMu.RLock()
					for id := range tgKnownUsers {
						targets = append(targets, id)
					}
					tgUsersMu.RUnlock()
				}

				if len(targets) == 0 {
					return errResult("no targets: provide chat_ids or all_users=true after tg.getUpdates has run"), nil
				}

				sent, failed := 0, 0
				var errs []string
				for _, cid := range targets {
					body := map[string]any{
						"chat_id":    cid,
						"text":       str(p, "text", ""),
						"parse_mode": str(p, "parse_mode", "Markdown"),
					}
					res, err := tgDo(token, "sendMessage", body)
					if err != nil || !res["ok"].(bool) {
						failed++
						errs = append(errs, cid)
					} else {
						sent++
					}
					time.Sleep(35 * time.Millisecond) // Telegram: max ~30 msg/sec
				}
				return textResult(map[string]any{"ok": failed == 0, "sent": sent, "failed": failed, "failed_ids": errs}), nil
			},
		},
		{
			Def: ToolDef{
				Name: "tg.send_file",
				Description: "Send a photo or document to a Telegram chat. " +
					"Provide file_data as base64-encoded bytes and file_name with extension. " +
					"type=photo for images, document for any file.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"telegram_token": map[string]any{"type": "string"},
						"chat_id":        map[string]any{"type": "string"},
						"type":           map[string]any{"type": "string", "enum": []string{"photo", "document", "audio", "video"}, "default": "document"},
						"file_data":      map[string]any{"type": "string", "description": "Base64-encoded file bytes"},
						"file_name":      map[string]any{"type": "string", "description": "Filename with extension"},
						"caption":        map[string]any{"type": "string"},
						"parse_mode":     map[string]any{"type": "string", "default": "Markdown"},
					},
					"required": []string{"chat_id", "file_data", "file_name"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "telegram_token", "TELEGRAM_BOT_TOKEN")
				fileType := str(p, "type", "document")
				fields := map[string]string{
					"chat_id":    str(p, "chat_id", ""),
					"parse_mode": str(p, "parse_mode", "Markdown"),
				}
				if cap := str(p, "caption", ""); cap != "" {
					fields["caption"] = cap
				}
				res, err := tgMultipart(token, "send"+titleCase(fileType), fileType, str(p, "file_name", "file"), str(p, "file_data", ""), fields)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{"ok": res["ok"], "body": res["body"]}), nil
			},
		},
		{
			Def: ToolDef{
				Name: "tg.updates",
				Description: "Poll for new messages (long-poll getUpdates). " +
					"Saves all seen user/chat IDs for tg.broadcast. " +
					"Returns {ok, messages[{id, chat_id, from, text, date}]}. " +
					"Set offset to last update_id+1 to avoid re-processing.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"telegram_token": map[string]any{"type": "string"},
						"offset":         map[string]any{"type": "number", "description": "update_id+1 of last processed update"},
						"limit":          map[string]any{"type": "number", "default": 100},
						"timeout":        map[string]any{"type": "number", "default": 0, "description": "Long-poll seconds (0=instant)"},
					},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{
					"limit":   int(num(p, "limit", 100)),
					"timeout": int(num(p, "timeout", 0)),
				}
				if off := num(p, "offset", 0); off > 0 {
					body["offset"] = int(off)
				}
				res, err := tgDo(env(p, "telegram_token", "TELEGRAM_BOT_TOKEN"), "getUpdates", body)
				if err != nil {
					return errResult(err.Error()), nil
				}

				var messages []map[string]any
				var lastID float64
				if b, ok := res["body"].(map[string]any); ok {
					if results, ok := b["result"].([]any); ok {
						for _, r := range results {
							upd, _ := r.(map[string]any)
							if upd == nil {
								continue
							}
							uid, _ := upd["update_id"].(float64)
							if uid > lastID {
								lastID = uid
							}
							msg, _ := upd["message"].(map[string]any)
							if msg == nil {
								continue
							}
							chatID := ""
							fromID := ""
							if chat, ok := msg["chat"].(map[string]any); ok {
								chatID = fmt.Sprintf("%v", chat["id"])
								// Save to known users
								tgUsersMu.Lock()
								tgKnownUsers[chatID] = true
								tgUsersMu.Unlock()
							}
							if from, ok := msg["from"].(map[string]any); ok {
								fromID = fmt.Sprintf("%v", from["id"])
							}
							text, _ := msg["text"].(string)
							messages = append(messages, map[string]any{
								"update_id": uid,
								"chat_id":   chatID,
								"from_id":   fromID,
								"text":      text,
								"date":      msg["date"],
							})
						}
					}
				}
				return textResult(map[string]any{
					"ok":          res["ok"],
					"count":       len(messages),
					"next_offset": lastID + 1,
					"messages":    messages,
				}), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "tg.webhook.set",
				Description: "Register a webhook URL so Telegram pushes updates to your server. Pass empty url to delete webhook.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"telegram_token":  map[string]any{"type": "string"},
						"url":             map[string]any{"type": "string", "description": "HTTPS endpoint Telegram will POST to"},
						"allowed_updates": map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "e.g. [\"message\",\"callback_query\"]"},
						"secret_token":    map[string]any{"type": "string", "description": "Header X-Telegram-Bot-Api-Secret-Token value"},
					},
					"required": []string{"url"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "telegram_token", "TELEGRAM_BOT_TOKEN")
				u := str(p, "url", "")
				if u == "" {
					res, err := tgDo(token, "deleteWebhook", map[string]any{})
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": res["ok"]}), nil
				}
				body := map[string]any{"url": u}
				if au := strSlice(p, "allowed_updates"); len(au) > 0 {
					body["allowed_updates"] = au
				}
				if st := str(p, "secret_token", ""); st != "" {
					body["secret_token"] = st
				}
				res, err := tgDo(token, "setWebhook", body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{"ok": res["ok"], "body": res["body"]}), nil
			},
		},
		{
			Def: ToolDef{
				Name: "tg.autoreply.set",
				Description: "Set an auto-reply rule: when a message matches keyword, bot auto-replies with response. " +
					"Stored in server memory (resets on cold start). Exact and contains matching supported.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"keyword":    map[string]any{"type": "string", "description": "Trigger word/phrase"},
						"response":   map[string]any{"type": "string", "description": "Reply message (Markdown)"},
						"match_type": map[string]any{"type": "string", "enum": []string{"exact", "contains"}, "default": "contains"},
						"delete":     map[string]any{"type": "boolean", "default": false, "description": "Remove this rule"},
					},
					"required": []string{"keyword"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				keyword := strings.ToLower(str(p, "keyword", ""))
				if boolVal(p, "delete", false) {
					tgAutoReplyMu.Lock()
					delete(tgAutoReplies, keyword)
					tgAutoReplyMu.Unlock()
					return textResult(map[string]any{"ok": true, "deleted": keyword}), nil
				}
				tgAutoReplyMu.Lock()
				tgAutoReplies[keyword] = tgAutoReply{
					Response:  str(p, "response", ""),
					MatchType: str(p, "match_type", "contains"),
				}
				tgAutoReplyMu.Unlock()
				return textResult(map[string]any{"ok": true, "rules": len(tgAutoReplies)}), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "tg.autoreply.list",
				Description: "List all active auto-reply rules.",
				InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				tgAutoReplyMu.RLock()
				defer tgAutoReplyMu.RUnlock()
				rules := make([]map[string]any, 0, len(tgAutoReplies))
				for k, v := range tgAutoReplies {
					rules = append(rules, map[string]any{"keyword": k, "response": v.Response, "match_type": v.MatchType})
				}
				return textResult(map[string]any{"ok": true, "rules": rules}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// LinkedIn ── Posts (REST API v202504)
		// Auth: LINKEDIN_TOKEN env var or linkedin_token param.
		// author URN: LINKEDIN_AUTHOR_URN env (urn:li:person:XXX or urn:li:organization:XXX)
		// Image posting is 3-step: initUpload → binary PUT → createPost with asset URN.
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "linkedin.post",
				Description: "Create a LinkedIn text post. " +
					"visibility: PUBLIC | CONNECTIONS | LOGGED_IN. " +
					"author_urn defaults to LINKEDIN_AUTHOR_URN env var. " +
					"Returns {ok, post_id}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"linkedin_token":  map[string]any{"type": "string", "description": "OAuth2 bearer token (or set LINKEDIN_TOKEN)"},
						"author_urn":      map[string]any{"type": "string", "description": "urn:li:person:XXX or urn:li:organization:XXX (or set LINKEDIN_AUTHOR_URN)"},
						"text":            map[string]any{"type": "string", "description": "Post commentary (Markdown-like, 3000 char max)"},
						"visibility":      map[string]any{"type": "string", "enum": []string{"PUBLIC", "CONNECTIONS", "LOGGED_IN"}, "default": "PUBLIC"},
						"reshare_disabled": map[string]any{"type": "boolean", "default": false},
					},
					"required": []string{"text"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "linkedin_token", "LINKEDIN_TOKEN")
				author := env(p, "author_urn", "LINKEDIN_AUTHOR_URN")
				if author == "" {
					return errResult("author_urn required (or set LINKEDIN_AUTHOR_URN env var)"), nil
				}
				vis := str(p, "visibility", "PUBLIC")
				body := map[string]any{
					"author":      author,
					"commentary":  str(p, "text", ""),
					"visibility":  vis,
					"distribution": map[string]any{
						"feedDistribution":             "MAIN_FEED",
						"targetEntities":               []any{},
						"thirdPartyDistributionChannels": []any{},
					},
					"lifecycleState":          "PUBLISHED",
					"isReshareDisabledByAuthor": boolVal(p, "reshare_disabled", false),
				}
				res, err := liDo("POST", "/rest/posts", token, body, nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if pid, ok := res["post_id"].(string); ok {
					out["post_id"] = pid
				}
				if !res["ok"].(bool) {
					out["body"] = res["body"]
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name: "linkedin.post_image",
				Description: "Create a LinkedIn post with an image. " +
					"Step 1: registers upload URL, Step 2: uploads image bytes, Step 3: creates post. " +
					"image_data must be base64-encoded JPEG or PNG. Returns {ok, post_id}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"linkedin_token": map[string]any{"type": "string"},
						"author_urn":     map[string]any{"type": "string"},
						"text":           map[string]any{"type": "string"},
						"image_data":     map[string]any{"type": "string", "description": "Base64-encoded JPEG or PNG bytes"},
						"image_title":    map[string]any{"type": "string"},
						"visibility":     map[string]any{"type": "string", "default": "PUBLIC"},
					},
					"required": []string{"text", "image_data"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "linkedin_token", "LINKEDIN_TOKEN")
				author := env(p, "author_urn", "LINKEDIN_AUTHOR_URN")
				if author == "" {
					return errResult("author_urn required"), nil
				}

				// Step 1: Initialize image upload
				initBody := map[string]any{
					"initializeUploadRequest": map[string]any{"owner": author},
				}
				initRes, err := liDo("POST", "/rest/images?action=initializeUpload", token, initBody, nil)
				if err != nil {
					return errResult("initUpload: " + err.Error()), nil
				}
				uploadURL := ""
				imageURN := ""
				if b, ok := initRes["body"].(map[string]any); ok {
					if val, ok := b["value"].(map[string]any); ok {
						uploadURL, _ = val["uploadUrl"].(string)
						imageURN, _ = val["image"].(string)
					}
				}
				if uploadURL == "" {
					return errResult("no uploadUrl in LinkedIn response"), nil
				}

				// Step 2: Upload binary
				imgBytes, err := base64.StdEncoding.DecodeString(str(p, "image_data", ""))
				if err != nil {
					return errResult("image_data decode: " + err.Error()), nil
				}
				req, _ := http.NewRequest("PUT", uploadURL, bytes.NewReader(imgBytes))
				req.Header.Set("Authorization", "Bearer "+token)
				req.Header.Set("Content-Type", "application/octet-stream")
				http.DefaultClient.Do(req)

				// Step 3: Create post with image
				body := map[string]any{
					"author":     author,
					"commentary": str(p, "text", ""),
					"visibility": str(p, "visibility", "PUBLIC"),
					"distribution": map[string]any{
						"feedDistribution":             "MAIN_FEED",
						"targetEntities":               []any{},
						"thirdPartyDistributionChannels": []any{},
					},
					"content": map[string]any{
						"media": map[string]any{
							"title": str(p, "image_title", ""),
							"id":    imageURN,
						},
					},
					"lifecycleState":          "PUBLISHED",
					"isReshareDisabledByAuthor": false,
				}
				res, err := liDo("POST", "/rest/posts", token, body, nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if pid, ok := res["post_id"].(string); ok {
					out["post_id"] = pid
				}
				if !res["ok"].(bool) {
					out["body"] = res["body"]
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "linkedin.profile",
				Description: "Get the authenticated user's LinkedIn profile (URN, name, headline). Use to get your author_urn.",
				InputSchema: map[string]any{
					"type":       "object",
					"properties": map[string]any{"linkedin_token": map[string]any{"type": "string"}},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "linkedin_token", "LINKEDIN_TOKEN")
				res, err := liDo("GET", "/v2/userinfo", token, nil, nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{"ok": res["ok"], "body": res["body"]}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Render.com ── Deployments & Services
		// Auth: RENDER_API_KEY env var or render_api_key param.
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name:        "render.services",
				Description: "List all Render services. Returns [{id, name, type, status, url, branch}].",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"render_api_key": map[string]any{"type": "string", "description": "Render API key (or set RENDER_API_KEY)"},
						"type":           map[string]any{"type": "string", "enum": []string{"web_service", "private_service", "background_worker", "static_site", "cron_job"}},
					},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				path := "/services?limit=20"
				if t := str(p, "type", ""); t != "" {
					path += "&type=" + t
				}
				res, err := renderDo("GET", path, env(p, "render_api_key", "RENDER_API_KEY"), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if arr, ok := res["body"].([]any); ok {
					var list []map[string]any
					for _, s := range arr {
						if svc, ok := s.(map[string]any); ok {
							srv, _ := svc["service"].(map[string]any)
							if srv == nil {
								srv = svc
							}
							list = append(list, map[string]any{
								"id":     srv["id"],
								"name":   srv["name"],
								"type":   srv["type"],
								"status": srv["suspended"],
								"url":    srv["serviceDetails"],
								"branch": func() any {
									if sd, ok := srv["serviceDetails"].(map[string]any); ok {
										return sd["branch"]
									}
									return nil
								}(),
							})
						}
					}
					out["services"] = list
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "render.deploy",
				Description: "Trigger a new deployment for a Render service. Returns {ok, deploy_id, status}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"render_api_key": map[string]any{"type": "string"},
						"service_id":     map[string]any{"type": "string", "description": "Render service ID (srv-xxx)"},
						"clear_cache":    map[string]any{"type": "boolean", "default": false},
					},
					"required": []string{"service_id"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{"clearCache": boolVal(p, "clear_cache", false)}
				res, err := renderDo("POST", "/services/"+str(p, "service_id", "")+"/deploys", env(p, "render_api_key", "RENDER_API_KEY"), body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["deploy_id"] = b["id"]
					out["status"] = b["status"]
					out["created_at"] = b["createdAt"]
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "render.deploys",
				Description: "List recent deployments for a service. Returns [{id, status, createdAt, commitMsg}].",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"render_api_key": map[string]any{"type": "string"},
						"service_id":     map[string]any{"type": "string"},
						"limit":          map[string]any{"type": "number", "default": 10},
					},
					"required": []string{"service_id"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := renderDo("GET", fmt.Sprintf("/services/%s/deploys?limit=%d", str(p, "service_id", ""), int(num(p, "limit", 10))), env(p, "render_api_key", "RENDER_API_KEY"), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if arr, ok := res["body"].([]any); ok {
					var list []map[string]any
					for _, d := range arr {
						if dep, ok := d.(map[string]any); ok {
							deploy, _ := dep["deploy"].(map[string]any)
							if deploy == nil {
								deploy = dep
							}
							commitMsg := ""
							if c, ok := deploy["commit"].(map[string]any); ok {
								commitMsg, _ = c["message"].(string)
								if len(commitMsg) > 80 {
									commitMsg = commitMsg[:80]
								}
							}
							list = append(list, map[string]any{
								"id": deploy["id"], "status": deploy["status"],
								"created_at": deploy["createdAt"], "commit": commitMsg,
							})
						}
					}
					out["deploys"] = list
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "render.env",
				Description: "Get or set environment variables for a Render service. Returns {ok, vars{}}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"render_api_key": map[string]any{"type": "string"},
						"service_id":     map[string]any{"type": "string"},
						"set":            map[string]any{"type": "object", "description": "Key/value pairs to set (omit to just get)"},
					},
					"required": []string{"service_id"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				apiKey := env(p, "render_api_key", "RENDER_API_KEY")
				sid := str(p, "service_id", "")

				if setVars := anyMap(p, "set"); setVars != nil {
					var envVars []map[string]any
					for k, v := range setVars {
						envVars = append(envVars, map[string]any{"key": k, "value": fmt.Sprintf("%v", v)})
					}
					res, err := renderDo("PUT", "/services/"+sid+"/env-vars", apiKey, envVars)
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": res["ok"]}), nil
				}

				res, err := renderDo("GET", "/services/"+sid+"/env-vars", apiKey, nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if arr, ok := res["body"].([]any); ok {
					vars := map[string]any{}
					for _, e := range arr {
						if ev, ok := e.(map[string]any); ok {
							envVar, _ := ev["envVar"].(map[string]any)
							if envVar == nil {
								envVar = ev
							}
							vars[fmt.Sprintf("%v", envVar["key"])] = envVar["value"]
						}
					}
					out["vars"] = vars
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "render.logs",
				Description: "Fetch recent log lines for a Render service. Returns {ok, lines[]}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"render_api_key": map[string]any{"type": "string"},
						"service_id":     map[string]any{"type": "string"},
						"limit":          map[string]any{"type": "number", "default": 100},
					},
					"required": []string{"service_id"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := renderDo("GET", fmt.Sprintf("/services/%s/log-streams", str(p, "service_id", "")), env(p, "render_api_key", "RENDER_API_KEY"), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(map[string]any{"ok": res["ok"], "body": res["body"]}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Vercel ── Deployments, Projects, Domains (REST API v6/v9/v10)
		// NOTE: Vercel Web Analytics has NO public REST query API (dashboard only).
		// Auth: VERCEL_TOKEN env var or vercel_token param.
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name:        "vercel.deployments",
				Description: "List recent Vercel deployments. Returns [{id, name, url, state, created}].",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"vercel_token": map[string]any{"type": "string", "description": "Vercel API token (or set VERCEL_TOKEN)"},
						"team_id":      map[string]any{"type": "string", "description": "Team ID (or set VERCEL_TEAM_ID)"},
						"project":      map[string]any{"type": "string", "description": "Filter by project name"},
						"limit":        map[string]any{"type": "number", "default": 20},
					},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "vercel_token", "VERCEL_TOKEN")
				teamID := env(p, "team_id", "VERCEL_TEAM_ID")
				path := fmt.Sprintf("/v6/deployments?limit=%d", int(num(p, "limit", 20)))
				if proj := str(p, "project", ""); proj != "" {
					path += "&projectId=" + proj
				}
				res, err := vercelDo("GET", path, token, teamID, nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					if deps, ok := b["deployments"].([]any); ok {
						var list []map[string]any
						for _, d := range deps {
							if dep, ok := d.(map[string]any); ok {
								list = append(list, map[string]any{
									"id":      dep["uid"],
									"name":    dep["name"],
									"url":     dep["url"],
									"state":   dep["state"],
									"created": dep["createdAt"],
								})
							}
						}
						out["deployments"] = list
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "vercel.deploy",
				Description: "Trigger a new deployment by re-deploying the latest deployment of a project. Returns {ok, id, url}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"vercel_token":  map[string]any{"type": "string"},
						"team_id":       map[string]any{"type": "string"},
						"deployment_id": map[string]any{"type": "string", "description": "Deployment UID to redeploy"},
						"project_id":    map[string]any{"type": "string"},
					},
					"required": []string{"deployment_id"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "vercel_token", "VERCEL_TOKEN")
				teamID := env(p, "team_id", "VERCEL_TEAM_ID")
				body := map[string]any{"deploymentId": str(p, "deployment_id", ""), "target": "production"}
				if pid := str(p, "project_id", ""); pid != "" {
					body["projectId"] = pid
				}
				res, err := vercelDo("POST", "/v13/deployments", token, teamID, body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["id"] = b["id"]
					out["url"] = b["url"]
					out["state"] = b["readyState"]
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "vercel.projects",
				Description: "List Vercel projects. Returns [{id, name, framework, latestDeployUrl}].",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"vercel_token": map[string]any{"type": "string"},
						"team_id":      map[string]any{"type": "string"},
						"limit":        map[string]any{"type": "number", "default": 20},
					},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "vercel_token", "VERCEL_TOKEN")
				teamID := env(p, "team_id", "VERCEL_TEAM_ID")
				res, err := vercelDo("GET", fmt.Sprintf("/v9/projects?limit=%d", int(num(p, "limit", 20))), token, teamID, nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					if projs, ok := b["projects"].([]any); ok {
						var list []map[string]any
						for _, pr := range projs {
							if proj, ok := pr.(map[string]any); ok {
								latestURL := ""
								if ld, ok := proj["latestDeployments"].([]any); ok && len(ld) > 0 {
									if dep, ok := ld[0].(map[string]any); ok {
										latestURL, _ = dep["url"].(string)
									}
								}
								list = append(list, map[string]any{
									"id": proj["id"], "name": proj["name"],
									"framework": proj["framework"], "latest_url": latestURL,
								})
							}
						}
						out["projects"] = list
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "vercel.dns",
				Description: "List or add DNS records for a Vercel domain. op=list or add. Returns {ok, records[]}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"vercel_token": map[string]any{"type": "string"},
						"team_id":      map[string]any{"type": "string"},
						"domain":       map[string]any{"type": "string", "description": "e.g. example.com"},
						"op":           map[string]any{"type": "string", "enum": []string{"list", "add", "delete"}, "default": "list"},
						"type":         map[string]any{"type": "string", "description": "For add: A, CNAME, MX, TXT, etc."},
						"name":         map[string]any{"type": "string", "description": "For add: subdomain or @ for root"},
						"value":        map[string]any{"type": "string", "description": "For add: record value"},
						"ttl":          map[string]any{"type": "number", "default": 3600},
						"record_id":    map[string]any{"type": "string", "description": "For delete"},
					},
					"required": []string{"domain", "op"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "vercel_token", "VERCEL_TOKEN")
				teamID := env(p, "team_id", "VERCEL_TEAM_ID")
				domain := str(p, "domain", "")
				switch str(p, "op", "list") {
				case "list":
					res, err := vercelDo("GET", "/v4/domains/"+domain+"/records", token, teamID, nil)
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": res["ok"], "body": res["body"]}), nil
				case "add":
					body := map[string]any{
						"type":  str(p, "type", ""),
						"name":  str(p, "name", "@"),
						"value": str(p, "value", ""),
						"ttl":   int(num(p, "ttl", 3600)),
					}
					res, err := vercelDo("POST", "/v2/domains/"+domain+"/records", token, teamID, body)
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": res["ok"], "body": res["body"]}), nil
				case "delete":
					rid := str(p, "record_id", "")
					res, err := vercelDo("DELETE", "/v2/domains/"+domain+"/records/"+rid, token, teamID, nil)
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": res["ok"]}), nil
				}
				return errResult("unknown op"), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "vercel.env",
				Description: "Get or set environment variables for a Vercel project. Returns {ok, vars{}}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"vercel_token": map[string]any{"type": "string"},
						"team_id":      map[string]any{"type": "string"},
						"project_id":   map[string]any{"type": "string"},
						"set":          map[string]any{"type": "object", "description": "Key/value map to create/update (omit to list)"},
						"target":       map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Deployment targets: production, preview, development"},
					},
					"required": []string{"project_id"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "vercel_token", "VERCEL_TOKEN")
				teamID := env(p, "team_id", "VERCEL_TEAM_ID")
				pid := str(p, "project_id", "")

				if setVars := anyMap(p, "set"); setVars != nil {
					targets := strSlice(p, "target")
					if len(targets) == 0 {
						targets = []string{"production", "preview", "development"}
					}
					var envs []map[string]any
					for k, v := range setVars {
						envs = append(envs, map[string]any{
							"key":    k,
							"value":  fmt.Sprintf("%v", v),
							"type":   "plain",
							"target": targets,
						})
					}
					res, err := vercelDo("POST", "/v10/projects/"+pid+"/env", token, teamID, envs)
					if err != nil {
						return errResult(err.Error()), nil
					}
					return textResult(map[string]any{"ok": res["ok"]}), nil
				}
				res, err := vercelDo("GET", "/v9/projects/"+pid+"/env", token, teamID, nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					if envs, ok := b["envs"].([]any); ok {
						vars := map[string]any{}
						for _, e := range envs {
							if ev, ok := e.(map[string]any); ok {
								vars[fmt.Sprintf("%v", ev["key"])] = map[string]any{"type": ev["type"], "target": ev["target"]}
							}
						}
						out["vars"] = vars
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name: "vercel.pages.deploy",
				Description: "Deploy a single static HTML/CSS/JS file to Vercel as a serverless deployment (GitHub Pages equivalent). " +
					"files is a map of {path: content_string}. Returns {ok, url}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"vercel_token": map[string]any{"type": "string"},
						"team_id":      map[string]any{"type": "string"},
						"project_name": map[string]any{"type": "string", "description": "Vercel project name"},
						"files":        map[string]any{"type": "object", "description": "Map of path→content, e.g. {\"index.html\": \"<h1>hi</h1>\"}"},
					},
					"required": []string{"project_name", "files"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "vercel_token", "VERCEL_TOKEN")
				teamID := env(p, "team_id", "VERCEL_TEAM_ID")
				files := anyMap(p, "files")
				if files == nil {
					return errResult("files is required"), nil
				}

				var deployFiles []map[string]any
				for path, content := range files {
					deployFiles = append(deployFiles, map[string]any{
						"file":     path,
						"data":     fmt.Sprintf("%v", content),
						"encoding": "utf-8",
					})
				}
				body := map[string]any{
					"name":   str(p, "project_name", ""),
					"files":  deployFiles,
					"target": "production",
				}
				res, err := vercelDo("POST", "/v13/deployments", token, teamID, body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["url"] = b["url"]
					out["id"] = b["id"]
					out["state"] = b["readyState"]
				}
				return textResult(out), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// GitHub Pages ── via github.file.write + repo settings
		// Enabling Pages = set source branch/path via Repos API.
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "github.pages.enable",
				Description: "Enable or update GitHub Pages for a repo. " +
					"source_branch defaults to main, source_path defaults to / (root) or /docs. " +
					"Returns {ok, url}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": ghProps(map[string]any{
						"source_branch": map[string]any{"type": "string", "default": "main"},
						"source_path":   map[string]any{"type": "string", "enum": []string{"/", "/docs"}, "default": "/"},
					}),
					"required": []string{"owner", "repo"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "github_token", "GITHUB_TOKEN")
				body := map[string]any{
					"source": map[string]any{
						"branch": str(p, "source_branch", "main"),
						"path":   str(p, "source_path", "/"),
					},
				}
				// POST creates, PUT updates
				path := fmt.Sprintf("/repos/%s/%s/pages", str(p, "owner", ""), str(p, "repo", ""))
				res, err := ghDo("POST", path, token, body)
				if err != nil {
					return errResult(err.Error()), nil
				}
				// If already exists, use PUT
				if s, ok := res["status"].(int); ok && s == 409 {
					res, err = ghDo("PUT", path, token, body)
					if err != nil {
						return errResult(err.Error()), nil
					}
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["url"] = b["html_url"]
					out["status"] = b["status"]
					if msg, ok := b["message"].(string); ok {
						out["error"] = msg
					}
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "github.pages.status",
				Description: "Get GitHub Pages deployment status for a repo.",
				InputSchema: map[string]any{
					"type":       "object",
					"properties": ghProps(nil),
					"required":   []string{"owner", "repo"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := ghDo("GET", fmt.Sprintf("/repos/%s/%s/pages", str(p, "owner", ""), str(p, "repo", "")), env(p, "github_token", "GITHUB_TOKEN"), nil)
				if err != nil {
					return errResult(err.Error()), nil
				}
				out := map[string]any{"ok": res["ok"]}
				if b, ok := res["body"].(map[string]any); ok {
					out["url"] = b["html_url"]
					out["status"] = b["status"]
					out["source"] = b["source"]
				}
				return textResult(out), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// UptimeRobot ── Monitor management
		// Auth: UPTIMEROBOT_API_KEY env var or uptimerobot_api_key param.
		// Free plan: up to 50 monitors, 5-minute check intervals.
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name:        "uptime.monitors",
				Description: "List all UptimeRobot monitors. Returns [{id, name, url, status, uptime_ratio}].",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"uptimerobot_api_key": map[string]any{"type": "string", "description": "UptimeRobot API key (or set UPTIMEROBOT_API_KEY)"},
					},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				apiKey := env(p, "uptimerobot_api_key", "UPTIMEROBOT_API_KEY")
				form := url.Values{}
				form.Set("api_key", apiKey)
				form.Set("format", "json")
				form.Set("custom_uptime_ratios", "7-30")
				req, err := http.NewRequest("POST", "https://api.uptimerobot.com/v2/getMonitors", strings.NewReader(form.Encode()))
				if err != nil {
					return errResult(err.Error()), nil
				}
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return errResult(err.Error()), nil
				}
				defer resp.Body.Close()
				raw, _ := io.ReadAll(resp.Body)
				var parsed map[string]any
				_ = json.Unmarshal(raw, &parsed)

				out := map[string]any{"ok": parsed["stat"] == "ok"}
				if monitors, ok := parsed["monitors"].([]any); ok {
					statusMap := map[float64]string{0: "paused", 1: "not_checked", 2: "up", 8: "seems_down", 9: "down"}
					var list []map[string]any
					for _, m := range monitors {
						if mon, ok := m.(map[string]any); ok {
							st, _ := mon["status"].(float64)
							list = append(list, map[string]any{
								"id": mon["id"], "name": mon["friendly_name"],
								"url": mon["url"], "status": statusMap[st],
								"uptime_7d":  mon["custom_uptime_ratio"],
							})
						}
					}
					out["monitors"] = list
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "uptime.add",
				Description: "Add a new UptimeRobot monitor. type: http(1) keyword(2) ping(3) port(4). Returns {ok, id}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"uptimerobot_api_key": map[string]any{"type": "string"},
						"name":                map[string]any{"type": "string"},
						"url":                 map[string]any{"type": "string", "format": "uri"},
						"type":                map[string]any{"type": "number", "enum": []int{1, 2, 3, 4}, "default": 1, "description": "1=HTTP 2=keyword 3=ping 4=port"},
						"interval":            map[string]any{"type": "number", "default": 300, "description": "Check interval seconds (free plan min 300)"},
						"alert_contacts":      map[string]any{"type": "string", "description": "Alert contact IDs separated by -"},
					},
					"required": []string{"name", "url"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				apiKey := env(p, "uptimerobot_api_key", "UPTIMEROBOT_API_KEY")
				form := url.Values{}
				form.Set("api_key", apiKey)
				form.Set("format", "json")
				form.Set("friendly_name", str(p, "name", ""))
				form.Set("url", str(p, "url", ""))
				form.Set("type", fmt.Sprintf("%d", int(num(p, "type", 1))))
				form.Set("interval", fmt.Sprintf("%d", int(num(p, "interval", 300))))
				if ac := str(p, "alert_contacts", ""); ac != "" {
					form.Set("alert_contacts", ac)
				}
				req, _ := http.NewRequest("POST", "https://api.uptimerobot.com/v2/newMonitor", strings.NewReader(form.Encode()))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return errResult(err.Error()), nil
				}
				defer resp.Body.Close()
				raw, _ := io.ReadAll(resp.Body)
				var parsed map[string]any
				_ = json.Unmarshal(raw, &parsed)
				out := map[string]any{"ok": parsed["stat"] == "ok"}
				if mon, ok := parsed["monitor"].(map[string]any); ok {
					out["id"] = mon["id"]
				}
				if e, ok := parsed["error"].(map[string]any); ok {
					out["error"] = e["message"]
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "uptime.delete",
				Description: "Delete a UptimeRobot monitor by ID.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"uptimerobot_api_key": map[string]any{"type": "string"},
						"monitor_id":          map[string]any{"type": "number"},
					},
					"required": []string{"monitor_id"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				apiKey := env(p, "uptimerobot_api_key", "UPTIMEROBOT_API_KEY")
				form := url.Values{}
				form.Set("api_key", apiKey)
				form.Set("format", "json")
				form.Set("id", fmt.Sprintf("%d", int(num(p, "monitor_id", 0))))
				req, _ := http.NewRequest("POST", "https://api.uptimerobot.com/v2/deleteMonitor", strings.NewReader(form.Encode()))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return errResult(err.Error()), nil
				}
				defer resp.Body.Close()
				raw, _ := io.ReadAll(resp.Body)
				var parsed map[string]any
				_ = json.Unmarshal(raw, &parsed)
				return textResult(map[string]any{"ok": parsed["stat"] == "ok"}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// TickTick ── Tasks (OAuth2, token via env TICKTICK_TOKEN)
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "ticktick.task.create",
				Description: "Create a task in TickTick. Returns {ok, id, title}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"ticktick_token": map[string]any{"type": "string", "description": "OAuth2 access token (or set TICKTICK_TOKEN)"},
						"title":          map[string]any{"type": "string"},
						"content":        map[string]any{"type": "string", "description": "Description/body"},
						"project_id":     map[string]any{"type": "string", "description": "Project/list ID (omit for inbox)"},
						"due_date":       map[string]any{"type": "string", "description": "ISO 8601 e.g. 2026-04-01T09:00:00+00:00"},
						"priority":       map[string]any{"type": "number", "enum": []int{0, 1, 3, 5}, "default": 0, "description": "0=none 1=low 3=medium 5=high"},
						"tags":           map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					},
					"required": []string{"title"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "ticktick_token", "TICKTICK_TOKEN")
				body := map[string]any{
					"title":    str(p, "title", ""),
					"priority": int(num(p, "priority", 0)),
				}
				if c := str(p, "content", ""); c != "" {
					body["content"] = c
				}
				if pid := str(p, "project_id", ""); pid != "" {
					body["projectId"] = pid
				}
				if d := str(p, "due_date", ""); d != "" {
					body["dueDate"] = d
				}
				if tags := strSlice(p, "tags"); len(tags) > 0 {
					body["tags"] = tags
				}
				b, _ := json.Marshal(body)
				req, err := http.NewRequest("POST", "https://api.ticktick.com/open/v1/task", bytes.NewReader(b))
				if err != nil {
					return errResult(err.Error()), nil
				}
				req.Header.Set("Authorization", "Bearer "+token)
				req.Header.Set("Content-Type", "application/json")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return errResult(err.Error()), nil
				}
				defer resp.Body.Close()
				raw, _ := io.ReadAll(resp.Body)
				var parsed map[string]any
				_ = json.Unmarshal(raw, &parsed)
				out := map[string]any{"ok": resp.StatusCode < 300}
				out["id"] = parsed["id"]
				out["title"] = parsed["title"]
				if e, ok := parsed["errorMessage"].(string); ok {
					out["error"] = e
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "ticktick.task.list",
				Description: "List tasks in a project or all tasks. Returns [{id, title, status, dueDate, priority}].",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"ticktick_token": map[string]any{"type": "string"},
						"project_id":     map[string]any{"type": "string", "description": "Omit for all projects"},
					},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "ticktick_token", "TICKTICK_TOKEN")
				path := "https://api.ticktick.com/open/v1/project"
				if pid := str(p, "project_id", ""); pid != "" {
					path = fmt.Sprintf("https://api.ticktick.com/open/v1/project/%s/data", pid)
				}
				req, _ := http.NewRequest("GET", path, nil)
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return errResult(err.Error()), nil
				}
				defer resp.Body.Close()
				raw, _ := io.ReadAll(resp.Body)
				var parsed any
				_ = json.Unmarshal(raw, &parsed)
				return textResult(map[string]any{"ok": resp.StatusCode < 300, "body": parsed}), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "ticktick.task.complete",
				Description: "Mark a task as completed.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"ticktick_token": map[string]any{"type": "string"},
						"project_id":     map[string]any{"type": "string"},
						"task_id":        map[string]any{"type": "string"},
					},
					"required": []string{"project_id", "task_id"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "ticktick_token", "TICKTICK_TOKEN")
				path := fmt.Sprintf("https://api.ticktick.com/open/v1/project/%s/task/%s/complete", str(p, "project_id", ""), str(p, "task_id", ""))
				req, _ := http.NewRequest("POST", path, nil)
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return errResult(err.Error()), nil
				}
				resp.Body.Close()
				return textResult(map[string]any{"ok": resp.StatusCode < 300}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// Habitica ── Gamified habits & tasks
		// Auth: HABITICA_USER_ID + HABITICA_API_KEY env vars
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name:        "habitica.tasks",
				Description: "List Habitica tasks. type: habits|dailys|todos|rewards. Returns [{id, text, type, value, due}].",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"habitica_user_id": map[string]any{"type": "string", "description": "User ID (or set HABITICA_USER_ID)"},
						"habitica_api_key": map[string]any{"type": "string", "description": "API key (or set HABITICA_API_KEY)"},
						"type":             map[string]any{"type": "string", "enum": []string{"habits", "dailys", "todos", "rewards", "completedTodos"}, "default": "todos"},
					},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				userID := env(p, "habitica_user_id", "HABITICA_USER_ID")
				apiKey := env(p, "habitica_api_key", "HABITICA_API_KEY")
				taskType := str(p, "type", "todos")
				req, _ := http.NewRequest("GET", "https://habitica.com/api/v3/tasks/user?type="+taskType, nil)
				req.Header.Set("x-api-user", userID)
				req.Header.Set("x-api-key", apiKey)
				req.Header.Set("x-client", "rose-mcp-7.0")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return errResult(err.Error()), nil
				}
				defer resp.Body.Close()
				raw, _ := io.ReadAll(resp.Body)
				var parsed map[string]any
				_ = json.Unmarshal(raw, &parsed)
				out := map[string]any{"ok": parsed["success"] == true}
				if data, ok := parsed["data"].([]any); ok {
					var list []map[string]any
					for _, t := range data {
						if task, ok := t.(map[string]any); ok {
							list = append(list, map[string]any{
								"id": task["id"], "text": task["text"],
								"type": task["type"], "value": task["value"],
								"due": task["date"],
							})
						}
					}
					out["tasks"] = list
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name: "habitica.task.create",
				Description: "Create a Habitica task (habit/daily/todo/reward).",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"habitica_user_id": map[string]any{"type": "string"},
						"habitica_api_key": map[string]any{"type": "string"},
						"text":             map[string]any{"type": "string"},
						"type":             map[string]any{"type": "string", "enum": []string{"habit", "daily", "todo", "reward"}, "default": "todo"},
						"notes":            map[string]any{"type": "string"},
						"due_date":         map[string]any{"type": "string", "description": "ISO 8601"},
						"priority":         map[string]any{"type": "number", "enum": []float64{0.1, 1, 1.5, 2}, "default": 1, "description": "0.1=trivial 1=easy 1.5=medium 2=hard"},
						"tags":             map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Tag UUIDs"},
					},
					"required": []string{"text"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				userID := env(p, "habitica_user_id", "HABITICA_USER_ID")
				apiKey := env(p, "habitica_api_key", "HABITICA_API_KEY")
				body := map[string]any{
					"text":     str(p, "text", ""),
					"type":     str(p, "type", "todo"),
					"priority": num(p, "priority", 1),
				}
				if n := str(p, "notes", ""); n != "" {
					body["notes"] = n
				}
				if d := str(p, "due_date", ""); d != "" {
					body["date"] = d
				}
				if tags := strSlice(p, "tags"); len(tags) > 0 {
					body["tags"] = tags
				}
				b, _ := json.Marshal(body)
				req, _ := http.NewRequest("POST", "https://habitica.com/api/v3/tasks/user", bytes.NewReader(b))
				req.Header.Set("x-api-user", userID)
				req.Header.Set("x-api-key", apiKey)
				req.Header.Set("x-client", "rose-mcp-7.0")
				req.Header.Set("Content-Type", "application/json")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return errResult(err.Error()), nil
				}
				defer resp.Body.Close()
				raw, _ := io.ReadAll(resp.Body)
				var parsed map[string]any
				_ = json.Unmarshal(raw, &parsed)
				out := map[string]any{"ok": parsed["success"] == true}
				if data, ok := parsed["data"].(map[string]any); ok {
					out["id"] = data["id"]
					out["text"] = data["text"]
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "habitica.task.score",
				Description: "Score (tick) a habit/daily/todo. direction: up or down.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"habitica_user_id": map[string]any{"type": "string"},
						"habitica_api_key": map[string]any{"type": "string"},
						"task_id":          map[string]any{"type": "string"},
						"direction":        map[string]any{"type": "string", "enum": []string{"up", "down"}, "default": "up"},
					},
					"required": []string{"task_id"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				userID := env(p, "habitica_user_id", "HABITICA_USER_ID")
				apiKey := env(p, "habitica_api_key", "HABITICA_API_KEY")
				path := fmt.Sprintf("https://habitica.com/api/v3/tasks/%s/score/%s", str(p, "task_id", ""), str(p, "direction", "up"))
				req, _ := http.NewRequest("POST", path, nil)
				req.Header.Set("x-api-user", userID)
				req.Header.Set("x-api-key", apiKey)
				req.Header.Set("x-client", "rose-mcp-7.0")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return errResult(err.Error()), nil
				}
				defer resp.Body.Close()
				raw, _ := io.ReadAll(resp.Body)
				var parsed map[string]any
				_ = json.Unmarshal(raw, &parsed)
				return textResult(map[string]any{"ok": parsed["success"] == true, "delta": parsed["data"]}), nil
			},
		},

		// ════════════════════════════════════════════════════════════════════
		// IMAP ── Read-only email access (Outlook, Gmail, any IMAP server)
		// Uses net/http to call Outlook Graph API for Outlook mail,
		// or direct IMAP-over-REST via Zapier-style proxy for generic IMAP.
		// For Outlook: OUTLOOK_TOKEN env var (Microsoft Graph OAuth2 token).
		// ════════════════════════════════════════════════════════════════════
		{
			Def: ToolDef{
				Name: "mail.read",
				Description: "Read emails from Outlook/Microsoft 365 via Microsoft Graph API. " +
					"Returns [{id, subject, from, date, preview(200ch), hasAttachments}]. " +
					"folder: inbox|sentitems|drafts|deleteditems. " +
					"Auth: OUTLOOK_TOKEN env var (OAuth2 token with Mail.Read scope).",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"outlook_token": map[string]any{"type": "string", "description": "Microsoft Graph token (or set OUTLOOK_TOKEN)"},
						"folder":        map[string]any{"type": "string", "default": "inbox", "description": "inbox|sentitems|drafts|deleteditems"},
						"top":           map[string]any{"type": "number", "default": 20, "description": "Max messages to return"},
						"filter":        map[string]any{"type": "string", "description": "OData filter e.g. isRead eq false"},
						"search":        map[string]any{"type": "string", "description": "Search query"},
					},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "outlook_token", "OUTLOOK_TOKEN")
				if token == "" {
					return errResult("outlook_token required (or set OUTLOOK_TOKEN with Mail.Read scope)"), nil
				}
				folder := str(p, "folder", "inbox")
				top := int(num(p, "top", 20))
				u := fmt.Sprintf("https://graph.microsoft.com/v1.0/me/mailFolders/%s/messages?$top=%d&$select=id,subject,from,receivedDateTime,bodyPreview,hasAttachments,isRead", folder, top)
				if f := str(p, "filter", ""); f != "" {
					u += "&$filter=" + url.QueryEscape(f)
				}
				if s := str(p, "search", ""); s != "" {
					u = fmt.Sprintf("https://graph.microsoft.com/v1.0/me/messages?$search=%s&$top=%d&$select=id,subject,from,receivedDateTime,bodyPreview,hasAttachments,isRead", url.QueryEscape(`"`+s+`"`), top)
				}
				req, _ := http.NewRequest("GET", u, nil)
				req.Header.Set("Authorization", "Bearer "+token)
				req.Header.Set("Accept", "application/json")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return errResult(err.Error()), nil
				}
				defer resp.Body.Close()
				raw, _ := io.ReadAll(resp.Body)
				var parsed map[string]any
				_ = json.Unmarshal(raw, &parsed)

				out := map[string]any{"ok": resp.StatusCode < 300}
				if msgs, ok := parsed["value"].([]any); ok {
					var list []map[string]any
					for _, m := range msgs {
						if msg, ok := m.(map[string]any); ok {
							fromAddr := ""
							if from, ok := msg["from"].(map[string]any); ok {
								if ep, ok := from["emailAddress"].(map[string]any); ok {
									fromAddr = fmt.Sprintf("%v <%v>", ep["name"], ep["address"])
								}
							}
							preview, _ := msg["bodyPreview"].(string)
							if len(preview) > 200 {
								preview = preview[:200] + "…"
							}
							list = append(list, map[string]any{
								"id": msg["id"], "subject": msg["subject"],
								"from": fromAddr, "date": msg["receivedDateTime"],
								"preview": preview, "hasAttachments": msg["hasAttachments"],
								"isRead": msg["isRead"],
							})
						}
					}
					out["count"] = len(list)
					out["messages"] = list
				}
				if e, ok := parsed["error"].(map[string]any); ok {
					out["error"] = e["message"]
					out["ok"] = false
				}
				return textResult(out), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "mail.read_message",
				Description: "Read full body of a specific email by ID (from mail.read). Returns {ok, subject, body, from, attachments[]}.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"outlook_token": map[string]any{"type": "string"},
						"message_id":    map[string]any{"type": "string"},
					},
					"required": []string{"message_id"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				token := env(p, "outlook_token", "OUTLOOK_TOKEN")
				mid := str(p, "message_id", "")
				req, _ := http.NewRequest("GET", "https://graph.microsoft.com/v1.0/me/messages/"+mid+"?$select=id,subject,from,body,hasAttachments,receivedDateTime", nil)
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return errResult(err.Error()), nil
				}
				defer resp.Body.Close()
				raw, _ := io.ReadAll(resp.Body)
				var msg map[string]any
				_ = json.Unmarshal(raw, &msg)
				out := map[string]any{"ok": resp.StatusCode < 300}
				out["subject"] = msg["subject"]
				out["date"] = msg["receivedDateTime"]
				if from, ok := msg["from"].(map[string]any); ok {
					if ep, ok := from["emailAddress"].(map[string]any); ok {
						out["from"] = fmt.Sprintf("%v <%v>", ep["name"], ep["address"])
					}
				}
				if body, ok := msg["body"].(map[string]any); ok {
					content, _ := body["content"].(string)
					// Strip HTML tags for token efficiency
					content = stripHTMLTags(content)
					if len(content) > 3000 {
						content = content[:3000] + "…"
					}
					out["body"] = content
				}
				return textResult(out), nil
			},
		},
	}
}

// ── Reddit client ─────────────────────────────────────────────────────────────
//
// Reddit OAuth2 rate limit: 600 requests per 10-minute window.
// Every response carries X-Ratelimit-Remaining + X-Ratelimit-Reset headers.
// redditRL tracks the last-known remaining count so bulk actions can back-pressure.

type redditRateState struct {
	mu        sync.Mutex
	remaining float64
	resetAt   time.Time
}

var redditRL = &redditRateState{remaining: 600}

func (s *redditRateState) update(remaining float64, resetSecs float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remaining = remaining
	s.resetAt = time.Now().Add(time.Duration(resetSecs) * time.Second)
}

func (s *redditRateState) wait() {
	s.mu.Lock()
	rem := s.remaining
	reset := s.resetAt
	s.mu.Unlock()
	if rem < 5 {
		sleep := time.Until(reset)
		if sleep > 0 {
			time.Sleep(sleep + 500*time.Millisecond)
		}
	}
}

// redditToken acquires a bearer token using the script/password flow.
// Returns the token string or an error.
func redditToken(clientID, clientSecret, username, password, userAgent string) (string, error) {
	// Fall back to env vars
	if clientID == "" {
		clientID = os.Getenv("REDDIT_CLIENT_ID")
	}
	if clientSecret == "" {
		clientSecret = os.Getenv("REDDIT_CLIENT_SECRET")
	}
	if username == "" {
		username = os.Getenv("REDDIT_USERNAME")
	}
	if password == "" {
		password = os.Getenv("REDDIT_PASSWORD")
	}
	if userAgent == "" {
		userAgent = "rose-mcp/7.0"
	}

	body := url.Values{}
	body.Set("grant_type", "password")
	body.Set("username", username)
	body.Set("password", password)

	req, err := http.NewRequest("POST", "https://www.reddit.com/api/v1/access_token", strings.NewReader(body.Encode()))
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(clientID, clientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", userAgent)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", fmt.Errorf("token parse: %s", string(raw))
	}
	if e, ok := parsed["error"].(string); ok {
		return "", fmt.Errorf("reddit auth: %s", e)
	}
	token, ok := parsed["access_token"].(string)
	if !ok {
		return "", fmt.Errorf("no access_token in response: %s", string(raw))
	}
	return token, nil
}

// redditDo executes a Reddit OAuth API call and updates rate-limit state.
func redditDo(method, path, token, userAgent string, form url.Values) (map[string]any, error) {
	redditRL.wait()

	var bodyReader io.Reader
	if form != nil {
		bodyReader = strings.NewReader(form.Encode())
	}

	req, err := http.NewRequest(method, "https://oauth.reddit.com"+path, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", userAgent)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Update rate-limit state from headers
	if rem := resp.Header.Get("X-Ratelimit-Remaining"); rem != "" {
		var r, s float64
		fmt.Sscanf(rem, "%f", &r)
		fmt.Sscanf(resp.Header.Get("X-Ratelimit-Reset"), "%f", &s)
		redditRL.update(r, s)
	}

	raw, _ := io.ReadAll(resp.Body)
	var parsed any
	_ = json.Unmarshal(raw, &parsed)
	return map[string]any{
		"status": resp.StatusCode,
		"ok":     resp.StatusCode >= 200 && resp.StatusCode < 300,
		"body":   parsed,
		"rate":   map[string]any{"remaining": redditRL.remaining},
	}, nil
}

// ── Upstash client ────────────────────────────────────────────────────────────

// upstashCmd sends a single Redis command via REST GET-style URL.
func upstashCmd(restURL, token string, args ...string) (any, error) {
	if restURL == "" {
		restURL = os.Getenv("UPSTASH_REDIS_REST_URL")
	}
	if token == "" {
		token = os.Getenv("UPSTASH_REDIS_REST_TOKEN")
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("no command")
	}
	parts := make([]string, len(args))
	for i, a := range args {
		parts[i] = url.PathEscape(a)
	}
	u := strings.TrimRight(restURL, "/") + "/" + strings.Join(parts, "/")

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var parsed map[string]any
	_ = json.Unmarshal(raw, &parsed)
	if e, ok := parsed["error"].(string); ok {
		return nil, fmt.Errorf("upstash: %s", e)
	}
	return parsed["result"], nil
}

// upstashPipeline sends multiple commands in one HTTP call via /pipeline.
// commands is [][]string e.g. [["SET","k","v"],["GET","k"]].
// Returns []any results in same order.
func upstashPipeline(restURL, token string, commands [][]string) ([]any, error) {
	if restURL == "" {
		restURL = os.Getenv("UPSTASH_REDIS_REST_URL")
	}
	if token == "" {
		token = os.Getenv("UPSTASH_REDIS_REST_TOKEN")
	}
	payload := make([][]string, len(commands))
	copy(payload, commands)
	b, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", strings.TrimRight(restURL, "/")+"/pipeline", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	var results []map[string]any
	if err := json.Unmarshal(raw, &results); err != nil {
		return nil, fmt.Errorf("pipeline parse: %s", string(raw))
	}
	out := make([]any, len(results))
	for i, r := range results {
		if e, ok := r["error"].(string); ok {
			out[i] = map[string]any{"error": e}
		} else {
			out[i] = r["result"]
		}
	}
	return out, nil
}

// upstashScanAll scans all keys matching pattern, following cursors server-side.
func upstashScanAll(restURL, token, pattern string, count int) ([]string, error) {
	if count <= 0 {
		count = 100
	}
	var allKeys []string
	cursor := "0"
	for {
		args := []string{"SCAN", cursor, "MATCH", pattern, "COUNT", fmt.Sprintf("%d", count)}
		result, err := upstashCmd(restURL, token, args...)
		if err != nil {
			return nil, err
		}
		// result is []any{cursor_string, []any{keys...}}
		arr, ok := result.([]any)
		if !ok || len(arr) < 2 {
			break
		}
		cursor, _ = arr[0].(string)
		if keys, ok := arr[1].([]any); ok {
			for _, k := range keys {
				if s, ok := k.(string); ok {
					allKeys = append(allKeys, s)
				}
			}
		}
		if cursor == "0" {
			break
		}
	}
	return allKeys, nil
}

// ── Telegram client ───────────────────────────────────────────────────────────

func tgDo(token, method string, body map[string]any) (map[string]any, error) {
	if token == "" {
		token = os.Getenv("TELEGRAM_BOT_TOKEN")
	}
	if token == "" {
		return nil, fmt.Errorf("telegram token required (or set TELEGRAM_BOT_TOKEN)")
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequest("POST", tgBase+token+"/"+method, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var parsed any
	_ = json.Unmarshal(raw, &parsed)
	return map[string]any{"status": resp.StatusCode, "ok": resp.StatusCode < 300, "body": parsed}, nil
}

// tgMultipart sends a file to Telegram using multipart/form-data.
// fileData is base64-encoded bytes from the tool caller.
// fields contains non-file form fields (chat_id, caption, etc).
func tgMultipart(token, method, fileField, fileName string, fileDataB64 string, fields map[string]string) (map[string]any, error) {
	if token == "" {
		token = os.Getenv("TELEGRAM_BOT_TOKEN")
	}
	fileData, err := base64.StdEncoding.DecodeString(fileDataB64)
	if err != nil {
		return nil, fmt.Errorf("file_data must be base64: %w", err)
	}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, v := range fields {
		_ = mw.WriteField(k, v)
	}
	fw, _ := mw.CreateFormFile(fileField, fileName)
	_, _ = fw.Write(fileData)
	mw.Close()

	req, err2 := http.NewRequest("POST", tgBase+token+"/"+method, &buf)
	if err2 != nil {
		return nil, err2
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err2 := http.DefaultClient.Do(req)
	if err2 != nil {
		return nil, err2
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var parsed any
	_ = json.Unmarshal(raw, &parsed)
	return map[string]any{"status": resp.StatusCode, "ok": resp.StatusCode < 300, "body": parsed}, nil
}

// ── LinkedIn client ───────────────────────────────────────────────────────────

func liDo(method, path, token string, body any, extraHeaders map[string]string) (map[string]any, error) {
	if token == "" {
		token = os.Getenv("LINKEDIN_TOKEN")
	}
	if token == "" {
		return nil, fmt.Errorf("linkedin_token required (or set LINKEDIN_TOKEN)")
	}
	var bodyReader io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, liBase+path, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Restli-Protocol-Version", "2.0.0")
	req.Header.Set("LinkedIn-Version", "202504")
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var parsed any
	_ = json.Unmarshal(raw, &parsed)
	// LinkedIn returns 201 for success on POST
	ok := resp.StatusCode >= 200 && resp.StatusCode < 300
	postID := resp.Header.Get("x-restli-id")
	out := map[string]any{"status": resp.StatusCode, "ok": ok, "body": parsed}
	if postID != "" {
		out["post_id"] = postID
	}
	return out, nil
}

// ── Render client ─────────────────────────────────────────────────────────────

func renderDo(method, path, apiKey string, body any) (map[string]any, error) {
	if apiKey == "" {
		apiKey = os.Getenv("RENDER_API_KEY")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("render_api_key required (or set RENDER_API_KEY)")
	}
	var bodyReader io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, renderBase+path, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var parsed any
	_ = json.Unmarshal(raw, &parsed)
	return map[string]any{"status": resp.StatusCode, "ok": resp.StatusCode >= 200 && resp.StatusCode < 300, "body": parsed}, nil
}

// ── Vercel API client ─────────────────────────────────────────────────────────

func vercelDo(method, path, token, teamID string, body any) (map[string]any, error) {
	if token == "" {
		token = os.Getenv("VERCEL_TOKEN")
	}
	if token == "" {
		return nil, fmt.Errorf("vercel_token required (or set VERCEL_TOKEN)")
	}
	u := "https://api.vercel.com" + path
	if teamID != "" {
		if strings.Contains(u, "?") {
			u += "&teamId=" + teamID
		} else {
			u += "?teamId=" + teamID
		}
	}
	var bodyReader io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, u, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var parsed any
	_ = json.Unmarshal(raw, &parsed)
	return map[string]any{"status": resp.StatusCode, "ok": resp.StatusCode >= 200 && resp.StatusCode < 300, "body": parsed}, nil
}

// ── MCP dispatch ──────────────────────────────────────────────────────────────

func handleMCP(tools []Tool, req MCPRequest) MCPResponse {
	index := make(map[string]Tool, len(tools))
	for _, t := range tools {
		index[t.Def.Name] = t
	}

	switch req.Method {
	case "initialize":
		return MCPResponse{
			JSONRPC: "2.0", ID: req.ID,
			Result: map[string]any{
				"protocolVersion": "2024-11-05",
				"serverInfo":      map[string]any{"name": serverName, "version": serverVer},
				"capabilities":    map[string]any{"tools": map[string]any{}},
			},
		}

	case "tools/list":
		defs := make([]ToolDef, 0, len(tools))
		for _, t := range tools {
			defs = append(defs, t.Def)
		}
		return MCPResponse{JSONRPC: "2.0", ID: req.ID, Result: map[string]any{"tools": defs}}

	case "tools/call":
		var p struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return MCPResponse{JSONRPC: "2.0", ID: req.ID, Error: &MCPError{Code: -32602, Message: "invalid params"}}
		}
		t, ok := index[p.Name]
		if !ok {
			return MCPResponse{JSONRPC: "2.0", ID: req.ID, Error: &MCPError{Code: -32601, Message: "unknown tool: " + p.Name}}
		}
		result, err := t.Handler(p.Arguments)
		if err != nil {
			return MCPResponse{JSONRPC: "2.0", ID: req.ID, Error: &MCPError{Code: -32603, Message: err.Error()}}
		}
		return MCPResponse{JSONRPC: "2.0", ID: req.ID, Result: result}

	case "notifications/initialized":
		return MCPResponse{}

	default:
		return MCPResponse{JSONRPC: "2.0", ID: req.ID, Error: &MCPError{Code: -32601, Message: "unknown method: " + req.Method}}
	}
}

// ── Vercel serverless entry point ─────────────────────────────────────────────

var mcpTools = buildTools()

func Handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"POST only"}`, http.StatusMethodNotAllowed)
		return
	}

	var req MCPRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(MCPResponse{JSONRPC: "2.0", Error: &MCPError{Code: -32700, Message: "parse error"}})
		return
	}

	resp := handleMCP(mcpTools, req)
	if resp.JSONRPC == "" {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
