package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// ── constants ─────────────────────────────────────────────────────────────────

const (
	calProxy   = "https://cal-proxy-phi.vercel.app/api/cal"
	serverName = "api-proxy"
	serverVer  = "3.0.0"
)

var eventTypes = map[int]int{15: 4513949, 30: 4513947}

// ── MCP wire types ────────────────────────────────────────────────────────────

type MCPRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type MCPResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id,omitempty"`
	Result  any    `json:"result,omitempty"`
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

// ── helpers ───────────────────────────────────────────────────────────────────

func textResult(v any) ToolResult {
	b, _ := json.MarshalIndent(v, "", "  ")
	return ToolResult{Content: []TextContent{{Type: "text", Text: string(b)}}}
}

func errResult(msg string) ToolResult {
	return textResult(map[string]any{"ok": false, "error": msg})
}

func calSecret() string { return "cal-proxy-sk-" }

func calRequest(path, method string, body any, version string) (map[string]any, error) {
	if version == "" {
		version = "2024-08-13"
	}
	reqURL := fmt.Sprintf("%s?path=%s&version=%s", calProxy, url.QueryEscape(path), version)

	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, reqURL, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-proxy-secret", calSecret())
	req.Header.Set("User-Agent", "Zapier")
	req.Header.Set("Content-Type", "application/json")

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
		default: // json
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
	return map[string]any{"status": resp.StatusCode, "ok": resp.StatusCode >= 200 && resp.StatusCode < 300, "body": parsed}, nil
}

// ── tool registry ─────────────────────────────────────────────────────────────

type HandlerFunc func(params map[string]any) (ToolResult, error)

type Tool struct {
	Def     ToolDef
	Handler HandlerFunc
}

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

func buildTools() []Tool {
	bodySchema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"url":       map[string]any{"type": "string", "format": "uri"},
			"body":      map[string]any{"type": "object"},
			"body_type": map[string]any{"type": "string", "enum": []string{"json", "form", "raw"}},
			"headers":   map[string]any{"type": "object"},
		},
		"required": []string{"url"},
	}

	tools := []Tool{
		// ── HTTP ──────────────────────────────────────────────────────────────
		{
			Def: ToolDef{
				Name:        "http_get",
				Description: "GET request to any URL",
				InputSchema: map[string]any{
					"type":       "object",
					"properties": map[string]any{"url": map[string]any{"type": "string"}, "headers": map[string]any{"type": "object"}},
					"required":   []string{"url"},
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
			Def: ToolDef{Name: "http_post", Description: "POST request — json, form, or raw body", InputSchema: bodySchema},
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
			Def: ToolDef{Name: "http_put", Description: "PUT request — json, form, or raw body", InputSchema: bodySchema},
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
			Def: ToolDef{Name: "http_patch", Description: "PATCH request — json, form, or raw body", InputSchema: bodySchema},
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
				Name:        "http_delete",
				Description: "DELETE request",
				InputSchema: map[string]any{
					"type":       "object",
					"properties": map[string]any{"url": map[string]any{"type": "string"}, "headers": map[string]any{"type": "object"}},
					"required":   []string{"url"},
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
				Name:        "http_head",
				Description: "HEAD request — status and headers only",
				InputSchema: map[string]any{
					"type":       "object",
					"properties": map[string]any{"url": map[string]any{"type": "string"}, "headers": map[string]any{"type": "object"}},
					"required":   []string{"url"},
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

		// ── CAL ───────────────────────────────────────────────────────────────
		{
			Def: ToolDef{
				Name:        "cal_get_slots",
				Description: "Get available booking slots for a date range",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"start_date":    map[string]any{"type": "string"},
						"end_date":      map[string]any{"type": "string"},
						"event_type_id": map[string]any{"type": "number", "default": 4513949},
					},
					"required": []string{"start_date", "end_date"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				etid := int(num(p, "event_type_id", 4513949))
				path := fmt.Sprintf("/slots?start=%s&end=%s&eventTypeId=%d", str(p, "start_date", ""), str(p, "end_date", ""), etid)
				res, err := calRequest(path, "GET", nil, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal_list_bookings",
				Description: "List all bookings, optionally filtered by status",
				InputSchema: map[string]any{
					"type":       "object",
					"properties": map[string]any{"status": map[string]any{"type": "string", "enum": []string{"upcoming", "past", "cancelled"}}},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				path := "/bookings"
				if s := str(p, "status", ""); s != "" {
					path += "?status=" + s
				}
				res, err := calRequest(path, "GET", nil, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal_get_booking",
				Description: "Get a single booking by UID",
				InputSchema: map[string]any{
					"type":       "object",
					"properties": map[string]any{"uid": map[string]any{"type": "string"}},
					"required":   []string{"uid"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := calRequest("/bookings/"+str(p, "uid", ""), "GET", nil, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal_create_booking",
				Description: "Create a booking with full control",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"event_type_id":     map[string]any{"type": "number", "default": 4513949},
						"start":             map[string]any{"type": "string", "description": "ISO 8601 e.g. 2026-03-25T12:00:00Z"},
						"attendee_name":     map[string]any{"type": "string"},
						"attendee_email":    map[string]any{"type": "string", "format": "email"},
						"attendee_timezone": map[string]any{"type": "string", "default": "Africa/Cairo"},
						"guests":            map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					},
					"required": []string{"start", "attendee_name", "attendee_email"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{
					"eventTypeId": int(num(p, "event_type_id", 4513949)),
					"start":       str(p, "start", ""),
					"attendee": map[string]any{
						"name":     str(p, "attendee_name", ""),
						"email":    str(p, "attendee_email", ""),
						"timeZone": str(p, "attendee_timezone", "Africa/Cairo"),
					},
				}
				if guests := strSlice(p, "guests"); len(guests) > 0 {
					body["guests"] = guests
				}
				res, err := calRequest("/bookings", "POST", body, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal_smart_book",
				Description: "One-shot booking: finds first available slot on a date, books it, and auto-confirms.",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"email":            map[string]any{"type": "string", "format": "email"},
						"name":             map[string]any{"type": "string"},
						"date":             map[string]any{"type": "string", "description": "e.g. 2026-03-26"},
						"timezone":         map[string]any{"type": "string", "default": "Africa/Cairo"},
						"duration_minutes": map[string]any{"type": "number", "default": 15},
						"guests":           map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					},
					"required": []string{"email", "name", "date"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				dur := int(num(p, "duration_minutes", 15))
				etid := eventTypes[15]
				if dur == 30 {
					etid = eventTypes[30]
				}
				date := str(p, "date", "")

				slotsRes, err := calRequest(fmt.Sprintf("/slots?start=%s&end=%s&eventTypeId=%d", date, date, etid), "GET", nil, "")
				if err != nil {
					return errResult(err.Error()), nil
				}

				// navigate body.data.slots
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
					return textResult(map[string]any{"ok": false, "error": "No available slots on this date", "slots": slotsRes["body"]}), nil
				}

				bookBody := map[string]any{
					"eventTypeId": etid,
					"start":       firstSlot,
					"attendee": map[string]any{
						"name":     str(p, "name", ""),
						"email":    str(p, "email", ""),
						"timeZone": str(p, "timezone", "Africa/Cairo"),
					},
				}
				if guests := strSlice(p, "guests"); len(guests) > 0 {
					bookBody["guests"] = guests
				}

				booking, err := calRequest("/bookings", "POST", bookBody, "")
				if err != nil {
					return errResult(err.Error()), nil
				}

				// extract uid from body.data.uid or body.uid
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
					return textResult(map[string]any{"ok": false, "error": "Booking failed", "booking": booking}), nil
				}

				confirm, err := calRequest("/bookings/"+uid+"/confirm", "POST", map[string]any{"confirmed": true}, "")
				if err != nil {
					return errResult(err.Error()), nil
				}

				return textResult(map[string]any{
					"ok":           true,
					"uid":          uid,
					"slot":         firstSlot,
					"attendee":     map[string]any{"name": str(p, "name", ""), "email": str(p, "email", "")},
					"guests":       strSlice(p, "guests"),
					"booking":      booking["body"],
					"confirmation": confirm["body"],
				}), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal_confirm_booking",
				Description: "Confirm (accept) a pending booking",
				InputSchema: map[string]any{
					"type":       "object",
					"properties": map[string]any{"uid": map[string]any{"type": "string"}},
					"required":   []string{"uid"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := calRequest("/bookings/"+str(p, "uid", "")+"/confirm", "POST", map[string]any{"confirmed": true}, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal_reject_booking",
				Description: "Reject a pending booking",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"uid":    map[string]any{"type": "string"},
						"reason": map[string]any{"type": "string"},
					},
					"required": []string{"uid"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{"confirmed": false}
				if r := str(p, "reason", ""); r != "" {
					body["reason"] = r
				}
				res, err := calRequest("/bookings/"+str(p, "uid", "")+"/confirm", "POST", body, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal_cancel_booking",
				Description: "Cancel a booking",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"uid":    map[string]any{"type": "string"},
						"reason": map[string]any{"type": "string"},
					},
					"required": []string{"uid"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{}
				if r := str(p, "reason", ""); r != "" {
					body["reason"] = r
				}
				res, err := calRequest("/bookings/"+str(p, "uid", "")+"/cancel", "POST", body, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal_reschedule_booking",
				Description: "Reschedule a booking to a new time",
				InputSchema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"uid":       map[string]any{"type": "string"},
						"new_start": map[string]any{"type": "string"},
						"reason":    map[string]any{"type": "string"},
					},
					"required": []string{"uid", "new_start"},
				},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				body := map[string]any{"start": str(p, "new_start", "")}
				if r := str(p, "reason", ""); r != "" {
					body["reason"] = r
				}
				res, err := calRequest("/bookings/"+str(p, "uid", "")+"/reschedule", "POST", body, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
		{
			Def: ToolDef{
				Name:        "cal_list_event_types",
				Description: "List all event types",
				InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
			},
			Handler: func(p map[string]any) (ToolResult, error) {
				res, err := calRequest("/event-types", "GET", nil, "")
				if err != nil {
					return errResult(err.Error()), nil
				}
				return textResult(res), nil
			},
		},
	}
	return tools
}

// ── MCP dispatch ──────────────────────────────────────────────────────────────

func handleMCP(tools []Tool, req MCPRequest) MCPResponse {
	toolIndex := map[string]Tool{}
	for _, t := range tools {
		toolIndex[t.Def.Name] = t
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
		t, ok := toolIndex[p.Name]
		if !ok {
			return MCPResponse{JSONRPC: "2.0", ID: req.ID, Error: &MCPError{Code: -32601, Message: "tool not found: " + p.Name}}
		}
		result, err := t.Handler(p.Arguments)
		if err != nil {
			return MCPResponse{JSONRPC: "2.0", ID: req.ID, Error: &MCPError{Code: -32603, Message: err.Error()}}
		}
		return MCPResponse{JSONRPC: "2.0", ID: req.ID, Result: result}

	case "notifications/initialized":
		return MCPResponse{} // no response for notifications

	default:
		return MCPResponse{JSONRPC: "2.0", ID: req.ID, Error: &MCPError{Code: -32601, Message: "method not found: " + req.Method}}
	}
}

// ── HTTP handler ──────────────────────────────────────────────────────────────

var mcpTools = buildTools()

// Handler is the Vercel serverless entry point.
func Handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"only POST allowed"}`, http.StatusMethodNotAllowed)
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
