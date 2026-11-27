package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"agy-acp-gateway-go/internal/protocol"
)

func geminiStatusFromHTTP(code int) string {
	switch code {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnsupportedMediaType, http.StatusUnprocessableEntity:
		return "INVALID_ARGUMENT"
	case http.StatusUnauthorized:
		return "UNAUTHENTICATED"
	case http.StatusForbidden:
		return "PERMISSION_DENIED"
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusConflict:
		return "ABORTED"
	case http.StatusTooManyRequests, 529:
		return "RESOURCE_EXHAUSTED"
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return "UNAVAILABLE"
	default:
		return "INTERNAL"
	}
}

func buildGeminiErrorPayload(code int, msg string) map[string]any {
	httpCode := code
	if httpCode == 529 {
		httpCode = http.StatusTooManyRequests
	}
	return map[string]any{
		"error": map[string]any{
			"code":    httpCode,
			"message": msg,
			"status":  geminiStatusFromHTTP(code),
		},
	}
}

func writeGeminiError(w http.ResponseWriter, code int, msg string) {
	httpCode := code
	if httpCode == 529 {
		httpCode = http.StatusTooManyRequests
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(httpCode)
	_ = json.NewEncoder(w).Encode(buildGeminiErrorPayload(code, msg))
}

func classifyGeminiUpstreamError(err error) (httpCode int, retryAfter bool, msg string) {
	rawCode, _, retry, errMsg := classifyUpstreamError(err)
	if rawCode == 529 {
		rawCode = http.StatusTooManyRequests
	}
	return rawCode, retry, errMsg
}

func (gs *GatewayServer) handleGeminiV1Beta(w http.ResponseWriter, r *http.Request, reqID int64) {
	cleanPath, _ := extractGeminiPathKey(r.URL.Path)
	if cleanPath == "/v1beta/models/" {
		cleanPath = "/v1beta/models"
	}

	if r.Method == http.MethodGet && cleanPath == "/v1beta/models" {
		gs.handleGeminiListModels(w, r)
		return
	}

	if r.Method == http.MethodGet && strings.HasPrefix(cleanPath, "/v1beta/models/") {
		rest := strings.TrimPrefix(cleanPath, "/v1beta/models/")
		rest = strings.TrimPrefix(rest, "models/")
		rest = strings.TrimSpace(rest)
		if rest == "" || strings.ContainsAny(rest, ":/") {
			writeGeminiError(w, http.StatusNotFound, "Model resource not found")
			return
		}
		if isImageGenCanonicalModel(rest) {
			normID := strings.ToLower(rest)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			_ = json.NewEncoder(w).Encode(protocol.BuildGeminiImageModelResource(normID))
			return
		}
		if exact := GetModel(rest); exact != nil {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			_ = json.NewEncoder(w).Encode(BuildGeminiModelResource(exact.ID, exact.Label, exact))
			return
		}
		sel := ResolveRequestedModel(rest, nil)
		if sel.Explicit && sel.Model != nil {
			normID := strings.ToLower(rest)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			_ = json.NewEncoder(w).Encode(BuildGeminiModelResource(normID, sel.Model.Label, sel.Model))
			return
		}
		writeGeminiError(w, http.StatusNotFound, fmt.Sprintf("models/%s is not found for API version v1beta", rest))
		return
	}

	if r.Method == http.MethodPost && strings.HasPrefix(cleanPath, "/v1beta/models/") {
		rest := strings.TrimPrefix(cleanPath, "/v1beta/models/")
		colon := strings.LastIndexByte(rest, ':')
		if colon <= 0 {
			writeGeminiError(w, http.StatusNotFound, "Missing action suffix on /v1beta/models/{model}:{action}")
			return
		}
		rawModel := strings.TrimPrefix(strings.TrimSpace(rest[:colon]), "models/")
		action := strings.TrimSpace(rest[colon+1:])
		if rawModel == "" || strings.ContainsAny(rawModel, ":/") {
			writeGeminiError(w, http.StatusBadRequest, "Invalid model identifier in path")
			return
		}
		switch action {
		case "countTokens":
			gs.handleGeminiCountTokens(w, r, rawModel)
		case "generateContent":
			gs.handleGeminiGenerateContent(w, r, reqID, rawModel, false)
		case "streamGenerateContent":
			gs.handleGeminiGenerateContent(w, r, reqID, rawModel, true)
		default:
			writeGeminiError(w, http.StatusNotFound, "Unsupported Gemini method: "+action)
		}
		return
	}

	writeGeminiError(w, http.StatusNotFound, "Not found")
}

func (gs *GatewayServer) handleGeminiListModels(w http.ResponseWriter, r *http.Request) {
	models, boundAccID := GlobalAccountManager.FilterGeminiModelsForRequest(r, ListGeminiModelsForAPI())
	if boundAccID != "" {
		w.Header().Set("X-Agy-Account", boundAccID)
	}
	offset := 0
	if pt := strings.TrimSpace(r.URL.Query().Get("pageToken")); pt != "" {
		n, err := strconv.Atoi(pt)
		if err != nil || n < 0 {
			writeGeminiError(w, http.StatusBadRequest, "Invalid pageToken")
			return
		}
		offset = n
	}
	if offset > len(models) {
		offset = len(models)
	}
	page := models[offset:]
	nextToken := ""
	if psStr := strings.TrimSpace(r.URL.Query().Get("pageSize")); psStr != "" {
		ps, err := strconv.Atoi(psStr)
		if err != nil || ps <= 0 {
			writeGeminiError(w, http.StatusBadRequest, "Invalid pageSize")
			return
		}
		if ps < len(page) {
			page = page[:ps]
			nextToken = strconv.Itoa(offset + ps)
		}
	}
	resp := map[string]any{"models": page}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(resp)
}

func readGeminiJSONBody(w http.ResponseWriter, r *http.Request) (map[string]any, bool) {
	var rawBody []byte
	var err error
	if preRead, ok := preReadBodyFromContext(r); ok {
		rawBody = preRead
	} else {
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
		rawBody, err = io.ReadAll(r.Body)
	}
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeGeminiError(w, http.StatusRequestEntityTooLarge, "Request body exceeds maximum size")
		} else {
			writeGeminiError(w, http.StatusBadRequest, "Failed to read request body")
		}
		return nil, false
	}
	if len(rawBody) == 0 {
		writeGeminiError(w, http.StatusBadRequest, "Empty JSON request body")
		return nil, false
	}
	var body map[string]any
	if err := unmarshalJSONPreserveNumbers(rawBody, &body); err != nil || body == nil {
		clearPreReadBody(r)
		writeGeminiError(w, http.StatusBadRequest, "Malformed JSON request body: "+fmt.Sprint(err))
		return nil, false
	}
	clearPreReadBody(r)
	rawBody = nil
	return body, true
}

func writeGeminiValidationError(w http.ResponseWriter, err error) {
	code := http.StatusBadRequest
	var mmErr *MultimodalValidationError
	if errors.As(err, &mmErr) && mmErr != nil && mmErr.StatusCode > 0 {
		code = mmErr.StatusCode
	}
	writeGeminiError(w, code, err.Error())
}

func (gs *GatewayServer) handleGeminiCountTokens(w http.ResponseWriter, r *http.Request, pathModel string) {
	rawBody, ok := readGeminiJSONBody(w, r)
	if !ok {
		return
	}
	effective := rawBody
	if gcr := mapField(rawBody, "generateContentRequest", "generate_content_request"); gcr != nil {
		merged := make(map[string]any, len(rawBody)+len(gcr))
		for k, v := range rawBody {
			merged[k] = v
		}
		for k, v := range gcr {
			merged[k] = v
		}
		effective = merged
	}
	modelStr := strings.TrimSpace(pathModel)
	if modelStr == "" {
		if m, okM := effective["model"].(string); okM && strings.TrimSpace(m) != "" {
			modelStr = m
		}
	}
	norm, err := normalizeGeminiV1BetaRequest(modelStr, effective, true)
	if err != nil {
		writeGeminiValidationError(w, err)
		return
	}

	charCount := len(normalizeSystemText(norm.Body["system"]))
	for _, m := range norm.Messages {
		charCount += len(formatSingleMessageText(m))
	}
	if rawTools, okT := norm.Body["tools"].([]any); okT && len(rawTools) > 0 {
		if b, errM := json.Marshal(rawTools); errM == nil {
			charCount += len(b)
		}
	}
	if soInst := extractStructuredOutputInstruction(norm.Body); soInst != "" {
		charCount += len(soInst)
	}
	imgCount := norm.TotalImages
	sel := ResolveRequestedModel(modelStr, effective)
	if maxImgs := effectiveModelMaxImages(sel.Model); maxImgs > 0 && imgCount > maxImgs {
		imgCount = maxImgs
	}
	textTokens := int(math.Ceil(float64(charCount) / 4.0))
	imgTokens := imgCount * 1600
	mediaCnt, _ := countMediaRelayBlocks(norm.Messages)
	mediaTokens := mediaCnt * 1600
	total := textTokens + imgTokens + mediaTokens
	if total < 1 {
		total = 1
		textTokens = 1
	}

	var details []map[string]any
	if textTokens > 0 {
		details = append(details, map[string]any{"modality": "TEXT", "tokenCount": textTokens})
	}
	if imgTokens > 0 {
		details = append(details, map[string]any{"modality": "IMAGE", "tokenCount": imgTokens})
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"totalTokens":         total,
		"promptTokensDetails": details,
	})
}

type syntheticToolBinding struct {
	SyntheticID string
	Name        string
}

type normalizedGeminiRequest struct {
	Body              map[string]any
	Messages          []map[string]any
	DisplayModel      string
	IncludeThoughts   bool
	SyntheticBindings []syntheticToolBinding
	TotalImages       int
}

func mapField(m map[string]any, camel, snake string) map[string]any {
	if m == nil {
		return nil
	}
	if v, ok := m[camel].(map[string]any); ok {
		return v
	}
	if snake != "" {
		if v, ok := m[snake].(map[string]any); ok {
			return v
		}
	}
	return nil
}

func sliceField(m map[string]any, camel, snake string) []any {
	if m == nil {
		return nil
	}
	if v, ok := m[camel].([]any); ok {
		return v
	}
	if snake != "" {
		if v, ok := m[snake].([]any); ok {
			return v
		}
	}
	return nil
}

func stringField(m map[string]any, camel, snake string) string {
	if m == nil {
		return ""
	}
	if v, ok := m[camel].(string); ok && v != "" {
		return v
	}
	if snake != "" {
		if v, ok := m[snake].(string); ok {
			return v
		}
	}
	return ""
}

func extractGeminiTextParts(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case map[string]any:
		if parts, ok := x["parts"].([]any); ok {
			return extractGeminiTextParts(parts)
		}
		if t, ok := x["text"].(string); ok {
			return t
		}
		return ""
	case []any:
		var out []string
		for _, item := range x {
			switch p := item.(type) {
			case string:
				if p != "" {
					out = append(out, p)
				}
			case map[string]any:
				if t, ok := p["text"].(string); ok && t != "" {
					out = append(out, t)
				}
			}
		}
		return strings.Join(out, "\n")
	default:
		return ""
	}
}

func normalizeGeminiSchemaType(raw string) string {
	low := strings.ToLower(strings.TrimSpace(raw))
	switch low {
	case "object", "string", "integer", "number", "boolean", "array", "null":
		return low
	case "type_unspecified", "":
		return ""
	default:
		return low
	}
}

func normalizeGeminiSchema(node any) any {
	switch v := node.(type) {
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = normalizeGeminiSchema(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		nullable, _ := v["nullable"].(bool)
		for k, val := range v {
			switch k {
			case "nullable", "propertyOrdering", "property_ordering", "example":
				continue
			case "type":
				switch tv := val.(type) {
				case string:
					if norm := normalizeGeminiSchemaType(tv); norm != "" {
						out["type"] = norm
					}
				case []any:
					var normArr []any
					for _, elem := range tv {
						if es, okS := elem.(string); okS {
							if norm := normalizeGeminiSchemaType(es); norm != "" {
								normArr = append(normArr, norm)
							}
						}
					}
					if len(normArr) > 0 {
						out["type"] = normArr
					}
				}
			default:
				out[k] = normalizeGeminiSchema(val)
			}
		}
		if out["type"] == nil {
			if _, hasProps := out["properties"]; hasProps {
				out["type"] = "object"
			} else if _, hasItems := out["items"]; hasItems {
				out["type"] = "array"
			} else if nullable {
				out["type"] = "string"
			}
		}
		return out
	default:
		return node
	}
}

func extractGeminiToolChoice(geminiBody map[string]any) any {
	tc := mapField(geminiBody, "toolConfig", "tool_config")
	if tc == nil {
		return nil
	}
	fcc := mapField(tc, "functionCallingConfig", "function_calling_config")
	if fcc == nil {
		return nil
	}
	mode := strings.ToUpper(strings.TrimSpace(stringField(fcc, "mode", "")))
	if mode == "NONE" {
		return "none"
	}
	_, hasCamelAllowed := fcc["allowedFunctionNames"]
	_, hasSnakeAllowed := fcc["allowed_function_names"]
	explicitAllowedList := hasCamelAllowed || hasSnakeAllowed
	var allowed []any
	for _, a := range sliceField(fcc, "allowedFunctionNames", "allowed_function_names") {
		if s, ok := a.(string); ok {
			if trimmed := strings.TrimSpace(s); trimmed != "" {
				allowed = append(allowed, trimmed)
			}
		}
	}
	if len(allowed) > 0 {
		outMode := "auto"
		if mode == "ANY" || mode == "VALIDATED" {
			outMode = "required"
		}
		return map[string]any{
			"type":  "allowed_tools",
			"mode":  outMode,
			"tools": allowed,
		}
	}
	if explicitAllowedList {
		// Explicit allowedFunctionNames: [] (or only blank names) fails closed to "none".
		return "none"
	}
	if mode == "ANY" || mode == "VALIDATED" {
		return "required"
	}
	return nil
}

func normalizeGeminiTools(geminiBody map[string]any) []any {
	rawTools, ok := geminiBody["tools"].([]any)
	if !ok || len(rawTools) == 0 {
		return nil
	}
	var out []any
	for _, rt := range rawTools {
		tm, okM := rt.(map[string]any)
		if !okM {
			continue
		}
		decls := sliceField(tm, "functionDeclarations", "function_declarations")
		if len(decls) == 0 {
			if _, hasName := tm["name"].(string); hasName {
				decls = []any{tm}
			}
		}
		for _, rd := range decls {
			fd, okF := rd.(map[string]any)
			if !okF {
				continue
			}
			name := strings.TrimSpace(stringField(fd, "name", ""))
			if name == "" {
				continue
			}
			desc := stringField(fd, "description", "")
			rawSchema := fd["parameters"]
			if rawSchema == nil {
				rawSchema = fd["parametersJsonSchema"]
			}
			if rawSchema == nil {
				rawSchema = fd["parameters_json_schema"]
			}
			if rawSchema == nil {
				rawSchema = fd["input_schema"]
			}
			if rawSchema == nil {
				rawSchema = map[string]any{"type": "object", "properties": map[string]any{}}
			}
			out = append(out, map[string]any{
				"name":         name,
				"description":  desc,
				"input_schema": normalizeGeminiSchema(rawSchema),
			})
		}
	}
	return out
}

func formatGeminiFunctionResponse(fr map[string]any, fieldPath string, mctx *mediaNormalizeContext) (any, bool, error) {
	isErr := false
	var textStr string
	respVal, hasResp := fr["response"]
	if !hasResp {
		textStr = ""
	} else {
		switch rv := respVal.(type) {
		case string:
			textStr = rv
		case map[string]any:
			if len(rv) == 1 {
				if errVal, hasErr := rv["error"]; hasErr {
					isErr = true
					if s, okS := errVal.(string); okS {
						textStr = s
					} else if b, errM := json.Marshal(errVal); errM == nil {
						textStr = string(b)
					}
				} else {
					for _, key := range []string{"output", "result", "content"} {
						if v, okK := rv[key]; okK {
							if s, okS := v.(string); okS {
								textStr = s
							}
						}
					}
				}
			} else if errVal, hasErr := rv["error"]; hasErr && errVal != nil && errVal != "" && errVal != false {
				isErr = true
			}
			if textStr == "" {
				if b, errM := json.Marshal(rv); errM == nil {
					textStr = string(b)
				}
			}
		default:
			if b, errM := json.Marshal(rv); errM == nil {
				textStr = string(b)
			}
		}
	}

	var nestedBlocks []any
	if parts, okP := fr["parts"].([]any); okP {
		for pi, rp := range parts {
			pm, okM := rp.(map[string]any)
			if !okM {
				continue
			}
			if inl := mapField(pm, "inlineData", "inline_data"); inl != nil {
				blk, err := normalizeGeminiInlineAttachmentWithCtx(inl, fmt.Sprintf("%s.parts[%d].inlineData", fieldPath, pi), false, mctx, true)
				if err != nil {
					return nil, isErr, err
				}
				nestedBlocks = append(nestedBlocks, blk)
			} else if fd := mapField(pm, "fileData", "file_data"); fd != nil {
				blk, err := normalizeGeminiFileAttachmentWithCtx(fd, fmt.Sprintf("%s.parts[%d].fileData", fieldPath, pi), mctx, true)
				if err != nil {
					return nil, isErr, err
				}
				nestedBlocks = append(nestedBlocks, blk)
			} else if t, okT := pm["text"].(string); okT && t != "" {
				if textStr != "" {
					textStr += "\n" + t
				} else {
					textStr = t
				}
			}
		}
	}
	if len(nestedBlocks) > 0 {
		blocks := make([]any, 0, len(nestedBlocks)+1)
		if textStr != "" {
			blocks = append(blocks, map[string]any{"type": "text", "text": textStr})
		}
		blocks = append(blocks, nestedBlocks...)
		return blocks, isErr, nil
	}
	return textStr, isErr, nil
}

func extractIncludeThoughts(geminiBody map[string]any) bool {
	for _, gcKey := range []string{"generationConfig", "generation_config"} {
		if gc, ok := geminiBody[gcKey].(map[string]any); ok {
			if tc := mapField(gc, "thinkingConfig", "thinking_config"); tc != nil {
				if v, okB := tc["includeThoughts"].(bool); okB {
					return v
				}
				if v, okB := tc["include_thoughts"].(bool); okB {
					return v
				}
			}
		}
	}
	if tc := mapField(geminiBody, "thinkingConfig", "thinking_config"); tc != nil {
		if v, okB := tc["includeThoughts"].(bool); okB {
			return v
		}
		if v, okB := tc["include_thoughts"].(bool); okB {
			return v
		}
	}
	return false
}

func normalizeGeminiV1BetaRequest(pathModel string, geminiBody map[string]any, allowEmptyContents bool) (*normalizedGeminiRequest, error) {
	outBody := make(map[string]any)
	reqModel := strings.TrimSpace(pathModel)
	if reqModel == "" {
		reqModel = strings.TrimSpace(stringField(geminiBody, "model", ""))
	}
	reqModel = strings.TrimPrefix(reqModel, "models/")
	outBody["model"] = reqModel

	var sysParts []string
	if sysInst, exists := geminiBody["systemInstruction"]; exists && sysInst != nil {
		s, err := extractSystemTextStrict(sysInst, "systemInstruction")
		if err != nil {
			return nil, err
		}
		if s = strings.TrimSpace(s); s != "" {
			sysParts = append(sysParts, s)
		}
	} else if sysInst, exists := geminiBody["system_instruction"]; exists && sysInst != nil {
		s, err := extractSystemTextStrict(sysInst, "system_instruction")
		if err != nil {
			return nil, err
		}
		if s = strings.TrimSpace(s); s != "" {
			sysParts = append(sysParts, s)
		}
	}

	if gc := mapField(geminiBody, "generationConfig", "generation_config"); gc != nil {
		outBody["generationConfig"] = gc
		if maxTok, ok := toInt(gc["maxOutputTokens"]); ok && maxTok > 0 {
			outBody["max_tokens"] = maxTok
		} else if maxTok, ok := toInt(gc["max_output_tokens"]); ok && maxTok > 0 {
			outBody["max_tokens"] = maxTok
		}
		respMime := strings.ToLower(strings.TrimSpace(stringField(gc, "responseMimeType", "response_mime_type")))
		if respMime == "application/json" {
			schema := gc["responseSchema"]
			if schema == nil {
				schema = gc["response_schema"]
			}
			if schema == nil {
				schema = gc["responseJsonSchema"]
			}
			if schema == nil {
				schema = gc["response_json_schema"]
			}
			if schema != nil {
				outBody["response_format"] = map[string]any{
					"type":   "json_schema",
					"schema": normalizeGeminiSchema(schema),
				}
			} else {
				outBody["response_format"] = map[string]any{
					"type": "json_object",
				}
			}
		}
	}
	if tc := mapField(geminiBody, "thinkingConfig", "thinking_config"); tc != nil {
		outBody["thinkingConfig"] = tc
	}

	meta := make(map[string]any)
	if existingMeta, ok := geminiBody["metadata"].(map[string]any); ok {
		for k, v := range existingMeta {
			meta[k] = v
		}
	}
	if labels, ok := geminiBody["labels"].(map[string]any); ok {
		for _, k := range []string{"session_id", "conversation_id", "user_id", "agent_id"} {
			if _, exists := meta[k]; !exists {
				if v, okS := labels[k].(string); okS && strings.TrimSpace(v) != "" {
					meta[k] = strings.TrimSpace(v)
				}
			}
		}
	}
	if len(meta) > 0 {
		outBody["metadata"] = meta
	}

	if normTools := normalizeGeminiTools(geminiBody); len(normTools) > 0 {
		outBody["tools"] = normTools
	}
	if tc := extractGeminiToolChoice(geminiBody); tc != nil {
		outBody["tool_choice"] = tc
	}
	var rawContents []any
	switch cv := geminiBody["contents"].(type) {
	case []any:
		rawContents = cv
	case map[string]any:
		rawContents = []any{cv}
	case string:
		if strings.TrimSpace(cv) != "" {
			rawContents = []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": cv}}}}
		}
	}

	mediaCtx := newMediaNormalizeContext(outBody, nil)
	if !mediaCtx.hasClientTools {
		for _, rc := range rawContents {
			cm, ok := rc.(map[string]any)
			if !ok {
				continue
			}
			for _, rp := range sliceField(cm, "parts", "") {
				pm, okP := rp.(map[string]any)
				if !okP {
					continue
				}
				if pm["functionCall"] != nil || pm["function_call"] != nil || pm["functionResponse"] != nil || pm["function_response"] != nil {
					mediaCtx.hasClientTools = true
					break
				}
			}
			if mediaCtx.hasClientTools {
				break
			}
		}
	}

	type pendingCall struct {
		id        string
		name      string
		synthetic bool
	}
	var pendingFIFO []pendingCall
	var syntheticBindings []syntheticToolBinding
	fifoSeq := 0

	var messages []map[string]any
	appendMessageBlocks := func(role string, blocks []any) {
		if len(blocks) == 0 {
			return
		}
		if len(messages) > 0 {
			last := messages[len(messages)-1]
			if lastRole, _ := last["role"].(string); lastRole == role {
				if prevArr, ok := last["content"].([]any); ok {
					last["content"] = append(prevArr, blocks...)
					return
				}
			} else if role == "assistant" {
				pendingFIFO = nil
			}
		} else if role == "assistant" {
			pendingFIFO = nil
		}
		messages = append(messages, map[string]any{
			"role":    role,
			"content": blocks,
		})
	}

	for ci, rc := range rawContents {
		switch item := rc.(type) {
		case string:
			if item != "" {
				appendMessageBlocks("user", []any{map[string]any{"type": "text", "text": item}})
			}
		case map[string]any:
			rawRole := strings.ToLower(strings.TrimSpace(stringField(item, "role", "")))
			if (rawRole == "system" || rawRole == "developer") && len(messages) == 0 {
				s, err := extractSystemTextStrict(item, fmt.Sprintf("contents[%d]", ci))
				if err != nil {
					return nil, err
				}
				if s = strings.TrimSpace(s); s != "" {
					sysParts = append(sysParts, s)
				}
				continue
			}

			var parts []any
			if pArr, ok := item["parts"].([]any); ok {
				parts = pArr
			} else if pMap, ok := item["parts"].(map[string]any); ok {
				parts = []any{pMap}
			} else if t, ok := item["text"].(string); ok {
				parts = []any{map[string]any{"text": t}}
			}

			defaultRole := "user"
			if rawRole == "model" || rawRole == "assistant" {
				defaultRole = "assistant"
			} else if rawRole == "" {
				for _, rp := range parts {
					if pm, okM := rp.(map[string]any); okM {
						isTh, _ := pm["thought"].(bool)
						if isTh || mapField(pm, "functionCall", "function_call") != nil {
							defaultRole = "assistant"
							break
						}
					}
				}
			}

			for pi, rp := range parts {
				pm, okM := rp.(map[string]any)
				if !okM {
					if ps, okS := rp.(string); okS && ps != "" {
						appendMessageBlocks(defaultRole, []any{map[string]any{"type": "text", "text": ps}})
					}
					continue
				}

				if fc := mapField(pm, "functionCall", "function_call"); fc != nil {
					name := strings.TrimSpace(stringField(fc, "name", ""))
					if name == "" {
						continue
					}
					id := strings.TrimSpace(stringField(fc, "id", ""))
					synthetic := false
					if id == "" {
						fifoSeq++
						id = fmt.Sprintf("gemini_fifo_%d", fifoSeq)
						synthetic = true
					}
					args := make(map[string]any)
					if rawArgs, okA := fc["args"].(map[string]any); okA && rawArgs != nil {
						args = rawArgs
					} else if argStr, okS := fc["args"].(string); okS && strings.TrimSpace(argStr) != "" {
						_ = unmarshalJSONPreserveNumbers([]byte(argStr), &args)
					}
					appendMessageBlocks("assistant", []any{
						map[string]any{"type": "tool_use", "id": id, "name": name, "input": args},
					})
					pendingFIFO = append(pendingFIFO, pendingCall{id: id, name: name, synthetic: synthetic})
					continue
				}

				if fr := mapField(pm, "functionResponse", "function_response"); fr != nil {
					name := strings.TrimSpace(stringField(fr, "name", ""))
					id := strings.TrimSpace(stringField(fr, "id", "call_id"))
					matchedSynthetic := false
					if id != "" {
						for i, p := range pendingFIFO {
							if p.id == id {
								pendingFIFO = append(pendingFIFO[:i], pendingFIFO[i+1:]...)
								break
							}
						}
					} else {
						matchIdx := -1
						for i, p := range pendingFIFO {
							if name != "" && p.name == name {
								matchIdx = i
								break
							}
						}
						if matchIdx < 0 && len(pendingFIFO) > 0 && name == "" {
							matchIdx = 0
						}
						if matchIdx >= 0 {
							matched := pendingFIFO[matchIdx]
							pendingFIFO = append(pendingFIFO[:matchIdx], pendingFIFO[matchIdx+1:]...)
							id = matched.id
							matchedSynthetic = matched.synthetic
							if name == "" {
								name = matched.name
							}
						} else {
							fifoSeq++
							id = fmt.Sprintf("gemini_fifo_%d", fifoSeq)
							matchedSynthetic = true
						}
					}
					if matchedSynthetic {
						syntheticBindings = append(syntheticBindings, syntheticToolBinding{
							SyntheticID: id,
							Name:        name,
						})
					}
					contentVal, isErr, frErr := formatGeminiFunctionResponse(fr, fmt.Sprintf("contents[%d].parts[%d].functionResponse", ci, pi), mediaCtx)
					if frErr != nil {
						return nil, frErr
					}
					trBlock := map[string]any{
						"type":        "tool_result",
						"tool_use_id": id,
						"content":     contentVal,
					}
					if isErr {
						trBlock["is_error"] = true
					}
					appendMessageBlocks("user", []any{trBlock})
					continue
				}

				if inl := mapField(pm, "inlineData", "inline_data"); inl != nil {
					blk, inlErr := normalizeGeminiInlineAttachmentWithCtx(inl, fmt.Sprintf("contents[%d].parts[%d].inlineData", ci, pi), false, mediaCtx, false)
					if inlErr != nil {
						return nil, inlErr
					}
					appendMessageBlocks(defaultRole, []any{blk})
					continue
				}

				if fd := mapField(pm, "fileData", "file_data"); fd != nil {
					blk, fdErr := normalizeGeminiFileAttachmentWithCtx(fd, fmt.Sprintf("contents[%d].parts[%d].fileData", ci, pi), mediaCtx, false)
					if fdErr != nil {
						return nil, fdErr
					}
					appendMessageBlocks(defaultRole, []any{blk})
					continue
				}

				if text, okT := pm["text"].(string); okT {
					if isThought, _ := pm["thought"].(bool); isThought {
						if text != "" {
							appendMessageBlocks("assistant", []any{map[string]any{"type": "thinking", "thinking": text}})
						}
					} else if text != "" {
						appendMessageBlocks(defaultRole, []any{map[string]any{"type": "text", "text": text}})
					}
				}
			}
		}
	}

	if len(sysParts) > 0 {
		outBody["system"] = strings.Join(sysParts, "\n\n")
	}
	outBody["messages"] = messages

	if len(messages) == 0 && !allowEmptyContents {
		return nil, errors.New("contents must contain at least one non-empty message")
	}

	if err := canonicalizeAndValidateMessagesWithCtx(outBody, messages, "/v1beta", mediaCtx); err != nil {
		return nil, err
	}
	selModel := ResolveRequestedModel(reqModel, outBody)
	_, totalImgs, _, err := validateLatestTurnImageBudget(messages, selModel.Model)
	if err != nil {
		return nil, err
	}

	displayModel := normalizeDisplayModel(reqModel, outBody)
	return &normalizedGeminiRequest{
		Body:              outBody,
		Messages:          messages,
		DisplayModel:      displayModel,
		IncludeThoughts:   extractIncludeThoughts(geminiBody),
		SyntheticBindings: syntheticBindings,
		TotalImages:       totalImgs,
	}, nil
}

// rebindUnidentifiedGeminiToolResults maps synthetic FIFO IDs back to the real parked MCP call IDs
// of the unique same-identity session when a Gemini client omits functionCall.id / functionResponse.id.
func (gs *GatewayServer) rebindUnidentifiedGeminiToolResults(r *http.Request, norm *normalizedGeminiRequest) {
	if gs == nil || norm == nil || len(norm.SyntheticBindings) == 0 || len(norm.Messages) == 0 {
		return
	}
	lastMsg := norm.Messages[len(norm.Messages)-1]
	if role, _ := lastMsg["role"].(string); role != "user" {
		return
	}
	trailingSet := make(map[string]bool)
	explicitClaimed := make(map[string]bool)
	if arr, ok := lastMsg["content"].([]any); ok {
		for _, item := range arr {
			if bm, okM := item.(map[string]any); okM && bm["type"] == "tool_result" {
				if id, okS := bm["tool_use_id"].(string); okS && id != "" {
					if strings.HasPrefix(id, "gemini_fifo_") {
						trailingSet[id] = true
					} else {
						explicitClaimed[id] = true
					}
				}
			}
		}
	}
	if len(trailingSet) == 0 {
		return
	}

	sysText := normalizeSystemText(norm.Body["system"])
	toolsCatalog := sanitizeToolsCatalog(norm.Body["tools"])
	baseKey := deriveConversationKey(r, norm.Body, sysText, toolsCatalog, norm.Messages)

	gs.mu.RLock()
	var candidate *AcpProjectSession
	var hibCandidate *AcpProjectSession
	ambiguous := false
	hibAmbiguous := false
	for key, s := range gs.sessions {
		if !sameConversationIdentity(key, baseKey) {
			continue
		}
		s.mu.Lock()
		hasParked := len(s.ParkedCalls) > 0
		hasHibParked := !hasParked && (len(s.HibernatedParkedCalls) > 0 || len(s.HibernatedParkedCallIDs) > 0)
		s.mu.Unlock()
		if hasParked {
			if candidate != nil {
				ambiguous = true
				break
			}
			candidate = s
		} else if hasHibParked {
			if hibCandidate != nil {
				hibAmbiguous = true
			} else {
				hibCandidate = s
			}
		}
	}
	gs.mu.RUnlock()
	useHibernated := false
	if candidate == nil && !ambiguous && hibCandidate != nil && !hibAmbiguous {
		candidate = hibCandidate
		useHibernated = true
	}
	if candidate == nil || ambiguous {
		return
	}

	idRewrite := make(map[string]string)
	if useHibernated {
		usedHib := make(map[string]bool, len(explicitClaimed))
		for id := range explicitClaimed {
			usedHib[id] = true
		}
		for _, bind := range norm.SyntheticBindings {
			if !trailingSet[bind.SyntheticID] {
				continue
			}
			if matchedID := candidate.MatchHibernatedParkedCallID(bind.Name, usedHib); matchedID != "" {
				usedHib[matchedID] = true
				idRewrite[bind.SyntheticID] = matchedID
			}
		}
	} else {
		type parkedCandidate struct {
			id   string
			name string
		}
		candidate.mu.Lock()
		seen := make(map[string]bool, len(candidate.ParkedCalls))
		var ordered []parkedCandidate
		for _, cid := range candidate.LastDeliveredCallIDs {
			if p, ok := candidate.ParkedCalls[cid]; ok {
				seen[cid] = true
				ordered = append(ordered, parkedCandidate{id: cid, name: p.Call.Name})
			}
		}
		if len(ordered) < len(candidate.ParkedCalls) {
			var extraIDs []string
			for cid := range candidate.ParkedCalls {
				if !seen[cid] {
					extraIDs = append(extraIDs, cid)
				}
			}
			sort.Strings(extraIDs)
			for _, cid := range extraIDs {
				ordered = append(ordered, parkedCandidate{id: cid, name: candidate.ParkedCalls[cid].Call.Name})
			}
		}
		candidate.mu.Unlock()
		if len(ordered) == 0 {
			return
		}

		claimed := make([]bool, len(ordered))
		for i, pc := range ordered {
			if explicitClaimed[pc.id] {
				claimed[i] = true
			}
		}
		for _, bind := range norm.SyntheticBindings {
			if !trailingSet[bind.SyntheticID] {
				continue
			}
			matchedIdx := -1
			for i, pc := range ordered {
				if !claimed[i] && bind.Name != "" && pc.name == bind.Name {
					matchedIdx = i
					break
				}
			}
			if matchedIdx < 0 && bind.Name == "" {
				for i := range ordered {
					if !claimed[i] {
						matchedIdx = i
						break
					}
				}
			}
			if matchedIdx >= 0 {
				claimed[matchedIdx] = true
				idRewrite[bind.SyntheticID] = ordered[matchedIdx].id
			}
		}
	}
	if len(idRewrite) == 0 {
		return
	}

	for _, msg := range norm.Messages {
		arr, ok := msg["content"].([]any)
		if !ok {
			continue
		}
		for _, item := range arr {
			bm, okM := item.(map[string]any)
			if !okM {
				continue
			}
			switch bm["type"] {
			case "tool_use":
				if oldID, okS := bm["id"].(string); okS {
					if realID, found := idRewrite[oldID]; found {
						bm["id"] = realID
					}
				}
			case "tool_result":
				if oldID, okS := bm["tool_use_id"].(string); okS {
					if realID, found := idRewrite[oldID]; found {
						bm["tool_use_id"] = realID
					}
				}
			}
		}
	}
}

func geminiFinishReason(stopReason string) string {
	if stopReason == "max_tokens" {
		return "MAX_TOKENS"
	}
	return "STOP"
}

func buildGeminiUsageMetadata(u *TurnUsage) map[string]any {
	return buildGeminiTurnUsageMetadata(TurnResult{Usage: u})
}

func buildGeminiTurnUsageMetadata(res TurnResult) map[string]any {
	u := res.Usage
	inTok, crTok, outTok := u.ClientTokens()
	thTok := effectiveReasoningTokens(res, outTok)
	candTok := outTok - thTok
	if candTok < 0 {
		candTok = 0
	}
	promptTok := inTok + crTok
	totalTok := promptTok + candTok + thTok
	meta := map[string]any{
		"promptTokenCount":     promptTok,
		"candidatesTokenCount": candTok,
		"totalTokenCount":      totalTok,
	}
	if crTok > 0 {
		meta["cachedContentTokenCount"] = crTok
	}
	if thTok > 0 {
		meta["thoughtsTokenCount"] = thTok
	}
	return meta
}

func buildGeminiResponseObject(respID, modelVersion string, parts []map[string]any, finishReason string, usage map[string]any) map[string]any {
	cand := map[string]any{
		"index": 0,
		"content": map[string]any{
			"role":  "model",
			"parts": parts,
		},
	}
	if finishReason != "" {
		cand["finishReason"] = finishReason
	}
	out := map[string]any{
		"responseId":   respID,
		"modelVersion": modelVersion,
		"candidates":   []map[string]any{cand},
	}
	if usage != nil {
		out["usageMetadata"] = usage
	}
	return out
}

func geminiWantsSSE(r *http.Request) bool {
	alt := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("alt")))
	if alt == "" {
		alt = strings.ToLower(strings.TrimSpace(r.URL.Query().Get("$alt")))
	}
	if alt == "sse" {
		return true
	}
	if alt != "" {
		return false
	}
	return strings.Contains(strings.ToLower(r.Header.Get("Accept")), "text/event-stream")
}

func (gs *GatewayServer) handleGeminiGenerateContent(w http.ResponseWriter, r *http.Request, reqID int64, pathModel string, isStream bool) {
	if tripped, reason := IsTosTripped(); tripped {
		recordPrimaryDirectOutcome(r, http.StatusServiceUnavailable, "tos_circuit_breaker", "", reason)
		writeGeminiError(w, http.StatusServiceUnavailable, "Gateway halted by ToS circuit breaker (delete runtime/tos-tripped to reset): "+reason)
		return
	}

	rawBody, ok := readGeminiJSONBody(w, r)
	if !ok {
		return
	}
	pathModelClean := strings.TrimPrefix(strings.TrimSpace(pathModel), "models/")
	if pathModelClean == "" {
		pathModelClean = strings.TrimPrefix(strings.TrimSpace(stringField(rawBody, "model", "")), "models/")
	}
	if isImageGenCanonicalModel(pathModelClean) {
		gs.handleGeminiImageGenerateContent(w, r, reqID, pathModelClean, rawBody, isStream)
		return
	}
	norm, err := normalizeGeminiV1BetaRequest(pathModel, rawBody, false)
	if err != nil {
		writeGeminiValidationError(w, err)
		return
	}
	gs.rebindUnidentifiedGeminiToolResults(r, norm)

	cleanPath, _ := extractGeminiPathKey(r.URL.Path)
	reqModel, _ := norm.Body["model"].(string)
	maxTok, _ := toInt(norm.Body["max_tokens"])
	var toolsLen int
	if ta, okT := norm.Body["tools"].([]any); okT {
		toolsLen = len(ta)
	}
	LogEvent("HTTP_REQ", fmt.Sprintf("REQ #%d %s %s", reqID, r.Method, cleanPath), map[string]any{
		"requestedModel": reqModel,
		"displayModel":   norm.DisplayModel,
		"stream":         isStream,
		"maxTokens":      maxTok,
		"messagesCount":  len(norm.Messages),
		"toolsCount":     toolsLen,
	})

	respID := "gem_" + randomHex(12)
	displayModel := norm.DisplayModel

	if !isStream {
		res, turnErr := gs.HandleMessagesRequest(r, norm.Body, norm.Messages, nil)
		if turnErr == nil {
			if softErr := DetectAntigravitySoftError(res.Text); softErr != nil {
				turnErr = softErr
			}
		}
		if turnErr != nil {
			httpCode, retryAfter, errMsg := classifyGeminiUpstreamError(turnErr)
			_, errType, _, _ := classifyUpstreamError(turnErr)
			raHdr := ""
			if retryAfter {
				raHdr = extractRetryAfterHeaderValue(errMsg)
			}
			recordPrimaryDirectOutcome(r, httpCode, errType, raHdr, errMsg)
			LogEvent("HTTP_ERR", fmt.Sprintf("REQ #%d Failed: %v", reqID, turnErr), nil)
			if retryAfter {
				w.Header().Set("Retry-After", extractRetryAfterHeaderValue(errMsg))
			}
			if isChildAccountWorker() && isLocalWorkerSlotError(httpCode, errType, errMsg, nil) {
				w.Header().Set(internalLocalSlotFullHeader, "1")
			}
			writeGeminiError(w, httpCode, errMsg)
			return
		}

		setAgyTurnHeaders(w, res.Usage)

		var parts []map[string]any
		if norm.IncludeThoughts && res.Thinking != "" {
			parts = append(parts, map[string]any{"thought": true, "text": res.Thinking})
		}
		if res.Text != "" {
			parts = append(parts, map[string]any{"text": res.Text})
		}
		for _, tc := range res.ToolCalls {
			args := tc.Input
			if args == nil {
				args = map[string]any{}
			}
			parts = append(parts, map[string]any{
				"functionCall": map[string]any{
					"id":   tc.ID,
					"name": tc.Name,
					"args": args,
				},
			})
		}
		if len(parts) == 0 {
			parts = []map[string]any{{"text": ""}}
		}

		payload := buildGeminiResponseObject(
			respID,
			displayModel,
			parts,
			geminiFinishReason(res.StopReason),
			buildGeminiTurnUsageMetadata(res),
		)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(payload)
		return
	}

	useSSE := geminiWantsSSE(r)
	ct := "application/json; charset=utf-8"
	if useSSE {
		ct = "text/event-stream; charset=utf-8"
	}
	stream := protocol.NewSSEStreamSession(w, protocol.SSEStreamOptions{
		ContentType:       ct,
		HeartbeatInterval: sseHeartbeatInterval,
		WriteTimeout:      sseWriteTimeout,
	})
	defer stream.ClearDeadline()

	arrayOpened := false
	arrayCount := 0
	streamedThinking := false
	streamedText := false

	emitChunkLocked := func(sw *protocol.StreamWriter, obj map[string]any) {
		b, _ := json.Marshal(obj)
		if useSSE {
			sw.WriteRaw(fmt.Sprintf("data: %s\n\n", string(b)))
			return
		}
		var frame strings.Builder
		if !arrayOpened {
			arrayOpened = true
			frame.WriteString("[\n")
		}
		if arrayCount > 0 {
			frame.WriteString(",\n")
		}
		frame.Write(b)
		arrayCount++
		sw.WriteRaw(frame.String())
	}

	stream.StartHeartbeat(func(sw *protocol.StreamWriter) {
		if useSSE {
			sw.WriteRaw(": ping\n\n")
		} else {
			sw.WriteRaw("\n")
		}
	})
	defer stream.StopHeartbeat()

	onDelta := func(c DeltaChunk) {
		if c.Type == "thinking" && c.Thinking != "" {
			if !norm.IncludeThoughts {
				return
			}
			stream.DoLocked(func(sw *protocol.StreamWriter) {
				streamedThinking = true
				emitChunkLocked(sw, buildGeminiResponseObject(
					respID,
					displayModel,
					[]map[string]any{{"thought": true, "text": c.Thinking}},
					"",
					nil,
				))
			})
		} else if c.Type == "text" && c.Text != "" {
			stream.DoLocked(func(sw *protocol.StreamWriter) {
				streamedText = true
				emitChunkLocked(sw, buildGeminiResponseObject(
					respID,
					displayModel,
					[]map[string]any{{"text": c.Text}},
					"",
					nil,
				))
			})
		}
	}

	r = withSessionWarnHook(r, func(warn string) {
		stream.SetPreHeader("X-Agy-Cache-Warning", warn)
	})
	res, turnErr := gs.HandleMessagesRequest(r, norm.Body, norm.Messages, onDelta)
	stream.StopHeartbeat()

	if turnErr == nil {
		if softErr := DetectAntigravitySoftError(res.Text); softErr != nil {
			turnErr = softErr
		}
	}
	if turnErr != nil {
		httpCode, retryAfter, errMsg := classifyGeminiUpstreamError(turnErr)
		_, errType, _, _ := classifyUpstreamError(turnErr)
		raHdr := ""
		if retryAfter {
			raHdr = extractRetryAfterHeaderValue(errMsg)
		}
		recordPrimaryDirectOutcome(r, httpCode, errType, raHdr, errMsg)
		LogEvent("HTTP_ERR", fmt.Sprintf("REQ #%d Failed: %v", reqID, turnErr), nil)
		stream.DoLocked(func(sw *protocol.StreamWriter) {
			if !sw.HeadersCommitted() {
				var extra map[string]string
				if retryAfter {
					extra = map[string]string{"Retry-After": extractRetryAfterHeaderValue(errMsg)}
				}
				if isChildAccountWorker() && isLocalWorkerSlotError(httpCode, errType, errMsg, nil) {
					if extra == nil {
						extra = map[string]string{}
					}
					extra[internalLocalSlotFullHeader] = "1"
				}
				status := httpCode
				if status == 529 {
					status = http.StatusTooManyRequests
				}
				sw.CommitErrorStatus(status, extra, "application/json; charset=utf-8")
				b, _ := json.Marshal(buildGeminiErrorPayload(httpCode, errMsg))
				sw.WriteRaw(string(b) + "\n")
				return
			}
			errBytes, _ := json.Marshal(buildGeminiErrorPayload(httpCode, errMsg))
			if useSSE {
				sw.WriteRaw(fmt.Sprintf("data: %s\n\n", string(errBytes)))
			} else {
				var frame strings.Builder
				if !arrayOpened {
					frame.WriteString("[\n")
				} else if arrayCount > 0 {
					frame.WriteString(",\n")
				}
				frame.Write(errBytes)
				frame.WriteString("\n]\n")
				sw.WriteRaw(frame.String())
			}
		})
		return
	}

	var finalParts []map[string]any
	if norm.IncludeThoughts && !streamedThinking && res.Thinking != "" {
		finalParts = append(finalParts, map[string]any{"thought": true, "text": res.Thinking})
	}
	if !streamedText && res.Text != "" {
		finalParts = append(finalParts, map[string]any{"text": res.Text})
	}
	for _, tc := range res.ToolCalls {
		args := tc.Input
		if args == nil {
			args = map[string]any{}
		}
		finalParts = append(finalParts, map[string]any{
			"functionCall": map[string]any{
				"id":   tc.ID,
				"name": tc.Name,
				"args": args,
			},
		})
	}
	if len(finalParts) == 0 {
		finalParts = []map[string]any{{"text": ""}}
	}

	stream.DoLocked(func(sw *protocol.StreamWriter) {
		if !sw.HeadersCommitted() {
			setAgyTurnHeadersOn(sw.Header(), res.Usage)
		}
		emitChunkLocked(sw, buildGeminiResponseObject(
			respID,
			displayModel,
			finalParts,
			geminiFinishReason(res.StopReason),
			buildGeminiTurnUsageMetadata(res),
		))
		if !useSSE && arrayOpened {
			sw.WriteRaw("\n]\n")
		}
	})
}

type geminiInlineImageRef struct {
	data      string
	mimeType  string
	fieldPath string
}

func parseGeminiImageResponseMime(raw string) (string, error) {
	s := strings.ToLower(strings.TrimSpace(raw))
	switch s {
	case "", "image/jpeg", "image/jpg":
		return "jpeg", nil
	case "image/png":
		return "png", nil
	default:
		return "", fmt.Errorf("unsupported responseMimeType %q for image generation; allowed values: image/jpeg, image/png", raw)
	}
}

func buildGeminiImageCandidates(results []generatedImageResult) []map[string]any {
	cands := make([]map[string]any, len(results))
	for i := range results {
		cands[i] = map[string]any{
			"index": i,
			"content": map[string]any{
				"role": "model",
				"parts": []map[string]any{
					{
						"inlineData": map[string]any{
							"mimeType": results[i].Mime,
							"data":     results[i].B64,
						},
					},
				},
			},
			"finishReason": "STOP",
		}
		results[i].B64 = ""
	}
	return cands
}

func (gs *GatewayServer) handleGeminiImageGenerateContent(w http.ResponseWriter, r *http.Request, reqID int64, reqModel string, rawBody map[string]any, isStream bool) {
	gc := mapField(rawBody, "generationConfig", "generation_config")
	imgCfg := mapField(gc, "imageConfig", "image_config")
	if imgCfg == nil {
		imgCfg = mapField(rawBody, "imageConfig", "image_config")
	}
	aspectRatio := stringField(imgCfg, "aspectRatio", "aspect_ratio")
	if aspectRatio == "" {
		aspectRatio = stringField(gc, "aspectRatio", "aspect_ratio")
	}
	if aspectRatio == "" {
		aspectRatio = stringField(rawBody, "aspectRatio", "aspect_ratio")
	}

	nVal := 1
	hasN := false
	if gc != nil {
		if _, okC := gc["candidateCount"]; okC {
			v, hN, errN := parseOptionalJSONInt(gc, "candidateCount")
			if errN != nil {
				writeGeminiError(w, http.StatusBadRequest, errN.Error())
				return
			}
			nVal, hasN = v, hN
		} else if _, okS := gc["candidate_count"]; okS {
			v, hN, errN := parseOptionalJSONInt(gc, "candidate_count")
			if errN != nil {
				writeGeminiError(w, http.StatusBadRequest, errN.Error())
				return
			}
			nVal, hasN = v, hN
		}
	}

	respMime := stringField(gc, "responseMimeType", "response_mime_type")
	if respMime == "" {
		respMime = stringField(imgCfg, "outputMimeType", "output_mime_type")
	}
	outputFormat, errMime := parseGeminiImageResponseMime(respMime)
	if errMime != nil {
		writeGeminiError(w, http.StatusBadRequest, errMime.Error())
		return
	}

	var sysParts []string
	if sysInst, exists := rawBody["systemInstruction"]; exists && sysInst != nil {
		if s := strings.TrimSpace(extractGeminiTextParts(sysInst)); s != "" {
			sysParts = append(sysParts, s)
		}
	} else if sysInst, exists := rawBody["system_instruction"]; exists && sysInst != nil {
		if s := strings.TrimSpace(extractGeminiTextParts(sysInst)); s != "" {
			sysParts = append(sysParts, s)
		}
	}

	var rawContents []any
	switch cv := rawBody["contents"].(type) {
	case []any:
		rawContents = cv
	case map[string]any:
		rawContents = []any{cv}
	case string:
		if strings.TrimSpace(cv) != "" {
			rawContents = []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": cv}}}}
		}
	}

	var (
		turns   []imagePromptTurn
		imgRefs []geminiInlineImageRef
	)
	for ci, rc := range rawContents {
		switch item := rc.(type) {
		case string:
			if strings.TrimSpace(item) != "" {
				turns = append(turns, imagePromptTurn{Role: "user", Text: item})
			}
		case map[string]any:
			rawRole := strings.ToLower(strings.TrimSpace(stringField(item, "role", "")))
			role := "user"
			if rawRole == "model" || rawRole == "assistant" {
				role = "assistant"
			} else if rawRole == "system" || rawRole == "developer" {
				if s := strings.TrimSpace(extractGeminiTextParts(item)); s != "" {
					sysParts = append(sysParts, s)
				}
				continue
			}

			var parts []any
			if pArr, ok := item["parts"].([]any); ok {
				parts = pArr
			} else if pMap, ok := item["parts"].(map[string]any); ok {
				parts = []any{pMap}
			} else if t, ok := item["text"].(string); ok {
				parts = []any{map[string]any{"text": t}}
			}

			var turnTexts []string
			for pi, rp := range parts {
				switch pm := rp.(type) {
				case string:
					if strings.TrimSpace(pm) != "" {
						turnTexts = append(turnTexts, pm)
					}
				case map[string]any:
					if inl := mapField(pm, "inlineData", "inline_data"); inl != nil {
						fp := fmt.Sprintf("contents[%d].parts[%d].inlineData", ci, pi)
						mimeStr := strings.TrimSpace(stringField(inl, "mimeType", "mime_type"))
						if strings.EqualFold(normalizeMimeBase(mimeStr), "image/svg+xml") {
							writeGeminiError(w, http.StatusBadRequest, fp+": image/svg+xml attachments are not supported")
							return
						}
						if mimeStr != "" && !hasPrefixFold(mimeStr, "image/") {
							writeGeminiValidationError(w, rejectUnsupportedMedia(mimeStr, fp))
							return
						}
						dataStr := strings.TrimSpace(stringField(inl, "data", ""))
						if dataStr != "" {
							imgRefs = append(imgRefs, geminiInlineImageRef{
								data:      dataStr,
								mimeType:  mimeStr,
								fieldPath: fp,
							})
						}
						continue
					}
					if fd := mapField(pm, "fileData", "file_data"); fd != nil {
						fp := fmt.Sprintf("contents[%d].parts[%d].fileData", ci, pi)
						writeGeminiError(w, http.StatusBadRequest, fp+": image generation only supports inlineData (base64); please pass reference images via inlineData instead of fileData")
						return
					}
					if isThought, _ := pm["thought"].(bool); isThought {
						continue
					}
					if t, okT := pm["text"].(string); okT && strings.TrimSpace(t) != "" {
						turnTexts = append(turnTexts, t)
					}
				}
			}
			if joined := strings.TrimSpace(strings.Join(turnTexts, "\n")); joined != "" {
				turns = append(turns, imagePromptTurn{Role: role, Text: joined})
			}
		}
	}

	prompt := buildMultiTurnImagePrompt(strings.Join(sysParts, "\n"), turns)
	if prompt == "" {
		writeGeminiError(w, http.StatusBadRequest, "contents must contain a non-empty text prompt for image generation")
		return
	}
	params, valErr := parseAndValidateCommonImageParams(prompt, reqModel, "", aspectRatio, "b64_json", outputFormat, nVal, hasN)
	if valErr != nil {
		writeGeminiError(w, http.StatusBadRequest, valErr.Error())
		return
	}

	// Keep only the last maxImageEditInputs (3) valid images in chronological order.
	if len(imgRefs) > maxImageEditInputs {
		imgRefs = imgRefs[len(imgRefs)-maxImageEditInputs:]
	}
	var imgs []validatedEditImage
	if len(imgRefs) > 0 {
		imgs = make([]validatedEditImage, 0, len(imgRefs))
		for _, ref := range imgRefs {
			vi, dErr := decodeEditImageString(ref.data, ref.fieldPath)
			if dErr != nil {
				for j := range imgs {
					imgs[j].raw = nil
				}
				imgs = nil
				writeGeminiValidationError(w, dErr)
				return
			}
			imgs = append(imgs, vi)
		}
	}
	imgRefs = nil
	rawBody = nil

	tmpDir, absPaths, stageErr := stageValidatedEditImages(reqID, imgs)
	for i := range imgs {
		imgs[i].raw = nil
	}
	imgs = nil
	if stageErr != nil {
		writeGeminiError(w, http.StatusInternalServerError, stageErr.Error())
		return
	}
	if tmpDir != "" {
		defer func() {
			_ = os.RemoveAll(tmpDir)
		}()
	}

	cleanPath, _ := extractGeminiPathKey(r.URL.Path)
	LogEvent("HTTP_REQ", fmt.Sprintf("REQ #%d %s %s (image-gen)", reqID, r.Method, cleanPath), map[string]any{
		"requestedModel": reqModel,
		"servedModel":    imageGenServedModel,
		"stream":         isStream,
		"n":              params.N,
		"aspectRatio":    params.AspectRatio,
		"outputFormat":   params.OutputFormat,
		"refImages":      len(absPaths),
	})

	respID := "gem_img_" + randomHex(12)
	promptTok := int(math.Ceil(float64(len(params.Prompt)) / 4.0))
	if promptTok < 1 {
		promptTok = 1
	}

	if !isStream {
		results, imgErr := generateImageBatch(r.Context(), reqID, params, absPaths)
		if imgErr != nil {
			recordPrimaryDirectOutcome(r, imgErr.StatusCode, imgErr.ErrType, "", imgErr.Message)
			if imgErr.StatusCode == 499 {
				return
			}
			writeGeminiError(w, imgErr.StatusCode, imgErr.Message)
			return
		}
		candTok := len(results) * 256
		cands := buildGeminiImageCandidates(results)
		results = nil

		payload := map[string]any{
			"responseId":   respID,
			"modelVersion": imageGenServedModel,
			"candidates":   cands,
			"usageMetadata": map[string]any{
				"promptTokenCount":     promptTok,
				"candidatesTokenCount": candTok,
				"totalTokenCount":      promptTok + candTok,
			},
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("X-Agy-Served-Model", imageGenServedModel)
		_ = json.NewEncoder(w).Encode(payload)
		clear(cands)
		return
	}

	useSSE := geminiWantsSSE(r)
	ct := "application/json; charset=utf-8"
	if useSSE {
		ct = "text/event-stream; charset=utf-8"
	}
	stream := protocol.NewSSEStreamSession(w, protocol.SSEStreamOptions{
		ContentType:       ct,
		HeartbeatInterval: sseHeartbeatInterval,
		WriteTimeout:      sseWriteTimeout,
	})
	defer stream.ClearDeadline()
	stream.SetPreHeader("X-Agy-Served-Model", imageGenServedModel)
	stream.StartHeartbeat(func(sw *protocol.StreamWriter) {
		if useSSE {
			sw.WriteRaw(": ping\n\n")
		} else {
			sw.WriteRaw("\n")
		}
	})

	results, imgErr := generateImageBatch(r.Context(), reqID, params, absPaths)
	stream.StopHeartbeat()
	if imgErr != nil {
		recordPrimaryDirectOutcome(r, imgErr.StatusCode, imgErr.ErrType, "", imgErr.Message)
		if imgErr.StatusCode == 499 {
			return
		}
		stream.DoLocked(func(sw *protocol.StreamWriter) {
			if !sw.HeadersCommitted() {
				sw.CommitErrorStatus(imgErr.StatusCode, nil, "application/json; charset=utf-8")
				b, _ := json.Marshal(buildGeminiErrorPayload(imgErr.StatusCode, imgErr.Message))
				sw.WriteRaw(string(b) + "\n")
				return
			}
			errBytes, _ := json.Marshal(buildGeminiErrorPayload(imgErr.StatusCode, imgErr.Message))
			if useSSE {
				sw.WriteRaw(fmt.Sprintf("data: %s\n\n", string(errBytes)))
			} else {
				sw.WriteRaw("[\n" + string(errBytes) + "\n]\n")
			}
		})
		return
	}

	candTok := len(results) * 256
	cands := buildGeminiImageCandidates(results)
	results = nil
	payload := map[string]any{
		"responseId":   respID,
		"modelVersion": imageGenServedModel,
		"candidates":   cands,
		"usageMetadata": map[string]any{
			"promptTokenCount":     promptTok,
			"candidatesTokenCount": candTok,
			"totalTokenCount":      promptTok + candTok,
		},
	}
	b, _ := json.Marshal(payload)
	clear(cands)
	payload = nil

	stream.DoLocked(func(sw *protocol.StreamWriter) {
		if useSSE {
			sw.WriteRaw(fmt.Sprintf("data: %s\n\n", string(b)))
		} else {
			sw.WriteRaw("[\n" + string(b) + "\n]\n")
		}
	})
	b = nil
}
