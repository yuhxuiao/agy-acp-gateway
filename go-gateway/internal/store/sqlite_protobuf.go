package store

import (
	"agy-acp-gateway-go/internal/protocol"
	"bytes"
	"database/sql"
	"encoding/binary"
	"math"
	_ "modernc.org/sqlite"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type GeminiPricing struct {
	BaseInputPerM       float64 `json:"baseInputPerM"`
	BaseCachedReadPerM  float64 `json:"baseCachedReadPerM"`
	BaseOutputPerM      float64 `json:"baseOutputPerM"`
	Context1MMultiplier float64 `json:"context1MMultiplier"`
	InputPerM           float64 `json:"inputPerM"`
	CachedReadPerM      float64 `json:"cachedReadPerM"`
	OutputPerM          float64 `json:"outputPerM"`
	Cap5hUsd            float64 `json:"cap5hUsd"`
	CapWeeklyUsd        float64 `json:"capWeeklyUsd"`
}

// DefaultGeminiPricing describes the Gemini quota pool in Gemini 4 Argon list-price USD.
// Pool sizes measured 2026-11-11 against official /usage fractions (weekly = 4x the 5h pool).
var DefaultGeminiPricing = GeminiPricing{
	BaseInputPerM:       2.0,
	BaseCachedReadPerM:  0.10,
	BaseOutputPerM:      10.0,
	Context1MMultiplier: 1.0,
	InputPerM:           2.0,
	CachedReadPerM:      0.10,
	OutputPerM:          10.0,
	Cap5hUsd:            200.0,
	CapWeeklyUsd:        800.0,
}

// DefaultClaudePoolPricing describes the Ultra/20x Claude/3p quota pool in Claude 5.5 list-price USD.
// Pool sizes measured 2026-11-11 against official /usage fractions ($25 per 5h / $50 per week).
var DefaultClaudePoolPricing = GeminiPricing{
	BaseInputPerM:       4.0,
	BaseCachedReadPerM:  0.20,
	BaseOutputPerM:      20.0,
	Context1MMultiplier: 1.0,
	InputPerM:           5.0,
	CachedReadPerM:      0.20,
	OutputPerM:          20.0,
	Cap5hUsd:            25.0,
	CapWeeklyUsd:        50.0,
}

// ProGeminiPricing describes the Google AI Pro Gemini quota pool (Neon / Hydrogen / Flash;
// Argon has its own separate weekly pool on Pro). Measured 2026-11-20 against official /usage
// fractions on two Pro accounts ($10 per 5h / $35 per week; runnable tokens estimated at Neon rate).
var ProGeminiPricing = GeminiPricing{
	BaseInputPerM:       1.0,
	BaseCachedReadPerM:  0.05,
	BaseOutputPerM:      5.0,
	Context1MMultiplier: 1.0,
	InputPerM:           1.0,
	CachedReadPerM:      0.05,
	OutputPerM:          5.0,
	Cap5hUsd:            10.0,
	CapWeeklyUsd:        35.0,
}

// ProClaudePoolPricing describes the Google AI Pro Claude/3p quota pool in Claude 5.5 list-price USD.
// Measured 2026-11-20 against official /usage fractions ($2.50 per 5h / $5.00 per week, 1/10 of Ultra).
var ProClaudePoolPricing = GeminiPricing{
	BaseInputPerM:       4.0,
	BaseCachedReadPerM:  0.20,
	BaseOutputPerM:      20.0,
	Context1MMultiplier: 1.0,
	InputPerM:           5.0,
	CachedReadPerM:      0.20,
	OutputPerM:          20.0,
	Cap5hUsd:            2.5,
	CapWeeklyUsd:        5.0,
}

type ParsedModelStep struct {
	SessionID      string  `json:"sessionId"`
	SourceSide     string  `json:"sourceSide"`
	SourceCategory string  `json:"sourceCategory"`
	SourceLabel    string  `json:"sourceLabel"`
	StepIdx        int     `json:"stepIdx"`
	TsSec          int64   `json:"tsSec"`
	TtftMs         int     `json:"ttftMs"`
	StreamMs       int     `json:"streamMs,omitempty"`
	ElapsedMs      int     `json:"elapsedMs"`
	Tps            float64 `json:"tps"`
	DecodeTps      float64 `json:"decodeTps,omitempty"`
	ModelEnum      int     `json:"modelEnum"`
	ModelID        string  `json:"modelId"`
	ModelLabel     string  `json:"modelLabel"`
	ModelFamily    string  `json:"modelFamily"`
	ModelTier      string  `json:"modelTier"`
	QuotaGroup     string  `json:"quotaGroup"`
	RawIn          int     `json:"rawIn"`
	RawCr          int     `json:"rawCr"`
	AlignedIn      int     `json:"alignedIn"`
	AlignedCr      int     `json:"alignedCr"`
	OutTok         int     `json:"outTok"`
	VisOutTok      int     `json:"visOutTok"`
	ThTok          int     `json:"thTok"`
	HasThinking    bool    `json:"hasThinking"`
	CwTok          int     `json:"cwTok"`
	TierMultiplier float64 `json:"tierMultiplier"`
	InputCostUsd   float64 `json:"inputCostUsd"`
	CacheCostUsd   float64 `json:"cacheCostUsd"`
	OutputCostUsd  float64 `json:"outputCostUsd"`
	ThinkCostUsd   float64 `json:"thinkCostUsd"`
	CostUsd        float64 `json:"costUsd"`
	Base1xCostUsd  float64 `json:"base1xCostUsd"`
	NoCacheCostUsd float64 `json:"noCacheCostUsd"`
	SavedUsd       float64 `json:"savedUsd"`
	IsDshBridge    bool    `json:"-"`
	ServedModel    string  `json:"servedModel,omitempty"`
}

// Protobuf varint reader (base-128 little-endian)
func readProtoVarint(buf []byte, pos int) (uint64, int, bool) {
	var val uint64
	var shift uint
	for pos < len(buf) {
		b := buf[pos]
		pos++
		val |= uint64(b&0x7f) << shift
		if (b & 0x80) == 0 {
			return val, pos, true
		}
		shift += 7
		if shift >= 64 {
			return 0, pos, false
		}
	}
	return 0, pos, false
}

func parseProtoTimestamp(sub []byte) float64 {
	pos := 0
	var sec int64
	var nanos int64
	for pos < len(sub) {
		tag, next, ok := readProtoVarint(sub, pos)
		if !ok {
			break
		}
		pos = next
		sfn := tag >> 3
		swt := tag & 7
		if swt == 0 {
			v, nextV, okV := readProtoVarint(sub, pos)
			if !okV {
				break
			}
			pos = nextV
			if sfn == 1 {
				sec = int64(v)
			} else if sfn == 2 {
				nanos = int64(v)
			}
		} else {
			break
		}
	}
	if sec > 0 {
		return float64(sec) + float64(nanos)/1e9
	}
	return 0
}

func parseProtoDuration(sub []byte) float64 {
	pos := 0
	var sec int64
	var nanos int64
	for pos < len(sub) {
		tag, next, ok := readProtoVarint(sub, pos)
		if !ok {
			break
		}
		pos = next
		sfn := tag >> 3
		swt := tag & 7
		if swt == 0 {
			v, nextV, okV := readProtoVarint(sub, pos)
			if !okV {
				break
			}
			pos = nextV
			if sfn == 1 {
				sec = int64(v)
			} else if sfn == 2 {
				nanos = int64(v)
			}
		} else {
			break
		}
	}
	if sec > 0 || nanos > 0 {
		return float64(sec) + float64(nanos)/1e9
	}
	return 0
}

type ParsedGenMetadata struct {
	SessionID        string  `json:"sessionId"`
	Idx              int     `json:"idx"`
	StepIndices      []int   `json:"stepIndices,omitempty"`
	ModelEnum        int     `json:"modelEnum"`
	ModelID          string  `json:"modelId"`
	InputTokens      int     `json:"inputTokens"`
	OutputTokens     int     `json:"outputTokens"`
	CacheWriteTokens int     `json:"cacheWriteTokens"`
	CachedReadTokens int     `json:"cachedReadTokens"`
	ThoughtTokens    int     `json:"thoughtTokens"`
	TextOutTokens    int     `json:"textOutTokens"`
	CwmEstTokens     int     `json:"cwmEstTokens"`
	CwmMaxTokens     int     `json:"cwmMaxTokens"`
	TtftSeconds      float64 `json:"ttftSeconds"`
	StreamSeconds    float64 `json:"streamSeconds"`
	// ServedModel is the upstream modelVersion persisted by agy in ChatModelMetadata:
	// response_model_full (.1.22) when non-empty, else response_model (.1.19, verbatim from
	// cloudcode-pa GenerateContentResponse.modelVersion, e.g. gemini-4-argon-b on shadow routing
	// or gemini-4-carbon on the primary pool). ModelDisplayName is .1.21.
	ServedModel       string `json:"servedModel,omitempty"`
	ResponseModelFull string `json:"responseModelFull,omitempty"`
	ModelDisplayName  string `json:"modelDisplayName,omitempty"`
}

func parseGenMetadataF1(sub []byte, gm *ParsedGenMetadata) {
	pos := 0
	for pos < len(sub) {
		tag, next, ok := readProtoVarint(sub, pos)
		if !ok {
			break
		}
		pos = next
		fn := tag >> 3
		wt := tag & 7
		if fn == 0 {
			break
		}
		switch wt {
		case 0:
			v, nextV, okV := readProtoVarint(sub, pos)
			if !okV {
				return
			}
			pos = nextV
			if fn == 3 && gm.ModelEnum == 0 {
				gm.ModelEnum = int(v)
			}
		case 2:
			ln, nextL, okL := readProtoVarint(sub, pos)
			if !okL || int(ln) < 0 || nextL+int(ln) > len(sub) {
				return
			}
			child := sub[nextL : nextL+int(ln)]
			pos = nextL + int(ln)
			switch fn {
			case 19:
				if isPrintableModelName(child) && gm.ResponseModelFull == "" {
					gm.ServedModel = string(child)
				}
			case 21:
				if isPrintableLabel(child) {
					gm.ModelDisplayName = string(child)
				}
			case 22:
				if isPrintableModelName(child) {
					gm.ResponseModelFull = string(child)
					gm.ServedModel = string(child)
				}
			case 4:
				sp := 0
				for sp < len(child) {
					st, nextSt, okSt := readProtoVarint(child, sp)
					if !okSt {
						break
					}
					sp = nextSt
					sfn := st >> 3
					swt := st & 7
					switch swt {
					case 0:
						sv, nextSv, okSv := readProtoVarint(child, sp)
						if !okSv {
							sp = len(child)
							break
						}
						sp = nextSv
						switch sfn {
						case 1:
							gm.ModelEnum = int(sv)
						case 2:
							gm.InputTokens = int(sv)
						case 3:
							gm.OutputTokens = int(sv)
						case 4:
							gm.CacheWriteTokens = int(sv)
						case 5:
							gm.CachedReadTokens = int(sv)
						case 9:
							gm.ThoughtTokens = int(sv)
						case 10:
							gm.TextOutTokens = int(sv)
						}
					case 2:
						sln, nextSln, okSln := readProtoVarint(child, sp)
						if !okSln || int(sln) < 0 || nextSln+int(sln) > len(child) {
							sp = len(child)
							break
						}
						sp = nextSln + int(sln)
					case 1:
						sp += 8
					case 5:
						sp += 4
					default:
						sp = len(child)
					}
				}
			case 9:
				sp := 0
				for sp < len(child) {
					st, nextSt, okSt := readProtoVarint(child, sp)
					if !okSt {
						break
					}
					sp = nextSt
					sfn := st >> 3
					swt := st & 7
					switch swt {
					case 0:
						_, nextSv, okSv := readProtoVarint(child, sp)
						if !okSv {
							sp = len(child)
							break
						}
						sp = nextSv
					case 2:
						sln, nextSln, okSln := readProtoVarint(child, sp)
						if !okSln || int(sln) < 0 || nextSln+int(sln) > len(child) {
							sp = len(child)
							break
						}
						cwmSub := child[nextSln : nextSln+int(sln)]
						sp = nextSln + int(sln)
						if sfn == 10 {
							qp := 0
							for qp < len(cwmSub) {
								qt, nextQt, okQt := readProtoVarint(cwmSub, qp)
								if !okQt {
									break
								}
								qp = nextQt
								qfn := qt >> 3
								qwt := qt & 7
								if qwt == 0 {
									qv, nextQv, okQv := readProtoVarint(cwmSub, qp)
									if !okQv {
										break
									}
									qp = nextQv
									if qfn == 1 {
										gm.CwmEstTokens = int(qv)
									} else if qfn == 4 {
										gm.CwmMaxTokens = int(qv)
									}
								} else if qwt == 2 {
									qln, nextQln, okQln := readProtoVarint(cwmSub, qp)
									if !okQln || int(qln) < 0 || nextQln+int(qln) > len(cwmSub) {
										break
									}
									qp = nextQln + int(qln)
								} else if qwt == 1 {
									qp += 8
								} else if qwt == 5 {
									qp += 4
								} else {
									break
								}
							}
						}
					case 1:
						sp += 8
					case 5:
						sp += 4
					default:
						sp = len(child)
					}
				}
			case 11:
				if d := parseProtoDuration(child); d > 0 {
					gm.TtftSeconds = d
				}
			case 12:
				if d := parseProtoDuration(child); d > 0 {
					gm.StreamSeconds = d
				}
			}
		case 1:
			pos += 8
		case 5:
			pos += 4
		default:
			return
		}
	}
}

func isPrintableModelName(b []byte) bool {
	if len(b) == 0 || len(b) > 96 {
		return false
	}
	for _, c := range b {
		if c < 0x21 || c > 0x7e {
			return false
		}
	}
	return true
}

func isPrintableLabel(b []byte) bool {
	if len(b) == 0 || len(b) > 96 {
		return false
	}
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}

func ParseGenMetadataBlob(data []byte) *ParsedGenMetadata {
	if len(data) == 0 {
		return nil
	}
	var gm ParsedGenMetadata
	pos := 0
	for pos < len(data) {
		tag, next, ok := readProtoVarint(data, pos)
		if !ok {
			break
		}
		pos = next
		fn := tag >> 3
		wt := tag & 7
		if fn == 0 {
			break
		}
		switch wt {
		case 0:
			v, nextV, okV := readProtoVarint(data, pos)
			if !okV {
				pos = len(data)
				break
			}
			pos = nextV
			if fn == 2 {
				gm.StepIndices = append(gm.StepIndices, int(v))
			}
		case 2:
			ln, nextL, okL := readProtoVarint(data, pos)
			if !okL || int(ln) < 0 || nextL+int(ln) > len(data) {
				pos = len(data)
				break
			}
			sub := data[nextL : nextL+int(ln)]
			pos = nextL + int(ln)
			switch fn {
			case 1:
				parseGenMetadataF1(sub, &gm)
			case 2:
				sp := 0
				for sp < len(sub) {
					sv, nextSp, okSp := readProtoVarint(sub, sp)
					if !okSp {
						break
					}
					sp = nextSp
					gm.StepIndices = append(gm.StepIndices, int(sv))
				}
			}
		case 1:
			pos += 8
		case 5:
			pos += 4
		default:
			pos = len(data)
		}
	}
	if gm.InputTokens == 0 && gm.CachedReadTokens == 0 && gm.OutputTokens == 0 {
		return nil
	}
	m, _ := protocol.ModelFromEnum(gm.ModelEnum)
	gm.ModelID = m.ID
	return &gm
}

type ArgonDegradationReport struct {
	Degraded      bool    `json:"degraded"`
	ServedModel   string  `json:"servedModel,omitempty"`
	CwmEstTokens  int     `json:"cwmEstTokens,omitempty"`
	PromptTokens  int     `json:"promptTokens"`
	CachedTokens  int     `json:"cachedTokens"`
	OutputTokens  int     `json:"outputTokens"`
	ThoughtTokens int     `json:"thoughtTokens"`
	TtftSeconds   float64 `json:"ttftSeconds,omitempty"`
	StreamSeconds float64 `json:"streamSeconds,omitempty"`
}

// EvaluateArgonDegradation deterministically detects Gemini 4 Argon shadow/canary pool routing
// via gen_metadata .1.19/.1.22 (ServedModel, e.g. gemini-4-argon-b vs primary gemini-4-argon).
func EvaluateArgonDegradation(gm *ParsedGenMetadata, modelID string) *ArgonDegradationReport {
	if gm == nil {
		return nil
	}
	rep := &ArgonDegradationReport{
		ServedModel:   gm.ServedModel,
		CwmEstTokens:  gm.CwmEstTokens,
		PromptTokens:  gm.InputTokens + gm.CachedReadTokens,
		CachedTokens:  gm.CachedReadTokens,
		OutputTokens:  gm.OutputTokens,
		ThoughtTokens: gm.ThoughtTokens,
		TtftSeconds:   gm.TtftSeconds,
		StreamSeconds: gm.StreamSeconds,
	}
	isArgon := strings.HasPrefix(strings.ToLower(modelID), "gemini-4-argon") || gm.ModelID == "gemini-4-argon"
	if isArgon && gm.ServedModel != "" && !strings.EqualFold(gm.ServedModel, "gemini-4-argon") {
		rep.Degraded = true
	}
	return rep
}

type payloadSubInfo struct {
	textBytes     int
	thoughtBytes  int
	toolJsonBytes int
	sigBytes      int
	isDshBridge   bool
}

func parseStepPayloadSub(pbuf []byte) payloadSubInfo {
	var info payloadSubInfo
	if len(pbuf) == 0 {
		return info
	}
	pos := 0
	for pos < len(pbuf) {
		tag, next, ok := readProtoVarint(pbuf, pos)
		if !ok {
			break
		}
		pos = next
		fn := tag >> 3
		wt := tag & 7
		if fn == 0 {
			break
		}
		switch wt {
		case 0:
			_, nextV, okV := readProtoVarint(pbuf, pos)
			if !okV {
				return info
			}
			pos = nextV
		case 2:
			ln, nextL, okL := readProtoVarint(pbuf, pos)
			if !okL || int(ln) < 0 || nextL+int(ln) > len(pbuf) {
				return info
			}
			sub := pbuf[nextL : nextL+int(ln)]
			pos = nextL + int(ln)
			if fn == 20 {
				sp := 0
				for sp < len(sub) {
					st, nextSt, okSt := readProtoVarint(sub, sp)
					if !okSt {
						break
					}
					sp = nextSt
					sfn := st >> 3
					swt := st & 7
					switch swt {
					case 2:
						sln, nextSln, okSln := readProtoVarint(sub, sp)
						if !okSln || int(sln) < 0 || nextSln+int(sln) > len(sub) {
							sp = len(sub)
							break
						}
						switch sfn {
						case 1:
							info.textBytes += int(sln)
						case 3:
							info.thoughtBytes += int(sln)
						case 7:
							info.toolJsonBytes += int(sln)
							endSlice := nextSln + int(sln)
							if endSlice > nextSln+180 {
								endSlice = nextSln + 180
							}
							if bytes.Contains(sub[nextSln:endSlice], []byte("mcp_ws-bridge_")) || bytes.Contains(sub[nextSln:endSlice], []byte("mcp_dsh-bridge_")) {
								info.isDshBridge = true
							}
						case 14:
							info.sigBytes += int(sln)
						}
						sp = nextSln + int(sln)
					case 0:
						_, nextSv, okSv := readProtoVarint(sub, sp)
						if !okSv {
							sp = len(sub)
							break
						}
						sp = nextSv
					case 1:
						sp += 8
					case 5:
						sp += 4
					default:
						sp = len(sub)
					}
				}
			}
		case 1:
			pos += 8
		case 5:
			pos += 4
		default:
			return info
		}
	}
	return info
}

func ParseStepMetadataBlob(r4, r9 []byte) *ParsedModelStep {
	pos := 0
	var tStart, tFirst, tEnd float64
	var modelEnum, inTok, outTok, cwTok, crTok, thTok int

	for pos < len(r4) {
		tag, next, ok := readProtoVarint(r4, pos)
		if !ok {
			break
		}
		pos = next
		fn := tag >> 3
		wt := tag & 7
		if fn == 0 {
			break
		}
		switch wt {
		case 0:
			_, nextV, okV := readProtoVarint(r4, pos)
			if !okV {
				pos = len(r4)
				break
			}
			pos = nextV
		case 2:
			ln, nextL, okL := readProtoVarint(r4, pos)
			if !okL || int(ln) < 0 || nextL+int(ln) > len(r4) {
				pos = len(r4)
				break
			}
			sub := r4[nextL : nextL+int(ln)]
			pos = nextL + int(ln)
			if fn == 1 && tStart == 0 {
				tStart = parseProtoTimestamp(sub)
			} else if fn == 6 && tFirst == 0 {
				tFirst = parseProtoTimestamp(sub)
			} else if fn == 7 && tEnd == 0 {
				tEnd = parseProtoTimestamp(sub)
			} else if fn == 9 {
				sp := 0
				for sp < len(sub) {
					st, nextSt, okSt := readProtoVarint(sub, sp)
					if !okSt {
						break
					}
					sp = nextSt
					sfn := st >> 3
					swt := st & 7
					switch swt {
					case 0:
						sv, nextSv, okSv := readProtoVarint(sub, sp)
						if !okSv {
							sp = len(sub)
							break
						}
						sp = nextSv
						switch sfn {
						case 1:
							modelEnum = int(sv)
						case 2:
							inTok = int(sv)
						case 3:
							outTok = int(sv)
						case 4:
							cwTok = int(sv)
						case 5:
							crTok = int(sv)
						case 9:
							thTok = int(sv)
						}
					case 2:
						sln, nextSln, okSln := readProtoVarint(sub, sp)
						if !okSln || int(sln) < 0 || nextSln+int(sln) > len(sub) {
							sp = len(sub)
							break
						}
						sp = nextSln + int(sln)
					case 1:
						sp += 8
					case 5:
						sp += 4
					default:
						sp = len(sub)
					}
				}
			}
		case 1:
			pos += 8
		case 5:
			pos += 4
		default:
			pos = len(r4)
		}
	}

	if inTok == 0 && crTok == 0 && outTok == 0 {
		return nil
	}

	pInfo := parseStepPayloadSub(r9)
	if thTok == 0 && outTok > 0 {
		if pInfo.thoughtBytes > 0 {
			est := int(math.Round(float64(pInfo.thoughtBytes) / 3.2))
			if est < 1 {
				est = 1
			}
			if est > outTok {
				est = outTok
			}
			thTok = est
		} else if pInfo.sigBytes > 0 {
			visEst := int(math.Round(float64(pInfo.textBytes+pInfo.toolJsonBytes) / 3.6))
			if outTok > visEst+8 {
				thTok = outTok - visEst
			}
		}
	}
	hasThinking := thTok > 0 || pInfo.thoughtBytes > 0 || pInfo.sigBytes > 0
	visOutTok := outTok - thTok
	if visOutTok < 0 {
		visOutTok = 0
	}

	rawTtftMs := 0
	if tFirst > tStart && tStart > 0 {
		rawTtftMs = int(math.Round((tFirst - tStart) * 1000))
	}
	rawStreamMs := 0
	if tEnd > tFirst && tFirst > 0 {
		rawStreamMs = int(math.Round((tEnd - tFirst) * 1000))
	}
	rawElapsedMs := 0
	if tEnd > tStart && tStart > 0 {
		rawElapsedMs = int(math.Round((tEnd - tStart) * 1000))
	}
	ttftMs, streamMs, elapsedMs, tps, decodeTps := ComputeStepTiming(rawTtftMs, rawStreamMs, rawElapsedMs, outTok)

	model, tier := protocol.ModelFromEnum(modelEnum)

	alignedIn := inTok
	alignedCr := crTok

	pr := protocol.PriceUsage(model, inTok, crTok, outTok, thTok, cwTok)
	mult := pr.TierMultiplier

	tsSec := int64(tStart)
	if tsSec == 0 {
		tsSec = int64(tEnd)
	}

	return &ParsedModelStep{
		TsSec:          tsSec,
		TtftMs:         ttftMs,
		StreamMs:       streamMs,
		ElapsedMs:      elapsedMs,
		Tps:            tps,
		DecodeTps:      decodeTps,
		ModelEnum:      modelEnum,
		ModelID:        model.ID,
		ModelLabel:     model.Label,
		ModelFamily:    model.Family,
		ModelTier:      tier,
		QuotaGroup:     model.QuotaGroup,
		RawIn:          inTok,
		RawCr:          crTok,
		AlignedIn:      alignedIn,
		AlignedCr:      alignedCr,
		OutTok:         outTok,
		VisOutTok:      visOutTok,
		ThTok:          thTok,
		HasThinking:    hasThinking,
		CwTok:          cwTok,
		TierMultiplier: mult,
		InputCostUsd:   pr.InputCostUsd,
		CacheCostUsd:   pr.CacheCostUsd,
		OutputCostUsd:  pr.OutputCostUsd,
		ThinkCostUsd:   pr.ThinkCostUsd,
		CostUsd:        pr.CostUsd,
		Base1xCostUsd:  pr.Base1xCostUsd,
		NoCacheCostUsd: pr.NoCacheCostUsd,
		SavedUsd:       pr.SavedUsd,
		IsDshBridge:    pInfo.isDshBridge,
	}
}

// Pure-Go SQLite3 B-Tree & WAL Reader (Zero CGO, Zero Subprocesses)
// SQLite varints are big-endian base-128 (up to 9 bytes)
func readSQLiteVarint(buf []byte, pos int) (int64, int, bool) {
	var v int64
	for i := 0; i < 8; i++ {
		if pos >= len(buf) {
			return 0, pos, false
		}
		b := buf[pos]
		pos++
		v = (v << 7) | int64(b&0x7f)
		if (b & 0x80) == 0 {
			return v, pos, true
		}
	}
	if pos >= len(buf) {
		return 0, pos, false
	}
	v = (v << 8) | int64(buf[pos])
	pos++
	return v, pos, true
}

func sqliteSerialTypeLen(st int64) int {
	switch st {
	case 0, 8, 9:
		return 0
	case 1:
		return 1
	case 2:
		return 2
	case 3:
		return 3
	case 4:
		return 4
	case 5:
		return 6
	case 6, 7:
		return 8
	default:
		if st >= 12 {
			return int((st - 12) / 2)
		}
		return 0
	}
}

func readSQLiteInt(buf []byte, st int64) int64 {
	switch st {
	case 0:
		return 0
	case 8:
		return 0
	case 9:
		return 1
	case 1:
		return int64(int8(buf[0]))
	case 2:
		return int64(int16(binary.BigEndian.Uint16(buf)))
	case 3:
		v := int32(buf[0])<<16 | int32(buf[1])<<8 | int32(buf[2])
		if (v & 0x800000) != 0 {
			v |= ^0xffffff
		}
		return int64(v)
	case 4:
		return int64(int32(binary.BigEndian.Uint32(buf)))
	case 5:
		v := int64(buf[0])<<40 | int64(buf[1])<<32 | int64(buf[2])<<24 | int64(buf[3])<<16 | int64(buf[4])<<8 | int64(buf[5])
		if (v & 0x800000000000) != 0 {
			v |= ^0xffffffffffff
		}
		return v
	case 6:
		return int64(binary.BigEndian.Uint64(buf))
	default:
		return 0
	}
}

type SQLiteImage struct {
	data     []byte
	pageSize int
	reserved int
	usable   int
	walPages map[uint32][]byte
}

func (img *SQLiteImage) HasWALPage(pgNo uint32) bool {
	if img == nil || img.walPages == nil {
		return false
	}
	_, ok := img.walPages[pgNo]
	return ok
}

func OpenSQLiteImage(dbPath string) (*SQLiteImage, error) {
	data, err := os.ReadFile(dbPath)
	if err != nil {
		return nil, err
	}
	if len(data) < 100 {
		return nil, os.ErrInvalid
	}
	if string(data[:16]) != "SQLite format 3\x00" {
		return nil, os.ErrInvalid
	}
	ps := int(binary.BigEndian.Uint16(data[16:18]))
	if ps == 1 {
		ps = 65536
	}
	if ps < 512 {
		return nil, os.ErrInvalid
	}
	res := int(data[20])
	usable := ps - res

	img := &SQLiteImage{
		data:     data,
		pageSize: ps,
		reserved: res,
		usable:   usable,
	}

	// Apply committed WAL frames from the current WAL generation (matching header magic, DB page
	// size & 8-byte salt, committed only at frames with dbSize > 0) for local files.
	if !strings.HasPrefix(dbPath, "/mnt/c/") {
		walData, errWal := os.ReadFile(dbPath + "-wal")
		if errWal == nil && len(walData) >= 32+24+ps {
			magic := binary.BigEndian.Uint32(walData[0:4])
			walPs := int(binary.BigEndian.Uint32(walData[8:12]))
			if walPs == 1 {
				walPs = 65536
			}
			if (magic == 0x377f0682 || magic == 0x377f0683) && walPs == ps {
				hdrSalt := walData[16:24]
				img.walPages = make(map[uint32][]byte)
				txPages := make(map[uint32][]byte)
				frameSize := 24 + ps
				for off := 32; off+frameSize <= len(walData); off += frameSize {
					if !bytes.Equal(walData[off+8:off+16], hdrSalt) {
						break
					}
					pgNo := binary.BigEndian.Uint32(walData[off : off+4])
					dbSizeAfter := binary.BigEndian.Uint32(walData[off+4 : off+8])
					if pgNo > 0 {
						txPages[pgNo] = walData[off+24 : off+frameSize]
					}
					if dbSizeAfter > 0 {
						for k, v := range txPages {
							if k <= dbSizeAfter {
								img.walPages[k] = v
							}
						}
						for k := range img.walPages {
							if k > dbSizeAfter {
								delete(img.walPages, k)
							}
						}
						clear(txPages)
					}
				}
			}
		}
	}
	return img, nil
}

func (img *SQLiteImage) getPage(pgNo uint32) []byte {
	if pgNo == 0 {
		return nil
	}
	if img.walPages != nil {
		if p, ok := img.walPages[pgNo]; ok {
			return p
		}
	}
	start := int(pgNo-1) * img.pageSize
	if start < 0 || start+img.pageSize > len(img.data) {
		return nil
	}
	return img.data[start : start+img.pageSize]
}

func (img *SQLiteImage) readCellPayload(cell []byte, payloadSize int) []byte {
	u := img.usable
	x := u - 35
	minLocal := ((u - 12) * 32 / 255) - 23
	maxLocal := x
	localSize := payloadSize
	if payloadSize > maxLocal {
		k := minLocal + ((payloadSize - minLocal) % (u - 4))
		if k <= maxLocal {
			localSize = k
		} else {
			localSize = minLocal
		}
	}
	if localSize > len(cell) {
		return nil
	}
	if localSize == payloadSize {
		return cell[:localSize]
	}
	if localSize+4 > len(cell) {
		return nil
	}
	out := make([]byte, 0, payloadSize)
	out = append(out, cell[:localSize]...)
	nextPg := binary.BigEndian.Uint32(cell[localSize : localSize+4])
	rem := payloadSize - localSize
	maxHops := (payloadSize / (u - 4)) + 4
	for nextPg > 0 && rem > 0 && maxHops > 0 {
		maxHops--
		pg := img.getPage(nextPg)
		if len(pg) < 4 {
			break
		}
		nextPg = binary.BigEndian.Uint32(pg[0:4])
		chunkCap := u - 4
		if chunkCap > len(pg)-4 {
			chunkCap = len(pg) - 4
		}
		take := rem
		if take > chunkCap {
			take = chunkCap
		}
		out = append(out, pg[4:4+take]...)
		rem -= take
	}
	return out
}

func (img *SQLiteImage) findTableRootPage(tableName string) uint32 {
	var found uint32
	img.walkTableBTree(1, 0, func(_ int64, cell []byte, pSize int) {
		if found != 0 {
			return
		}
		rec := img.readCellPayload(cell, pSize)
		if len(rec) < 4 {
			return
		}
		hdrLen, pos, ok := readSQLiteVarint(rec, 0)
		if !ok || int(hdrLen) > len(rec) {
			return
		}
		var sts [5]int64
		for i := 0; i < 5 && pos < int(hdrLen); i++ {
			st, next, okS := readSQLiteVarint(rec, pos)
			if !okS {
				return
			}
			sts[i] = st
			pos = next
		}
		dpos := int(hdrLen)
		// col 0: type ("table"), col 1: name, col 2: tbl_name, col 3: rootpage
		l0 := sqliteSerialTypeLen(sts[0])
		l1 := sqliteSerialTypeLen(sts[1])
		l2 := sqliteSerialTypeLen(sts[2])
		l3 := sqliteSerialTypeLen(sts[3])
		if dpos+l0+l1+l2+l3 > len(rec) {
			return
		}
		typeBytes := rec[dpos : dpos+l0]
		nameBytes := rec[dpos+l0 : dpos+l0+l1]
		if string(typeBytes) == "table" && string(nameBytes) == tableName {
			rp := readSQLiteInt(rec[dpos+l0+l1+l2:dpos+l0+l1+l2+l3], sts[3])
			if rp > 0 {
				found = uint32(rp)
			}
		}
	})
	return found
}

func (img *SQLiteImage) findStepsRootPage() uint32 {
	return img.findTableRootPage("steps")
}

func (img *SQLiteImage) walkTableBTree(pgNo uint32, depth int, onCell func(rowID int64, cellBody []byte, payloadSize int)) {
	if pgNo == 0 || depth > 16 {
		return
	}
	pg := img.getPage(pgNo)
	if len(pg) == 0 {
		return
	}
	hdrOff := 0
	if pgNo == 1 {
		hdrOff = 100
	}
	if hdrOff+8 > len(pg) {
		return
	}
	pgType := pg[hdrOff]
	numCells := int(binary.BigEndian.Uint16(pg[hdrOff+3 : hdrOff+5]))
	switch pgType {
	case 0x05: // Interior table b-tree page
		if hdrOff+12 > len(pg) {
			return
		}
		rightChild := binary.BigEndian.Uint32(pg[hdrOff+8 : hdrOff+12])
		ptrBase := hdrOff + 12
		for i := 0; i < numCells; i++ {
			if ptrBase+i*2+2 > len(pg) {
				break
			}
			cellOff := int(binary.BigEndian.Uint16(pg[ptrBase+i*2 : ptrBase+i*2+2]))
			if cellOff+4 > len(pg) {
				continue
			}
			leftChild := binary.BigEndian.Uint32(pg[cellOff : cellOff+4])
			img.walkTableBTree(leftChild, depth+1, onCell)
		}
		img.walkTableBTree(rightChild, depth+1, onCell)
	case 0x0d: // Leaf table b-tree page
		ptrBase := hdrOff + 8
		for i := 0; i < numCells; i++ {
			if ptrBase+i*2+2 > len(pg) {
				break
			}
			cellOff := int(binary.BigEndian.Uint16(pg[ptrBase+i*2 : ptrBase+i*2+2]))
			if cellOff >= len(pg) {
				continue
			}
			pSize, next1, ok1 := readSQLiteVarint(pg, cellOff)
			if !ok1 || pSize <= 0 {
				continue
			}
			rowID, next2, ok2 := readSQLiteVarint(pg, next1)
			if !ok2 || next2 > len(pg) {
				continue
			}
			onCell(rowID, pg[next2:], int(pSize))
		}
	}
}

func ClassifyStepSource(sourceSide, defaultCat, defaultLabel string, isGateway bool) (side, cat, label string) {
	side = sourceSide
	if side == "" {
		side = "HOST"
	}
	if side == "WIN" {
		return "WIN", "WIN_HOST", "Win 宿主机"
	}
	if isGateway {
		return side, "CLAUDE_GW", "ACP Gateway"
	}
	if side == "APP" || defaultCat == "AGY_APP" {
		if defaultLabel == "" {
			defaultLabel = "Antigravity App"
		}
		return "APP", "AGY_APP", defaultLabel
	}
	if defaultCat == "" {
		defaultCat = "AGY_CLI"
	}
	if defaultLabel == "" {
		defaultLabel = "AGY CLI"
	}
	return side, defaultCat, defaultLabel
}

func ReadGenMetaIndexFromImage(img *SQLiteImage) (byStep map[int]GenStepMeta, byTok map[[3]int]GenStepMeta) {
	if img == nil {
		return nil, nil
	}
	rootPg := img.findTableRootPage("gen_metadata")
	if rootPg == 0 {
		return nil, nil
	}
	byStep = map[int]GenStepMeta{}
	byTok = map[[3]int]GenStepMeta{}
	img.walkTableBTree(rootPg, 0, func(_ int64, cellBody []byte, pSize int) {
		hdrLen, pos, ok := readSQLiteVarint(cellBody, 0)
		if !ok || hdrLen <= 0 || int(hdrLen) > len(cellBody) {
			return
		}
		var sts [4]int64
		colCount := 0
		for colCount < 4 && pos < int(hdrLen) {
			sv, next, okS := readSQLiteVarint(cellBody, pos)
			if !okS {
				break
			}
			sts[colCount] = sv
			colCount++
			pos = next
		}
		if colCount < 2 {
			return
		}
		l0 := sqliteSerialTypeLen(sts[0])
		l1 := sqliteSerialTypeLen(sts[1])
		if l1 <= 0 {
			return
		}
		dpos := int(hdrLen)
		fullRec := img.readCellPayload(cellBody, pSize)
		if len(fullRec) < dpos+l0+l1 {
			return
		}
		gm := ParseGenMetadataBlob(fullRec[dpos+l0 : dpos+l0+l1])
		if gm == nil {
			return
		}
		meta := GenStepMeta{
			ServedModel: gm.ServedModel,
			TtftMs:      int(math.Round(gm.TtftSeconds * 1000)),
			StreamMs:    int(math.Round(gm.StreamSeconds * 1000)),
		}
		if meta.ServedModel == "" && meta.TtftMs == 0 && meta.StreamMs == 0 {
			return
		}
		for _, si := range gm.StepIndices {
			prev := byStep[si]
			if meta.ServedModel == "" {
				meta.ServedModel = prev.ServedModel
			}
			byStep[si] = meta
		}
		tokKey := [3]int{gm.InputTokens, gm.CachedReadTokens, gm.OutputTokens}
		if prev, ok := byTok[tokKey]; ok && meta.ServedModel == "" {
			meta.ServedModel = prev.ServedModel
		}
		byTok[tokKey] = meta
	})
	return byStep, byTok
}

func parseParentRefForkStepIdx(blob []byte) int {
	if len(blob) == 0 {
		return -1
	}
	pos := 0
	hasRef := false
	forkIdx := -1
	for pos < len(blob) {
		tag, next, ok := readProtoVarint(blob, pos)
		if !ok {
			break
		}
		fn := tag >> 3
		wt := tag & 7
		if fn == 0 {
			break
		}
		pos = next
		switch wt {
		case 0:
			v, nextV, okV := readProtoVarint(blob, pos)
			if !okV {
				return forkIdx
			}
			pos = nextV
			if fn == 2 {
				forkIdx = int(v)
				hasRef = true
			}
		case 2:
			ln, nextL, okL := readProtoVarint(blob, pos)
			if !okL || int(ln) < 0 || nextL+int(ln) > len(blob) {
				return forkIdx
			}
			sub := blob[nextL : nextL+int(ln)]
			pos = nextL + int(ln)
			if (fn == 1 || fn == 6) && len(sub) > 0 {
				hasRef = true
			}
		case 1:
			if pos+8 > len(blob) {
				return forkIdx
			}
			pos += 8
		case 5:
			if pos+4 > len(blob) {
				return forkIdx
			}
			pos += 4
		default:
			return forkIdx
		}
	}
	if forkIdx >= 0 {
		return forkIdx
	}
	if hasRef {
		return 0
	}
	return -1
}

// readMaxForkStepIdxFromImage inspects parent_references (present on Antigravity Desktop App
// forked child conversations) using the pure in-memory SQLite page/WAL reader and returns the
// highest pre-fork step index cloned from a parent trajectory, or -1 when un-forked.
func readMaxForkStepIdxFromImage(img *SQLiteImage) int {
	if img == nil {
		return -1
	}
	rootPg := img.findTableRootPage("parent_references")
	if rootPg == 0 {
		return -1
	}
	maxForkIdx := -1
	img.walkTableBTree(rootPg, 0, func(_ int64, cellBody []byte, pSize int) {
		hdrLen, pos, ok := readSQLiteVarint(cellBody, 0)
		if !ok || hdrLen <= 0 || int(hdrLen) > len(cellBody) {
			return
		}
		var sts [4]int64
		colCount := 0
		for colCount < len(sts) && pos < int(hdrLen) {
			sv, next, okS := readSQLiteVarint(cellBody, pos)
			if !okS {
				break
			}
			sts[colCount] = sv
			colCount++
			pos = next
		}
		if colCount < 2 {
			return
		}
		l0 := sqliteSerialTypeLen(sts[0])
		l1 := sqliteSerialTypeLen(sts[1])
		if l1 <= 0 {
			return
		}
		dpos := int(hdrLen)
		fullRec := img.readCellPayload(cellBody, pSize)
		if len(fullRec) < dpos+l0+l1 {
			return
		}
		if fk := parseParentRefForkStepIdx(fullRec[dpos+l0 : dpos+l0+l1]); fk > maxForkIdx {
			maxForkIdx = fk
		}
	})
	return maxForkIdx
}

func ReadMaxForkStepIdxFromSQLite(dbPath string) int {
	img, err := OpenSQLiteImage(dbPath)
	if err != nil {
		return -1
	}
	return readMaxForkStepIdxFromImage(img)
}

func ReadModelStepsFromSQLiteAfterInternal(dbPath, sourceSide string, gwSids map[string]bool, afterIdx int) ([]ParsedModelStep, int, bool) {
	st, err := os.Stat(dbPath)
	if err != nil || st.Size() < 100 {
		return nil, -1, false
	}
	sessionID := strings.ToLower(strings.TrimSuffix(filepath.Base(dbPath), ".db"))
	isKnownGw := gwSids[sessionID]

	img, err := OpenSQLiteImage(dbPath)
	if err != nil {
		return nil, -1, false
	}
	maxForkIdx := -1
	effectiveAfter := afterIdx
	if sourceSide == "APP" {
		maxForkIdx = readMaxForkStepIdxFromImage(img)
		if maxForkIdx > effectiveAfter {
			effectiveAfter = maxForkIdx
		}
	}
	rootPg := img.findStepsRootPage()
	if rootPg == 0 {
		return nil, maxForkIdx, true
	}

	var rawSteps []ParsedModelStep
	hasAnyDshBridge := isKnownGw
	fallbackTs := st.ModTime().Unix()

	img.walkTableBTree(rootPg, 0, func(rowID int64, cellBody []byte, pSize int) {
		// Fast inspect of record header from local cell slice BEFORE reading overflow pages!
		hdrLen, pos, ok := readSQLiteVarint(cellBody, 0)
		if !ok || hdrLen <= 0 || int(hdrLen) > len(cellBody) {
			return
		}
		var sts [16]int64
		colCount := 0
		for colCount < len(sts) && pos < int(hdrLen) {
			sv, next, okS := readSQLiteVarint(cellBody, pos)
			if !okS {
				break
			}
			sts[colCount] = sv
			colCount++
			pos = next
		}
		if colCount < 5 {
			return
		}
		// Check col 0 (idx) and col 1 (step_type) in local cellBody!
		l0 := sqliteSerialTypeLen(sts[0])
		l1 := sqliteSerialTypeLen(sts[1])
		dpos := int(hdrLen)
		if dpos+l0+l1 > len(cellBody) {
			return
		}
		stepType := readSQLiteInt(cellBody[dpos+l0:dpos+l0+l1], sts[1])
		if stepType != 15 {
			// Skip non-model steps without touching overflow pages!
			return
		}
		stepIdx := int(rowID)
		if sts[0] != 0 {
			stepIdx = int(readSQLiteInt(cellBody[dpos:dpos+l0], sts[0]))
		}
		if stepIdx <= effectiveAfter && (hasAnyDshBridge || sourceSide == "APP") {
			return
		}

		// Now read full payload (including overflow pages if any) for step_type == 15
		fullRec := img.readCellPayload(cellBody, pSize)
		if len(fullRec) < dpos+l0+l1 {
			return
		}
		curOff := dpos
		var metaSlice, payloadSlice []byte
		for c := 0; c < colCount; c++ {
			clen := sqliteSerialTypeLen(sts[c])
			if curOff+clen > len(fullRec) {
				break
			}
			if c == 4 && clen > 0 {
				metaSlice = fullRec[curOff : curOff+clen]
			} else if c == 9 && clen > 0 {
				payloadSlice = fullRec[curOff : curOff+clen]
			}
			curOff += clen
		}
		if len(metaSlice) == 0 {
			return
		}
		parsed := ParseStepMetadataBlob(metaSlice, payloadSlice)
		if parsed == nil {
			return
		}
		if parsed.IsDshBridge {
			hasAnyDshBridge = true
		}
		if stepIdx <= effectiveAfter {
			return
		}
		if parsed.TsSec == 0 {
			parsed.TsSec = fallbackTs
		}
		parsed.SessionID = sessionID
		parsed.StepIdx = stepIdx
		rawSteps = append(rawSteps, *parsed)
	})

	if len(rawSteps) > 0 {
		byStep, byTok := ReadGenMetaIndexFromImage(img)
		applyGenMetaIndex(rawSteps, byStep, byTok)
	}
	effSide, sourceCat, sourceLbl := ClassifyStepSource(sourceSide, "", "", hasAnyDshBridge)
	for i := range rawSteps {
		rawSteps[i].SourceSide = effSide
		rawSteps[i].SourceCategory = sourceCat
		rawSteps[i].SourceLabel = sourceLbl
	}
	return rawSteps, maxForkIdx, true
}

func ReadModelStepsFromSQLiteAfter(dbPath, sourceSide string, gwSids map[string]bool, afterIdx int) ([]ParsedModelStep, bool) {
	steps, _, ok := ReadModelStepsFromSQLiteAfterInternal(dbPath, sourceSide, gwSids, afterIdx)
	return steps, ok
}

func ReadModelStepsFromSQLite(dbPath, sourceSide string, gwSids map[string]bool) []ParsedModelStep {
	steps, _ := ReadModelStepsFromSQLiteAfter(dbPath, sourceSide, gwSids, -1)
	return steps
}

func parseCortexStepProto(stepBuf []byte) (stepType int, metaSlice []byte, ok bool) {
	pos := 0
	for pos < len(stepBuf) {
		tag, next, okTag := readProtoVarint(stepBuf, pos)
		if !okTag {
			return 0, nil, false
		}
		fn := tag >> 3
		wt := tag & 7
		if fn == 0 || fn > 536870911 {
			return 0, nil, false
		}
		pos = next
		switch wt {
		case 0:
			v, nextV, okV := readProtoVarint(stepBuf, pos)
			if !okV {
				return 0, nil, false
			}
			pos = nextV
			if fn == 1 {
				stepType = int(v)
			}
		case 1:
			if pos+8 > len(stepBuf) {
				return 0, nil, false
			}
			pos += 8
		case 5:
			if pos+4 > len(stepBuf) {
				return 0, nil, false
			}
			pos += 4
		case 2:
			ln, nextL, okL := readProtoVarint(stepBuf, pos)
			if !okL || int(ln) < 0 || nextL+int(ln) > len(stepBuf) {
				return 0, nil, false
			}
			if fn == 4 && len(metaSlice) == 0 {
				metaSlice = stepBuf[nextL : nextL+int(ln)]
			}
			pos = nextL + int(ln)
		default:
			return 0, nil, false
		}
	}
	return stepType, metaSlice, pos == len(stepBuf)
}

// ReadModelStepsFromProtobuf parses unencrypted legacy <uuid>.pb CortexTrajectory files strictly
// read-only. Encrypted/non-protobuf .pb blobs return (nil, true) so callers advance the cursor
// without re-reading or emitting spurious sync errors.
func ReadModelStepsFromProtobuf(pbPath, sourceSide, defaultCat, defaultLabel string, gwSids map[string]bool, afterIdx int, fallbackTsSec int64) ([]ParsedModelStep, bool) {
	data, err := os.ReadFile(pbPath)
	if err != nil {
		return nil, false
	}
	if len(data) == 0 {
		return nil, true
	}
	sessionID := strings.ToLower(strings.TrimSuffix(filepath.Base(pbPath), ".pb"))
	hasAnyDsh := gwSids[sessionID]
	if fallbackTsSec == 0 {
		if st, errS := os.Stat(pbPath); errS == nil {
			fallbackTsSec = st.ModTime().Unix()
		}
	}

	var rawSteps []ParsedModelStep
	pos := 0
	stepSeq := 0
	for pos < len(data) {
		tag, next, okTag := readProtoVarint(data, pos)
		if !okTag {
			return nil, true
		}
		fn := tag >> 3
		wt := tag & 7
		if fn == 0 || fn > 536870911 {
			return nil, true
		}
		pos = next
		switch wt {
		case 0:
			_, nextV, okV := readProtoVarint(data, pos)
			if !okV {
				return nil, true
			}
			pos = nextV
		case 1:
			if pos+8 > len(data) {
				return nil, true
			}
			pos += 8
		case 5:
			if pos+4 > len(data) {
				return nil, true
			}
			pos += 4
		case 2:
			ln, nextL, okL := readProtoVarint(data, pos)
			if !okL || int(ln) < 0 || nextL+int(ln) > len(data) {
				return nil, true
			}
			sub := data[nextL : nextL+int(ln)]
			pos = nextL + int(ln)
			if fn == 2 {
				idx := stepSeq
				stepSeq++
				stType, metaSlice, validStep := parseCortexStepProto(sub)
				if !validStep || stType != 15 || len(metaSlice) == 0 {
					continue
				}
				parsed := ParseStepMetadataBlob(metaSlice, sub)
				if parsed == nil {
					continue
				}
				if parsed.IsDshBridge {
					hasAnyDsh = true
				}
				if idx <= afterIdx {
					continue
				}
				if parsed.TsSec == 0 {
					parsed.TsSec = fallbackTsSec
				}
				parsed.SessionID = sessionID
				parsed.StepIdx = idx
				rawSteps = append(rawSteps, *parsed)
			}
		default:
			return nil, true
		}
	}

	effSide, srcCat, srcLbl := ClassifyStepSource(sourceSide, defaultCat, defaultLabel, hasAnyDsh)
	for i := range rawSteps {
		rawSteps[i].SourceSide = effSide
		rawSteps[i].SourceCategory = srcCat
		rawSteps[i].SourceLabel = srcLbl
	}
	return rawSteps, true
}

// Hot/Cold Incremental Cache for Dashboard & Gateway Step Lookup
func SQLiteFileURI(dbPath, rawQuery string) string {
	p := dbPath
	if abs, err := filepath.Abs(dbPath); err == nil && abs != "" {
		p = abs
	}
	slash := filepath.ToSlash(p)
	if !strings.HasPrefix(slash, "/") {
		slash = "/" + slash
	}
	u := url.URL{
		Scheme:   "file",
		Path:     slash,
		OmitHost: true,
		RawQuery: rawQuery,
	}
	return u.String()
}

func OpenReadOnlySQLite(dbPath string) (*sql.DB, error) {
	dsn := SQLiteFileURI(dbPath, "mode=ro&_pragma=busy_timeout(100)&_pragma=query_only(1)")
	return sql.Open("sqlite", dsn)
}

func QueryModelStepsFromDB(db *sql.DB, sessionID, sourceSide string, gwSids map[string]bool, afterIdx int, fallbackTsSec int64) ([]ParsedModelStep, bool) {
	if db == nil {
		return nil, false
	}
	hasAnyDsh := gwSids[sessionID]

	rows, err := db.Query("SELECT idx, metadata, step_payload FROM steps WHERE step_type = 15 AND idx > ? ORDER BY idx ASC", afterIdx)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such table: steps") {
			return nil, true
		}
		return nil, false
	}
	defer rows.Close()

	var rawSteps []ParsedModelStep
	for rows.Next() {
		var idx int
		var meta, payload []byte
		if err := rows.Scan(&idx, &meta, &payload); err != nil || len(meta) == 0 {
			continue
		}
		parsed := ParseStepMetadataBlob(meta, payload)
		if parsed == nil {
			continue
		}
		parsed.SessionID = sessionID
		parsed.SourceSide = sourceSide
		parsed.StepIdx = idx
		if parsed.TsSec == 0 {
			parsed.TsSec = fallbackTsSec
		}
		if parsed.IsDshBridge {
			hasAnyDsh = true
		}
		rawSteps = append(rawSteps, *parsed)
	}
	if err := rows.Err(); err != nil {
		return nil, false
	}
	attachServedModels(db, rawSteps)

	effSide, srcCat, srcLabel := ClassifyStepSource(sourceSide, "", "", hasAnyDsh)
	for i := range rawSteps {
		rawSteps[i].SourceSide = effSide
		rawSteps[i].SourceCategory = srcCat
		rawSteps[i].SourceLabel = srcLabel
	}
	return rawSteps, true
}

// QueryStepTierAndMaxIdxAfterDB reads only steps(idx, metadata) for step_type=15 after afterIdx
// to extract the latest ModelTier and max step idx without fetching step_payload or scanning gen_metadata.
func QueryStepTierAndMaxIdxAfterDB(db *sql.DB, afterIdx int) (actualTier string, maxIdx int, ok bool) {
	maxIdx = afterIdx
	if db == nil {
		return "", maxIdx, false
	}
	rows, err := db.Query("SELECT idx, metadata FROM steps WHERE step_type = 15 AND idx > ? ORDER BY idx ASC", afterIdx)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such table: steps") {
			return "", maxIdx, true
		}
		return "", maxIdx, false
	}
	defer rows.Close()
	for rows.Next() {
		var idx int
		var meta []byte
		if err := rows.Scan(&idx, &meta); err != nil || len(meta) == 0 {
			continue
		}
		if parsed := ParseStepMetadataBlob(meta, nil); parsed != nil {
			if idx > maxIdx {
				maxIdx = idx
			}
			if parsed.ModelTier != "" {
				actualTier = parsed.ModelTier
			}
		}
	}
	if err := rows.Err(); err != nil {
		return "", maxIdx, false
	}
	return actualTier, maxIdx, true
}

// ReadSessionStepTierAndMaxIdxAfter opens the session DB read-only and returns latest ModelTier and max step idx.
func ReadSessionStepTierAndMaxIdxAfter(convDir, sessionID string, afterIdx int) (actualTier string, maxIdx int, ok bool) {
	maxIdx = afterIdx
	if convDir == "" || sessionID == "" {
		return "", maxIdx, false
	}
	dbPath := filepath.Join(convDir, sessionID+".db")
	db, err := OpenReadOnlySQLite(dbPath)
	if err != nil {
		return "", maxIdx, false
	}
	defer db.Close()
	return QueryStepTierAndMaxIdxAfterDB(db, afterIdx)
}

func ReadModelStepsSQL(dbPath, sourceSide string, gwSids map[string]bool, afterIdx int, fallbackTsSec int64) ([]ParsedModelStep, bool) {
	if sourceSide == "APP" {
		return ReadModelStepsFromSQLiteAfter(dbPath, sourceSide, gwSids, afterIdx)
	}
	db, err := OpenReadOnlySQLite(dbPath)
	if err != nil {
		return nil, false
	}
	defer db.Close()

	sessionID := strings.ToLower(strings.TrimSuffix(filepath.Base(dbPath), ".db"))
	return QueryModelStepsFromDB(db, sessionID, sourceSide, gwSids, afterIdx, fallbackTsSec)
}

// maxPlausibleDecodeTps caps post-TTFT streaming decode speed so server-buffered tool-call or
// thought-summary bursts (e.g. 3,000 JSON tokens emitted across a 0.3s tail after 31s buffering)
// are never misreported as multi-thousand tok/s decode rates, while preserving genuine fast Gemini
// 3.8 Flash / 4 Hydrogen / Carbon streaming (120-950 tok/s).
const maxPlausibleDecodeTps = 1200.0

// ComputeStepTiming normalizes upstream or turn durations and returns both end-to-end throughput
// (e2eTps = outTok / elapsedSec, aligned with downstream proxies like GPT-Load) and post-TTFT
// streaming decode throughput (decodeTps = outTok / streamSec, aligned with Agy native speed).
func ComputeStepTiming(ttftMs, streamMs, elapsedMs, outTok int) (normTtftMs, normStreamMs, normElapsedMs int, e2eTps, decodeTps float64) {
	normTtftMs = ttftMs
	if normTtftMs > 0 && normTtftMs < 10 {
		normTtftMs = 10
	}
	normStreamMs = streamMs
	if normStreamMs < 0 {
		normStreamMs = 0
	}
	normElapsedMs = elapsedMs
	if normElapsedMs <= 0 && (normTtftMs > 0 || normStreamMs > 0) {
		normElapsedMs = normTtftMs + normStreamMs
	}
	if normElapsedMs > 0 && normElapsedMs < normTtftMs {
		normElapsedMs = normTtftMs
	}
	if normStreamMs == 0 && normTtftMs > 0 && normElapsedMs > normTtftMs {
		normStreamMs = normElapsedMs - normTtftMs
	}
	if normElapsedMs > 0 && outTok > 0 {
		e2eTps = math.Round((float64(outTok)/(float64(normElapsedMs)/1000.0))*10) / 10
	}
	if outTok > 0 && normStreamMs >= 400 {
		rawDec := float64(outTok) / (float64(normStreamMs) / 1000.0)
		if rawDec > 0 && rawDec <= maxPlausibleDecodeTps && (normStreamMs <= 30000 || rawDec >= 8.0) {
			decodeTps = math.Round(rawDec*10) / 10
		}
	}
	return normTtftMs, normStreamMs, normElapsedMs, e2eTps, decodeTps
}

type GenStepMeta struct {
	ServedModel string
	TtftMs      int
	StreamMs    int
}

func applyGenStepMeta(step *ParsedModelStep, meta GenStepMeta) {
	if meta.ServedModel != "" {
		step.ServedModel = meta.ServedModel
	}
	if meta.TtftMs > 0 || meta.StreamMs > 0 {
		ttft := meta.TtftMs
		if ttft <= 0 {
			ttft = step.TtftMs
		}
		strm := meta.StreamMs
		elap := step.ElapsedMs
		if meta.TtftMs > 0 && meta.StreamMs > 0 {
			elap = meta.TtftMs + meta.StreamMs
		}
		step.TtftMs, step.StreamMs, step.ElapsedMs, step.Tps, step.DecodeTps = ComputeStepTiming(ttft, strm, elap, step.OutTok)
	}
}

func applyGenMetaIndex(steps []ParsedModelStep, byStep map[int]GenStepMeta, byTok map[[3]int]GenStepMeta) {
	if len(steps) == 0 || (len(byStep) == 0 && len(byTok) == 0) {
		return
	}
	for i := range steps {
		if m, ok := byStep[steps[i].StepIdx]; ok {
			applyGenStepMeta(&steps[i], m)
		} else if m, ok := byTok[[3]int{steps[i].RawIn, steps[i].RawCr, steps[i].OutTok}]; ok {
			applyGenStepMeta(&steps[i], m)
		}
	}
}

// attachServedModels copies the upstream serving model (gen_metadata .1.22/.1.19) and authoritative
// generation durations (.1.11 ttft, .1.12 stream) onto steps, matching primarily by
// CortexStepGeneratorMetadata.step_indices (.2) and falling back to raw token split.
func attachServedModels(db *sql.DB, steps []ParsedModelStep) {
	if len(steps) == 0 {
		return
	}
	byStep, byTok := readSessionGenMetaIndex(db)
	applyGenMetaIndex(steps, byStep, byTok)
}

func readSessionGenMetaIndex(db *sql.DB) (byStep map[int]GenStepMeta, byTok map[[3]int]GenStepMeta) {
	q, err := db.Query("SELECT data FROM gen_metadata ORDER BY idx ASC")
	if err != nil {
		return nil, nil
	}
	defer q.Close()
	byStep = map[int]GenStepMeta{}
	byTok = map[[3]int]GenStepMeta{}
	for q.Next() {
		var data []byte
		if q.Scan(&data) != nil {
			continue
		}
		gm := ParseGenMetadataBlob(data)
		if gm == nil {
			continue
		}
		meta := GenStepMeta{
			ServedModel: gm.ServedModel,
			TtftMs:      int(math.Round(gm.TtftSeconds * 1000)),
			StreamMs:    int(math.Round(gm.StreamSeconds * 1000)),
		}
		if meta.ServedModel == "" && meta.TtftMs == 0 && meta.StreamMs == 0 {
			continue
		}
		for _, si := range gm.StepIndices {
			prev := byStep[si]
			if meta.ServedModel == "" {
				meta.ServedModel = prev.ServedModel
			}
			byStep[si] = meta
		}
		tokKey := [3]int{gm.InputTokens, gm.CachedReadTokens, gm.OutputTokens}
		if prev, ok := byTok[tokKey]; ok && meta.ServedModel == "" {
			meta.ServedModel = prev.ServedModel
		}
		byTok[tokKey] = meta
	}
	return byStep, byTok
}

func ReadSessionServedModels(db *sql.DB) (byStep map[int]string, byTok map[[3]int]string) {
	mStep, mTok := readSessionGenMetaIndex(db)
	if len(mStep) == 0 && len(mTok) == 0 {
		return nil, nil
	}
	byStep = map[int]string{}
	byTok = map[[3]int]string{}
	for k, v := range mStep {
		if v.ServedModel != "" {
			byStep[k] = v.ServedModel
		}
	}
	for k, v := range mTok {
		if v.ServedModel != "" {
			byTok[k] = v.ServedModel
		}
	}
	return byStep, byTok
}

func ReadLatestStepFromSQLiteFast(dbPath string) *ParsedModelStep {
	st, err := os.Stat(dbPath)
	if err != nil || st.Size() < 4096 {
		return nil
	}
	db, err := OpenReadOnlySQLite(dbPath)
	if err == nil {
		defer db.Close()
		rows, qErr := db.Query("SELECT idx, metadata, step_payload FROM steps WHERE step_type = 15 ORDER BY idx DESC LIMIT 8")
		if qErr == nil {
			sessionID := strings.ToLower(strings.TrimSuffix(filepath.Base(dbPath), ".db"))
			var found *ParsedModelStep
			for rows.Next() {
				var idx int
				var meta, payload []byte
				if rows.Scan(&idx, &meta, &payload) == nil && len(meta) > 0 {
					if parsed := ParseStepMetadataBlob(meta, payload); parsed != nil {
						parsed.SessionID = sessionID
						parsed.SourceSide = "WSL"
						parsed.StepIdx = idx
						if parsed.TsSec == 0 {
							parsed.TsSec = st.ModTime().Unix()
						}
						found = parsed
						break
					}
				}
			}
			_ = rows.Close()
			if found != nil {
				tmp := []ParsedModelStep{*found}
				attachServedModels(db, tmp)
				return &tmp[0]
			}
		}
	}
	sessionID := strings.ToLower(strings.TrimSuffix(filepath.Base(dbPath), ".db"))
	gwMap := map[string]bool{sessionID: true}
	steps := ReadModelStepsFromSQLite(dbPath, "WSL", gwMap)
	if len(steps) == 0 {
		return nil
	}
	last := steps[len(steps)-1]
	return &last
}

func QueryMaxGenMetadataIdxDB(db *sql.DB) int {
	if db == nil {
		return -1
	}
	var maxIdx int = -1
	_ = db.QueryRow("SELECT COALESCE(MAX(idx), -1) FROM gen_metadata").Scan(&maxIdx)
	return maxIdx
}

func ReadMaxSessionGenMetadataIdx(convDir, sessionID string) int {
	if sessionID == "" {
		return -1
	}
	dbPath := filepath.Join(convDir, sessionID+".db")
	db, err := OpenReadOnlySQLite(dbPath)
	if err != nil {
		return -1
	}
	defer db.Close()
	return QueryMaxGenMetadataIdxDB(db)
}

func QueryMaxSessionStepIdxDB(db *sql.DB) int {
	if db == nil {
		return -1
	}
	var maxIdx int = -1
	_ = db.QueryRow("SELECT COALESCE(MAX(idx), -1) FROM steps WHERE step_type = 15").Scan(&maxIdx)
	return maxIdx
}

func ReadMaxSessionStepIdx(convDir, sessionID string) int {
	if sessionID == "" {
		return -1
	}
	dbPath := filepath.Join(convDir, sessionID+".db")
	db, err := OpenReadOnlySQLite(dbPath)
	if err != nil {
		return -1
	}
	defer db.Close()
	return QueryMaxSessionStepIdxDB(db)
}

func QuerySessionGenMetadataAfterDB(db *sql.DB, sessionID string, afterIdx int) (rows []*ParsedGenMetadata, maxIdx int, ok bool) {
	maxIdx = afterIdx
	if db == nil {
		return nil, maxIdx, false
	}
	q, err := db.Query("SELECT idx, data FROM gen_metadata WHERE idx > ? ORDER BY idx ASC", afterIdx)
	if err != nil {
		return nil, maxIdx, false
	}
	defer q.Close()
	sid := strings.ToLower(sessionID)
	for q.Next() {
		var idx int
		var data []byte
		if q.Scan(&idx, &data) != nil {
			continue
		}
		if idx > maxIdx {
			maxIdx = idx
		}
		if gm := ParseGenMetadataBlob(data); gm != nil {
			gm.SessionID = sid
			gm.Idx = idx
			rows = append(rows, gm)
		}
	}
	return rows, maxIdx, q.Err() == nil
}

// ReadSessionGenMetadataAfter returns every gen_metadata row with idx > afterIdx in ascending
// order (zero-token rows are skipped but still advance maxIdx). ok is false when the session
// database or table cannot be read. Each row is one upstream generation; callers sum them so a
// retried or multi-step prompt is billed completely and exactly once.
func ReadSessionGenMetadataAfter(convDir, sessionID string, afterIdx int) (rows []*ParsedGenMetadata, maxIdx int, ok bool) {
	maxIdx = afterIdx
	if sessionID == "" {
		return nil, maxIdx, false
	}
	dbPath := filepath.Join(convDir, sessionID+".db")
	if st, err := os.Stat(dbPath); err != nil || st.Size() < 100 {
		return nil, maxIdx, false
	}
	db, err := OpenReadOnlySQLite(dbPath)
	if err != nil {
		return nil, maxIdx, false
	}
	defer db.Close()
	return QuerySessionGenMetadataAfterDB(db, sessionID, afterIdx)
}

func ReadSessionStepsAfter(convDir, sessionID string, afterIdx int) (steps []ParsedModelStep, maxIdx int, ok bool) {
	maxIdx = afterIdx
	if sessionID == "" {
		return nil, maxIdx, false
	}
	dbPath := filepath.Join(convDir, sessionID+".db")
	st, err := os.Stat(dbPath)
	if err != nil || st.Size() < 100 {
		return nil, maxIdx, false
	}
	steps, ok = ReadModelStepsSQL(dbPath, "WSL", map[string]bool{strings.ToLower(sessionID): true}, afterIdx, st.ModTime().Unix())
	for _, s := range steps {
		if s.StepIdx > maxIdx {
			maxIdx = s.StepIdx
		}
	}
	return steps, maxIdx, ok
}

type PlannerToolCall struct {
	UpstreamID  string
	RawToolName string
	ArgsJSON    string
}

func parsePlannerToolCallSub(tcBuf []byte) (PlannerToolCall, bool) {
	var tc PlannerToolCall
	pos := 0
	for pos < len(tcBuf) {
		tag, next, ok := readProtoVarint(tcBuf, pos)
		if !ok {
			break
		}
		pos = next
		fn := tag >> 3
		wt := tag & 7
		if fn == 0 {
			break
		}
		switch wt {
		case 0:
			_, nextV, okV := readProtoVarint(tcBuf, pos)
			if !okV {
				return tc, tc.RawToolName != ""
			}
			pos = nextV
		case 2:
			ln, nextL, okL := readProtoVarint(tcBuf, pos)
			if !okL || ln > uint64(len(tcBuf)-nextL) {
				return tc, tc.RawToolName != ""
			}
			sub := tcBuf[nextL : nextL+int(ln)]
			pos = nextL + int(ln)
			switch fn {
			case 1:
				tc.UpstreamID = string(sub)
			case 2:
				tc.RawToolName = string(sub)
			case 3:
				tc.ArgsJSON = string(sub)
			}
		case 1:
			if pos+8 > len(tcBuf) {
				return tc, tc.RawToolName != ""
			}
			pos += 8
		case 5:
			if pos+4 > len(tcBuf) {
				return tc, tc.RawToolName != ""
			}
			pos += 4
		default:
			return tc, tc.RawToolName != ""
		}
	}
	return tc, tc.RawToolName != ""
}

func parsePlannerResponseToolCalls(stepPayload []byte) []PlannerToolCall {
	var out []PlannerToolCall
	pos := 0
	for pos < len(stepPayload) {
		tag, next, ok := readProtoVarint(stepPayload, pos)
		if !ok {
			break
		}
		pos = next
		fn := tag >> 3
		wt := tag & 7
		if fn == 0 {
			break
		}
		switch wt {
		case 0:
			_, nextV, okV := readProtoVarint(stepPayload, pos)
			if !okV {
				return out
			}
			pos = nextV
		case 2:
			ln, nextL, okL := readProtoVarint(stepPayload, pos)
			if !okL || ln > uint64(len(stepPayload)-nextL) {
				return out
			}
			sub := stepPayload[nextL : nextL+int(ln)]
			pos = nextL + int(ln)
			if fn == 20 {
				sp := 0
				for sp < len(sub) {
					st, nextSt, okSt := readProtoVarint(sub, sp)
					if !okSt {
						break
					}
					sp = nextSt
					sfn := st >> 3
					swt := st & 7
					if sfn == 0 {
						break
					}
					switch swt {
					case 0:
						_, nextSv, okSv := readProtoVarint(sub, sp)
						if !okSv {
							sp = len(sub)
							break
						}
						sp = nextSv
					case 2:
						sln, nextSln, okSln := readProtoVarint(sub, sp)
						if !okSln || sln > uint64(len(sub)-nextSln) {
							sp = len(sub)
							break
						}
						child := sub[nextSln : nextSln+int(sln)]
						sp = nextSln + int(sln)
						if sfn == 7 {
							if tc, okTc := parsePlannerToolCallSub(child); okTc {
								out = append(out, tc)
							}
						}
					case 1:
						if sp+8 > len(sub) {
							sp = len(sub)
							break
						}
						sp += 8
					case 5:
						if sp+4 > len(sub) {
							sp = len(sub)
							break
						}
						sp += 4
					default:
						sp = len(sub)
					}
				}
			}
		case 1:
			if pos+8 > len(stepPayload) {
				return out
			}
			pos += 8
		case 5:
			if pos+4 > len(stepPayload) {
				return out
			}
			pos += 4
		default:
			return out
		}
	}
	return out
}

func QueryLatestPlannerToolCallsDB(db *sql.DB, afterIdx int) (plannerIdx int, calls []PlannerToolCall) {
	if db == nil {
		return -1, nil
	}
	var payload []byte
	if err := db.QueryRow("SELECT idx, step_payload FROM steps WHERE step_type = 15 AND idx > ? ORDER BY idx DESC LIMIT 1", afterIdx).Scan(&plannerIdx, &payload); err != nil {
		return -1, nil
	}
	return plannerIdx, parsePlannerResponseToolCalls(payload)
}

// ReadLatestPlannerToolCalls returns the latest step_type=15 (CortexStepPlannerResponse) idx > afterIdx
// and its ordered .20.7 ToolCall proposals. In oneharness (agy >= 1.5.0), stepGenerate commits the
// PlannerResponse step with all proposed tool calls before stepToolDispatch executes tool #1 over MCP.
func ReadLatestPlannerToolCalls(convDir, sessionID string, afterIdx int) (plannerIdx int, calls []PlannerToolCall) {
	plannerIdx = -1
	if sessionID == "" {
		return -1, nil
	}
	dbPath := filepath.Join(convDir, sessionID+".db")
	st, err := os.Stat(dbPath)
	if err != nil || st.Size() < 100 {
		return -1, nil
	}
	db, err := OpenReadOnlySQLite(dbPath)
	if err != nil {
		return -1, nil
	}
	defer db.Close()
	return QueryLatestPlannerToolCallsDB(db, afterIdx)
}

// ParsedErrorStep represents a decoded step_type=17 (CortexStepErrorMessage) row from <sid>.db.
// In agy >= 1.5.13, internal retry attempts (ShouldShowUser=false) are no longer streamed over
// ACP agent_message_chunk, but are still committed to SQLite WAL immediately on each retry.
type ParsedErrorStep struct {
	StepIdx           int
	ShouldShowUser    bool
	ErrorCode         int
	UserErrorMessage  string
	ShortError        string
	ModelErrorMessage string
}

func parseCortexErrorDetailsSub(buf []byte, dst *ParsedErrorStep) {
	pos := 0
	for pos < len(buf) {
		tag, next, ok := readProtoVarint(buf, pos)
		if !ok {
			break
		}
		pos = next
		fn := tag >> 3
		wt := tag & 7
		if fn == 0 {
			break
		}
		switch wt {
		case 0:
			v, nextV, okV := readProtoVarint(buf, pos)
			if !okV {
				return
			}
			pos = nextV
			if fn == 7 && dst.ErrorCode == 0 {
				dst.ErrorCode = int(v)
			}
		case 2:
			ln, nextL, okL := readProtoVarint(buf, pos)
			if !okL || ln > uint64(len(buf)-nextL) {
				return
			}
			sub := buf[nextL : nextL+int(ln)]
			pos = nextL + int(ln)
			switch fn {
			case 1:
				if dst.UserErrorMessage == "" {
					dst.UserErrorMessage = string(sub)
				}
			case 2:
				if dst.ShortError == "" {
					dst.ShortError = string(sub)
				}
			case 9:
				if dst.ModelErrorMessage == "" {
					dst.ModelErrorMessage = string(sub)
				}
			}
		case 1:
			if pos+8 > len(buf) {
				return
			}
			pos += 8
		case 5:
			if pos+4 > len(buf) {
				return
			}
			pos += 4
		default:
			return
		}
	}
}

// ParseErrorStepPayload decodes CortexStep.error_message (field 24) from step_payload,
// falling back to the raw error_details column when step_payload carries no message fields.
func ParseErrorStepPayload(stepPayload, errorDetailsCol []byte) *ParsedErrorStep {
	var out ParsedErrorStep
	pos := 0
	for pos < len(stepPayload) {
		tag, next, ok := readProtoVarint(stepPayload, pos)
		if !ok {
			break
		}
		pos = next
		fn := tag >> 3
		wt := tag & 7
		if fn == 0 {
			break
		}
		switch wt {
		case 0:
			_, nextV, okV := readProtoVarint(stepPayload, pos)
			if !okV {
				pos = len(stepPayload)
				break
			}
			pos = nextV
		case 2:
			ln, nextL, okL := readProtoVarint(stepPayload, pos)
			if !okL || ln > uint64(len(stepPayload)-nextL) {
				pos = len(stepPayload)
				break
			}
			sub := stepPayload[nextL : nextL+int(ln)]
			pos = nextL + int(ln)
			if fn == 24 {
				sp := 0
				for sp < len(sub) {
					st, nextSt, okSt := readProtoVarint(sub, sp)
					if !okSt {
						break
					}
					sp = nextSt
					sfn := st >> 3
					swt := st & 7
					if sfn == 0 {
						break
					}
					switch swt {
					case 0:
						v, nextSv, okSv := readProtoVarint(sub, sp)
						if !okSv {
							sp = len(sub)
							break
						}
						sp = nextSv
						if sfn == 5 {
							out.ShouldShowUser = v != 0
						}
					case 2:
						sln, nextSln, okSln := readProtoVarint(sub, sp)
						if !okSln || sln > uint64(len(sub)-nextSln) {
							sp = len(sub)
							break
						}
						child := sub[nextSln : nextSln+int(sln)]
						sp = nextSln + int(sln)
						if sfn == 3 {
							parseCortexErrorDetailsSub(child, &out)
						}
					case 1:
						if sp+8 > len(sub) {
							sp = len(sub)
							break
						}
						sp += 8
					case 5:
						if sp+4 > len(sub) {
							sp = len(sub)
							break
						}
						sp += 4
					default:
						sp = len(sub)
					}
				}
			}
		case 1:
			if pos+8 > len(stepPayload) {
				pos = len(stepPayload)
				break
			}
			pos += 8
		case 5:
			if pos+4 > len(stepPayload) {
				pos = len(stepPayload)
				break
			}
			pos += 4
		default:
			pos = len(stepPayload)
		}
	}
	if out.ShortError == "" && out.UserErrorMessage == "" && out.ModelErrorMessage == "" && len(errorDetailsCol) > 0 {
		parseCortexErrorDetailsSub(errorDetailsCol, &out)
	}
	if out.ShortError == "" && out.UserErrorMessage == "" && out.ModelErrorMessage == "" && out.ErrorCode == 0 {
		return nil
	}
	return &out
}

// QueryMaxAllStepIdxDB returns the highest steps.idx across all step types (-1 if empty/unavailable).
func QueryMaxAllStepIdxDB(db *sql.DB) int {
	if db == nil {
		return -1
	}
	var maxIdx int = -1
	_ = db.QueryRow("SELECT COALESCE(MAX(idx), -1) FROM steps").Scan(&maxIdx)
	return maxIdx
}

func ReadMaxSessionAllStepIdx(convDir, sessionID string) int {
	if sessionID == "" {
		return -1
	}
	dbPath := filepath.Join(convDir, sessionID+".db")
	db, err := OpenReadOnlySQLite(dbPath)
	if err != nil {
		return -1
	}
	defer db.Close()
	return QueryMaxAllStepIdxDB(db)
}

// QueryErrorStepsAfterDB returns all step_type=17 rows with idx > afterIdx in ascending order.
func QueryErrorStepsAfterDB(db *sql.DB, afterIdx int) (steps []ParsedErrorStep, maxIdx int, ok bool) {
	maxIdx = afterIdx
	if db == nil {
		return nil, maxIdx, false
	}
	q, err := db.Query("SELECT idx, step_payload, error_details FROM steps WHERE step_type = 17 AND idx > ? ORDER BY idx ASC", afterIdx)
	if err != nil {
		return nil, maxIdx, false
	}
	defer q.Close()
	for q.Next() {
		var idx int
		var payload, errCol []byte
		if q.Scan(&idx, &payload, &errCol) != nil {
			continue
		}
		if idx > maxIdx {
			maxIdx = idx
		}
		if parsed := ParseErrorStepPayload(payload, errCol); parsed != nil {
			parsed.StepIdx = idx
			steps = append(steps, *parsed)
		}
	}
	return steps, maxIdx, q.Err() == nil
}

func ReadSessionErrorStepsAfter(convDir, sessionID string, afterIdx int) (steps []ParsedErrorStep, maxIdx int, ok bool) {
	maxIdx = afterIdx
	if sessionID == "" {
		return nil, maxIdx, false
	}
	dbPath := filepath.Join(convDir, sessionID+".db")
	if st, err := os.Stat(dbPath); err != nil || st.Size() < 100 {
		return nil, maxIdx, false
	}
	db, err := OpenReadOnlySQLite(dbPath)
	if err != nil {
		return nil, maxIdx, false
	}
	defer db.Close()
	return QueryErrorStepsAfterDB(db, afterIdx)
}
