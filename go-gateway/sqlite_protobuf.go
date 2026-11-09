package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const TpuTailReserveTokens = 2054

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

var DefaultGeminiPricing = GeminiPricing{
	BaseInputPerM:       1.25,
	BaseCachedReadPerM:  0.125,
	BaseOutputPerM:      10.0,
	Context1MMultiplier: 1.8,
	InputPerM:           2.25,
	CachedReadPerM:      0.225,
	OutputPerM:          18.0,
	Cap5hUsd:            200.0,
	CapWeeklyUsd:        800.0,
}

type ParsedModelStep struct {
	SessionID      string  `json:"sessionId"`
	SourceSide     string  `json:"sourceSide"`
	SourceCategory string  `json:"sourceCategory"`
	SourceLabel    string  `json:"sourceLabel"`
	StepIdx        int     `json:"stepIdx"`
	TsSec          int64   `json:"tsSec"`
	TtftMs         int     `json:"ttftMs"`
	ElapsedMs      int     `json:"elapsedMs"`
	Tps            float64 `json:"tps"`
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
			_, nextV, okV := readProtoVarint(data, pos)
			if !okV {
				pos = len(data)
				break
			}
			pos = nextV
		case 2:
			ln, nextL, okL := readProtoVarint(data, pos)
			if !okL || int(ln) < 0 || nextL+int(ln) > len(data) {
				pos = len(data)
				break
			}
			sub := data[nextL : nextL+int(ln)]
			pos = nextL + int(ln)
			if fn == 1 {
				parseGenMetadataF1(sub, &gm)
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
	m, _ := ModelFromEnum(gm.ModelEnum)
	gm.ModelID = m.ID
	return &gm
}

type ArgonSignal struct {
	Kind   string `json:"kind"`
	Points int    `json:"points"`
	Detail string `json:"detail"`
}

type ArgonDegradationReport struct {
	Degraded      bool          `json:"degraded"`
	Score         int           `json:"score"`
	Signals       []ArgonSignal `json:"signals,omitempty"`
	CwmEstTokens  int           `json:"cwmEstTokens,omitempty"`
	PromptTokens  int           `json:"promptTokens"`
	CachedTokens  int           `json:"cachedTokens"`
	OutputTokens  int           `json:"outputTokens"`
	TextOutTokens int           `json:"textOutTokens"`
	ThoughtTokens int           `json:"thoughtTokens"`
	TtftSeconds   float64       `json:"ttftSeconds,omitempty"`
	StreamSeconds float64       `json:"streamSeconds,omitempty"`
}

type ArgonEvalContext struct {
	ModelID              string
	ThoughtLevel         string
	CompactedThisTurn    bool
	PrevRawCachedTokens  int
	PrevTotalInputTokens int
}

func EvaluateArgonDegradation(gm *ParsedGenMetadata, ctx ArgonEvalContext) *ArgonDegradationReport {
	if gm == nil {
		return nil
	}
	promptTok := gm.InputTokens + gm.CachedReadTokens
	textOut := gm.TextOutTokens
	if textOut <= 0 {
		if gm.OutputTokens > gm.ThoughtTokens {
			textOut = gm.OutputTokens - gm.ThoughtTokens
		} else {
			textOut = gm.OutputTokens
		}
	}
	rep := &ArgonDegradationReport{
		CwmEstTokens:  gm.CwmEstTokens,
		PromptTokens:  promptTok,
		CachedTokens:  gm.CachedReadTokens,
		OutputTokens:  gm.OutputTokens,
		TextOutTokens: textOut,
		ThoughtTokens: gm.ThoughtTokens,
		TtftSeconds:   gm.TtftSeconds,
		StreamSeconds: gm.StreamSeconds,
	}
	isArgon := strings.HasPrefix(strings.ToLower(ctx.ModelID), "gemini-4-argon") || gm.ModelID == "gemini-4-argon"
	if !isArgon {
		return rep
	}

	// F1: Context truncated at 128K boundary (+5)
	if gm.CwmEstTokens >= 140000 && promptTok >= 127950 && promptTok <= 128000 {
		rep.Signals = append(rep.Signals, ArgonSignal{
			Kind:   "context_truncated",
			Points: 5,
			Detail: fmt.Sprintf("cwmEst=%d clamped to prompt=%d", gm.CwmEstTokens, promptTok),
		})
	}

	// F2: Cache cliff without compaction (+4)
	if !ctx.CompactedThisTurn && ctx.PrevRawCachedTokens >= 16384 && gm.CachedReadTokens == 0 &&
		ctx.PrevTotalInputTokens > 0 && (ctx.PrevTotalInputTokens-promptTok) >= 2048 {
		rep.Signals = append(rep.Signals, ArgonSignal{
			Kind:   "cache_cliff",
			Points: 4,
			Detail: fmt.Sprintf("prevCache=%d->0, inputDrop=%d", ctx.PrevRawCachedTokens, ctx.PrevTotalInputTokens-promptTok),
		})
	}

	// F3: Fast decode TPS (+2 / +3)
	if textOut >= 64 && gm.StreamSeconds >= 0.35 {
		streamTps := float64(textOut) / gm.StreamSeconds
		if streamTps >= 260 {
			rep.Signals = append(rep.Signals, ArgonSignal{
				Kind:   "fast_decode",
				Points: 3,
				Detail: fmt.Sprintf("streamTps=%.1f", streamTps),
			})
		} else if streamTps >= 220 {
			rep.Signals = append(rep.Signals, ArgonSignal{
				Kind:   "fast_decode",
				Points: 2,
				Detail: fmt.Sprintf("streamTps=%.1f", streamTps),
			})
		}
	} else if textOut >= 80 && gm.ThoughtTokens == 0 && gm.TtftSeconds > 0 && gm.TtftSeconds < 5.5 {
		wallSec := gm.TtftSeconds + gm.StreamSeconds
		if wallSec > 0 {
			wallTps := float64(textOut) / wallSec
			if wallTps >= 180 {
				rep.Signals = append(rep.Signals, ArgonSignal{
					Kind:   "fast_decode",
					Points: 2,
					Detail: fmt.Sprintf("coldBurstWallTps=%.1f", wallTps),
				})
			}
		}
	}

	// F4: Block-aligned 2048 cache hit (+2 / +3, corroborating only)
	if gm.CachedReadTokens >= 4096 && (gm.CachedReadTokens%2048) == 0 {
		pts := 2
		if gm.CachedReadTokens >= 16384 {
			pts = 3
		}
		rep.Signals = append(rep.Signals, ArgonSignal{
			Kind:   "block_cache",
			Points: pts,
			Detail: fmt.Sprintf("cachedRead=%d (2048-aligned)", gm.CachedReadTokens),
		})
	}

	// F5: Shallow thought on high effort (+1 ~ +3)
	if strings.EqualFold(ctx.ThoughtLevel, "high") && gm.ThoughtTokens == 0 && textOut >= 32 {
		pts := 1
		if gm.TtftSeconds > 0 && gm.TtftSeconds < 6.0 {
			pts++
		}
		if promptTok >= 16384 {
			pts++
		}
		rep.Signals = append(rep.Signals, ArgonSignal{
			Kind:   "shallow_thought",
			Points: pts,
			Detail: fmt.Sprintf("high-tier thought=0 textOut=%d ttft=%.2fs", textOut, gm.TtftSeconds),
		})
	}

	score := 0
	hasPrimary := false
	for _, s := range rep.Signals {
		score += s.Points
		if s.Kind != "block_cache" {
			hasPrimary = true
		}
	}
	rep.Score = score
	rep.Degraded = score >= 3 && hasPrimary
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
							if bytes.Contains(sub[nextSln:endSlice], []byte("mcp_dsh-bridge_")) {
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

	ttftMs := 0
	if tFirst > tStart {
		ttftMs = int(math.Round((tFirst - tStart) * 1000))
		if ttftMs < 10 {
			ttftMs = 10
		}
	}
	elapsedMs := 0
	if tEnd > tStart {
		elapsedMs = int(math.Round((tEnd - tStart) * 1000))
		if elapsedMs < ttftMs {
			elapsedMs = ttftMs
		}
	}
	tps := 0.0
	if elapsedMs > 0 && outTok > 0 {
		tps = math.Round((float64(outTok)/(float64(elapsedMs)/1000.0))*10) / 10
	}

	model, tier := ModelFromEnum(modelEnum)

	// TPU tail alignment is a Gemini-specific caching heuristic; Claude/GPT usage is taken as reported
	alignedIn := inTok
	alignedCr := crTok
	if model.Family != "claude" && model.Family != "gpt" && alignedCr > 0 && alignedIn > TpuTailReserveTokens {
		trueDelta := alignedIn - TpuTailReserveTokens
		if trueDelta < 1 {
			trueDelta = 1
		}
		alignedCr += alignedIn - trueDelta
		alignedIn = trueDelta
	}

	pr := PriceUsage(model, inTok, crTok, outTok, thTok, cwTok)
	mult := pr.TierMultiplier

	tsSec := int64(tStart)
	if tsSec == 0 {
		tsSec = int64(tEnd)
	}

	return &ParsedModelStep{
		TsSec:          tsSec,
		TtftMs:         ttftMs,
		ElapsedMs:      elapsedMs,
		Tps:            tps,
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

type sqliteImage struct {
	data     []byte
	pageSize int
	reserved int
	usable   int
	walPages map[uint32][]byte
}

func openSQLiteImage(dbPath string) (*sqliteImage, error) {
	data, err := os.ReadFile(dbPath)
	if err != nil || len(data) < 100 {
		return nil, err
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

	img := &sqliteImage{
		data:     data,
		pageSize: ps,
		reserved: res,
		usable:   usable,
	}

	// Also apply uncommitted WAL frames if <dbPath>-wal exists and is non-empty (only for local WSL files)
	if !strings.HasPrefix(dbPath, "/mnt/c/") {
		walData, errWal := os.ReadFile(dbPath + "-wal")
		if errWal == nil && len(walData) >= 32+24+ps {
			img.walPages = make(map[uint32][]byte)
			frameSize := 24 + ps
			for off := 32; off+frameSize <= len(walData); off += frameSize {
				pgNo := binary.BigEndian.Uint32(walData[off : off+4])
				if pgNo > 0 {
					img.walPages[pgNo] = walData[off+24 : off+frameSize]
				}
			}
		}
	}
	return img, nil
}

func (img *sqliteImage) getPage(pgNo uint32) []byte {
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

func (img *sqliteImage) readCellPayload(cell []byte, payloadSize int) []byte {
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

func (img *sqliteImage) findStepsRootPage() uint32 {
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
		// col 0: type ("table"), col 1: name ("steps"), col 2: tbl_name, col 3: rootpage
		l0 := sqliteSerialTypeLen(sts[0])
		l1 := sqliteSerialTypeLen(sts[1])
		l2 := sqliteSerialTypeLen(sts[2])
		l3 := sqliteSerialTypeLen(sts[3])
		if dpos+l0+l1+l2+l3 > len(rec) {
			return
		}
		nameBytes := rec[dpos+l0 : dpos+l0+l1]
		if string(nameBytes) == "steps" {
			rp := readSQLiteInt(rec[dpos+l0+l1+l2:dpos+l0+l1+l2+l3], sts[3])
			if rp > 0 {
				found = uint32(rp)
			}
		}
	})
	return found
}

func (img *sqliteImage) walkTableBTree(pgNo uint32, depth int, onCell func(rowID int64, cellBody []byte, payloadSize int)) {
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

func ReadModelStepsFromSQLite(dbPath, sourceSide string, gwSids map[string]bool) []ParsedModelStep {
	st, err := os.Stat(dbPath)
	if err != nil || st.Size() < 4096 {
		return nil
	}
	sessionID := strings.ToLower(strings.TrimSuffix(filepath.Base(dbPath), ".db"))
	isKnownGw := gwSids[sessionID]

	img, err := openSQLiteImage(dbPath)
	if err != nil {
		return nil
	}
	rootPg := img.findStepsRootPage()
	if rootPg == 0 {
		return nil
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
		var sts [10]int64
		colCount := 0
		for colCount < 10 && pos < int(hdrLen) {
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
		// Check col 1 (step_type) in local cellBody!
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

		// Now read full payload (including overflow pages if any) for step_type == 15
		fullRec := img.readCellPayload(cellBody, pSize)
		if len(fullRec) < dpos+l0+l1 {
			return
		}
		stepIdx := int(rowID)
		if l0 > 0 {
			stepIdx = int(readSQLiteInt(fullRec[dpos:dpos+l0], sts[0]))
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
		if parsed.TsSec == 0 {
			parsed.TsSec = fallbackTs
		}
		parsed.SessionID = sessionID
		parsed.SourceSide = sourceSide
		parsed.StepIdx = stepIdx
		rawSteps = append(rawSteps, *parsed)
	})

	sourceCat := "WSL_HOST"
	sourceLbl := "宿主机 Agy"
	if sourceSide == "WIN" {
		sourceCat = "WIN_HOST"
		sourceLbl = "Win 宿主机"
	} else if hasAnyDshBridge {
		sourceCat = "CLAUDE_GW"
		sourceLbl = "Claude 网关"
	}
	for i := range rawSteps {
		rawSteps[i].SourceCategory = sourceCat
		rawSteps[i].SourceLabel = sourceLbl
	}
	return rawSteps
}

// Hot/Cold Incremental Cache for Dashboard & Gateway Step Lookup
type dbCacheEntry struct {
	mtimeUnixNano int64
	size          int64
	walMtimeNano  int64
	walSize       int64
	isKnownGw     bool
	steps         []ParsedModelStep
	lastCheckedAt time.Time
}

type DBStepCacheManager struct {
	mu      sync.RWMutex
	entries map[string]*dbCacheEntry
}

var GlobalDBCache = &DBStepCacheManager{
	entries: make(map[string]*dbCacheEntry),
}

func (m *DBStepCacheManager) GetSteps(dbPath, sourceSide string, gwSids map[string]bool) []ParsedModelStep {
	now := time.Now()
	sessionID := strings.ToLower(strings.TrimSuffix(filepath.Base(dbPath), ".db"))
	isKnownGw := gwSids[sessionID]

	m.mu.RLock()
	cached, exists := m.entries[dbPath]
	m.mu.RUnlock()

	// Cold file optimization: if a file was modified > 1 hour ago and we already checked it within the last 30s,
	// return cached steps immediately without even calling os.Stat()!
	if exists && cached.isKnownGw == isKnownGw {
		ageSinceMod := now.Sub(time.Unix(0, cached.mtimeUnixNano))
		if ageSinceMod > time.Hour && now.Sub(cached.lastCheckedAt) < 30*time.Second {
			return cached.steps
		}
	}

	st, err := os.Stat(dbPath)
	if err != nil || st.Size() < 4096 {
		return nil
	}
	var walMtime, walSize int64
	if !strings.HasPrefix(dbPath, "/mnt/c/") {
		if wst, errW := os.Stat(dbPath + "-wal"); errW == nil {
			walMtime = wst.ModTime().UnixNano()
			walSize = wst.Size()
		}
	}

	if exists &&
		cached.mtimeUnixNano == st.ModTime().UnixNano() &&
		cached.size == st.Size() &&
		cached.walMtimeNano == walMtime &&
		cached.walSize == walSize &&
		cached.isKnownGw == isKnownGw {
		m.mu.Lock()
		cached.lastCheckedAt = now
		m.mu.Unlock()
		return cached.steps
	}

	// Incremental SQL path via modernc.org/sqlite: if we already have cached steps for this file,
	// only query steps with idx > lastKnownIdx!
	var steps []ParsedModelStep
	if exists && len(cached.steps) > 0 && cached.isKnownGw == isKnownGw {
		lastIdx := cached.steps[len(cached.steps)-1].StepIdx
		if delta, ok := ReadModelStepsSQL(dbPath, sourceSide, gwSids, lastIdx, st.ModTime().Unix()); ok {
			steps = make([]ParsedModelStep, 0, len(cached.steps)+len(delta))
			steps = append(steps, cached.steps...)
			steps = append(steps, delta...)
		}
	}
	if steps == nil {
		if sqlSteps, ok := ReadModelStepsSQL(dbPath, sourceSide, gwSids, -1, st.ModTime().Unix()); ok {
			steps = sqlSteps
		} else {
			steps = ReadModelStepsFromSQLite(dbPath, sourceSide, gwSids)
		}
	}

	m.mu.Lock()
	m.entries[dbPath] = &dbCacheEntry{
		mtimeUnixNano: st.ModTime().UnixNano(),
		size:          st.Size(),
		walMtimeNano:  walMtime,
		walSize:       walSize,
		isKnownGw:     isKnownGw,
		steps:         steps,
		lastCheckedAt: now,
	}
	m.mu.Unlock()
	return steps
}

func openReadOnlySQLite(dbPath string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(100)&_pragma=query_only(1)", dbPath)
	return sql.Open("sqlite", dsn)
}

func ReadModelStepsSQL(dbPath, sourceSide string, gwSids map[string]bool, afterIdx int, fallbackTsSec int64) ([]ParsedModelStep, bool) {
	db, err := openReadOnlySQLite(dbPath)
	if err != nil {
		return nil, false
	}
	defer db.Close()

	sessionID := strings.ToLower(strings.TrimSuffix(filepath.Base(dbPath), ".db"))
	hasAnyDsh := gwSids[sessionID]

	rows, err := db.Query("SELECT idx, metadata, step_payload FROM steps WHERE step_type = 15 AND idx > ? ORDER BY idx ASC", afterIdx)
	if err != nil {
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

	srcCat := "WSL_HOST"
	srcLabel := "宿主机 Agy"
	if sourceSide == "WIN" {
		srcCat = "WIN_HOST"
		srcLabel = "Win 宿主机"
	} else if hasAnyDsh {
		srcCat = "CLAUDE_GW"
		srcLabel = "Claude 网关"
	}
	for i := range rawSteps {
		rawSteps[i].SourceCategory = srcCat
		rawSteps[i].SourceLabel = srcLabel
	}
	return rawSteps, true
}

func ReadLatestStepFromSQLiteFast(dbPath string) *ParsedModelStep {
	st, err := os.Stat(dbPath)
	if err != nil || st.Size() < 4096 {
		return nil
	}
	db, err := openReadOnlySQLite(dbPath)
	if err == nil {
		defer db.Close()
		rows, qErr := db.Query("SELECT idx, metadata, step_payload FROM steps WHERE step_type = 15 ORDER BY idx DESC LIMIT 8")
		if qErr == nil {
			defer rows.Close()
			sessionID := strings.ToLower(strings.TrimSuffix(filepath.Base(dbPath), ".db"))
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
						return parsed
					}
				}
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

func ReadLatestSessionStepUsage(sessionID string) *ParsedModelStep {
	if sessionID == "" {
		return nil
	}
	dbPath := filepath.Join(ConversationsDir(), sessionID+".db")
	return ReadLatestStepFromSQLiteFast(dbPath)
}

func ReadLatestGenMetadataFromSQLite(dbPath string, afterIdx int) *ParsedGenMetadata {
	st, err := os.Stat(dbPath)
	if err != nil || st.Size() < 100 {
		return nil
	}
	db, err := openReadOnlySQLite(dbPath)
	if err != nil {
		return nil
	}
	defer db.Close()
	rows, err := db.Query("SELECT idx, data FROM gen_metadata WHERE idx > ? ORDER BY idx DESC LIMIT 4", afterIdx)
	if err != nil {
		return nil
	}
	defer rows.Close()
	sessionID := strings.ToLower(strings.TrimSuffix(filepath.Base(dbPath), ".db"))
	for rows.Next() {
		var idx int
		var data []byte
		if rows.Scan(&idx, &data) == nil && len(data) > 0 {
			if gm := ParseGenMetadataBlob(data); gm != nil {
				gm.SessionID = sessionID
				gm.Idx = idx
				return gm
			}
		}
	}
	return nil
}

func ReadLatestSessionGenMetadata(sessionID string, afterIdx int) *ParsedGenMetadata {
	if sessionID == "" {
		return nil
	}
	dbPath := filepath.Join(ConversationsDir(), sessionID+".db")
	return ReadLatestGenMetadataFromSQLite(dbPath, afterIdx)
}

func ReadMaxSessionGenMetadataIdx(sessionID string) int {
	if sessionID == "" {
		return -1
	}
	dbPath := filepath.Join(ConversationsDir(), sessionID+".db")
	db, err := openReadOnlySQLite(dbPath)
	if err != nil {
		return -1
	}
	defer db.Close()
	var maxIdx int = -1
	_ = db.QueryRow("SELECT COALESCE(MAX(idx), -1) FROM gen_metadata").Scan(&maxIdx)
	return maxIdx
}

func ReadMaxSessionStepIdx(sessionID string) int {
	if sessionID == "" {
		return -1
	}
	dbPath := filepath.Join(ConversationsDir(), sessionID+".db")
	db, err := openReadOnlySQLite(dbPath)
	if err != nil {
		return -1
	}
	defer db.Close()
	var maxIdx int = -1
	_ = db.QueryRow("SELECT COALESCE(MAX(idx), -1) FROM steps WHERE step_type = 15").Scan(&maxIdx)
	return maxIdx
}
