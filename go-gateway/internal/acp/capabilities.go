package acp

import (
	"encoding/json"
	"errors"
)

// Capabilities records the capabilities advertised by agy during initialize.
type Capabilities struct {
	Resume  bool
	Load    bool
	McpHTTP bool
}

// ParseCapabilities parses an ACP initialize JSON result into normalized Capabilities.
func ParseCapabilities(raw []byte) (Capabilities, error) {
	var r struct {
		ProtocolVersion   int `json:"protocolVersion"`
		AgentCapabilities struct {
			LoadSession         bool                       `json:"loadSession"`
			SessionCapabilities map[string]json.RawMessage `json:"sessionCapabilities"`
			McpCapabilities     struct {
				HTTP bool `json:"http"`
			} `json:"mcpCapabilities"`
		} `json:"agentCapabilities"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return Capabilities{}, err
	}
	if r.ProtocolVersion != 1 {
		return Capabilities{}, errors.New("unsupported ACP protocol version; expected v1")
	}
	declared := func(v json.RawMessage) bool {
		return len(v) > 0 && string(v) != "null" && string(v) != "false"
	}
	return Capabilities{
		Resume:  declared(r.AgentCapabilities.SessionCapabilities["resume"]),
		Load:    r.AgentCapabilities.LoadSession || declared(r.AgentCapabilities.SessionCapabilities["load"]),
		McpHTTP: r.AgentCapabilities.McpCapabilities.HTTP,
	}, nil
}
