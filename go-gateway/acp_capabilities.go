package main

import (
	"encoding/json"
	"errors"
)

type acpCapabilities struct{ Resume, Load, McpHTTP bool }

func parseAcpCapabilities(raw []byte) (acpCapabilities, error) {
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
		return acpCapabilities{}, err
	}
	if r.ProtocolVersion != 1 {
		return acpCapabilities{}, errors.New("unsupported ACP protocol version; expected v1")
	}
	declared := func(v json.RawMessage) bool { return len(v) > 0 && string(v) != "null" && string(v) != "false" }
	return acpCapabilities{Resume: declared(r.AgentCapabilities.SessionCapabilities["resume"]), Load: r.AgentCapabilities.LoadSession || declared(r.AgentCapabilities.SessionCapabilities["load"]), McpHTTP: r.AgentCapabilities.McpCapabilities.HTTP}, nil
}

func (e *SharedAcpEngine) capabilities() acpCapabilities {
	e.capsMu.RLock()
	defer e.capsMu.RUnlock()
	return e.caps
}
