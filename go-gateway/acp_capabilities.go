package main

import (
	"encoding/json"

	"agy-acp-gateway-go/internal/acp"
)

type acpCapabilities = acp.Capabilities

func parseAcpCapabilities(raw []byte) (acpCapabilities, error) {
	return acp.ParseCapabilities(raw)
}

func (e *SharedAcpEngine) capabilities() acpCapabilities {
	e.capsMu.RLock()
	defer e.capsMu.RUnlock()
	return e.caps
}

// Capabilities implements acp.Engine.
func (e *SharedAcpEngine) Capabilities() acp.Capabilities {
	return e.capabilities()
}

var _ = json.Unmarshal
