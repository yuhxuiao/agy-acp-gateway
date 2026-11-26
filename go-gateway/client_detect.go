package main

import (
	"net/http"

	"agy-acp-gateway-go/internal/protocol"
)

type ClientIdentity = protocol.ClientIdentity

func detectInboundProtocol(r *http.Request) string { return protocol.DetectInboundProtocol(r) }

func fallbackClientForProtocol(proto string) ClientIdentity {
	return protocol.FallbackClientForProtocol(proto)
}

func isKnownSpecificClientID(id string) bool { return protocol.IsKnownSpecificClientID(id) }

func DetectClientIdentity(r *http.Request, body map[string]any, convKey, rawSys, cleanSys string, rawTools any) ClientIdentity {
	return protocol.DetectClientIdentity(r, body, convKey, rawSys, cleanSys, rawTools)
}

func InferHistoricalClientIdentity(convKey, sysText, initToolsJSON, curToolsJSON string) ClientIdentity {
	return protocol.InferHistoricalClientIdentity(convKey, sysText, initToolsJSON, curToolsJSON)
}
