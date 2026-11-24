package dashboard

import "strings"

// KnownUpgradeMetricStates enumerates all Prometheus gauge label values for agy_gateway_upgrade_state.
var KnownUpgradeMetricStates = []string{
	"none",
	"idle",
	"testing",
	"ready",
	"approved",
	"applying",
	"waiting_idle",
	"applied",
	"rolled_back",
	"failed",
	"status_error",
}

// NormalizeUpgradeMetricState maps raw candidate status state + approval presence to a fixed Prometheus label.
func NormalizeUpgradeMetricState(hasFile bool, readErr bool, rawState string, hasCandidate, approvalPending bool) string {
	switch {
	case !hasFile:
		return "none"
	case readErr:
		return "status_error"
	case !hasCandidate:
		return "idle"
	}
	state := strings.TrimSpace(rawState)
	if state == "" {
		return "idle"
	}
	if state == "ready" && approvalPending {
		return "approved"
	}
	for _, known := range KnownUpgradeMetricStates {
		if state == known {
			return state
		}
	}
	return "other"
}
