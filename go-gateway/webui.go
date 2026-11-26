package main

import (
	_ "embed"
	"sync"

	"agy-acp-gateway-go/internal/dashboard"
)

//go:embed webui/dashboard.html
var dashboardHTMLTemplate string

//go:embed webui/lucide-icons.json
var lucideIconsJSON string

var (
	dashboardOnce    sync.Once
	dashboardHTML    string
	dashboardBuildID string
)

func initDashboardAssets() {
	dashboardOnce.Do(func() {
		dashboardBuildID, dashboardHTML = dashboard.RenderHTML(dashboardHTMLTemplate, lucideIconsJSON)
	})
}

func DashboardBuildID() string {
	initDashboardAssets()
	return dashboardBuildID
}

// RenderDashboardHTML returns the embedded UI with Lucide icons (ISC, see webui/LICENSE-lucide.txt) inlined.
func RenderDashboardHTML() string {
	initDashboardAssets()
	return dashboardHTML
}
