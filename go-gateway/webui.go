package main

import (
	_ "embed"
	"strings"
	"sync"
)

//go:embed webui/dashboard.html
var dashboardHTMLTemplate string

//go:embed webui/lucide-icons.json
var lucideIconsJSON string

var (
	dashboardOnce sync.Once
	dashboardHTML string
)

// RenderDashboardHTML returns the embedded UI with Lucide icons (ISC, see webui/LICENSE-lucide.txt) inlined.
func RenderDashboardHTML() string {
	dashboardOnce.Do(func() {
		dashboardHTML = strings.Replace(dashboardHTMLTemplate, "/*__LUCIDE__*/", "const ICONS = "+lucideIconsJSON+";", 1)
	})
	return dashboardHTML
}
