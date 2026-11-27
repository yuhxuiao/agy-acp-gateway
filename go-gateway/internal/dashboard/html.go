package dashboard

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// BuildID computes a short deterministic cache-busting content hash for the dashboard bundle.
func BuildID(templateHTML, lucideIconsJSON string) string {
	sum := sha256.Sum256([]byte(templateHTML + "\n" + lucideIconsJSON))
	return hex.EncodeToString(sum[:6])
}

// RenderHTML injects build ID and Lucide icon map constants into templateHTML.
func RenderHTML(templateHTML, lucideIconsJSON string) (buildID, html string) {
	buildID = BuildID(templateHTML, lucideIconsJSON)
	rendered := strings.Replace(templateHTML, "/*__UI_BUILD__*/", `const UI_BUILD_ID = "`+buildID+`";`, 1)
	html = strings.Replace(rendered, "/*__LUCIDE__*/", "const ICONS = "+lucideIconsJSON+";", 1)
	return buildID, html
}
