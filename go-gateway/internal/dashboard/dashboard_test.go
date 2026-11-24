package dashboard

import (
	"strings"
	"testing"
)

func TestRenderHTMLAndBuildID(t *testing.T) {
	tpl := `<script>/*__UI_BUILD__*//*__LUCIDE__*/</script>`
	icons := `{"check":"<svg/>"}`
	id, html := RenderHTML(tpl, icons)
	if len(id) != 12 {
		t.Fatalf("expected 12-hex build ID, got %q", id)
	}
	if !strings.Contains(html, `const UI_BUILD_ID = "`+id+`";`) {
		t.Fatalf("missing injected build ID in %q", html)
	}
	if !strings.Contains(html, `const ICONS = {"check":"<svg/>"};`) {
		t.Fatalf("missing injected icons in %q", html)
	}
}

func TestNormalizeUpgradeMetricState(t *testing.T) {
	cases := []struct {
		hasFile, readErr, hasCand, approved bool
		raw                                 string
		want                                string
	}{
		{false, false, false, false, "", "none"},
		{true, true, false, false, "", "status_error"},
		{true, false, false, false, "", "idle"},
		{true, false, true, false, "ready", "ready"},
		{true, false, true, true, "ready", "approved"},
		{true, false, true, false, "unrecognized_xyz", "other"},
	}
	for _, tc := range cases {
		if got := NormalizeUpgradeMetricState(tc.hasFile, tc.readErr, tc.raw, tc.hasCand, tc.approved); got != tc.want {
			t.Fatalf("got %q want %q for %+v", got, tc.want, tc)
		}
	}
}
