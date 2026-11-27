---
name: agy-acp-gateway Console
description: Operational telemetry and multi-account control console embedded in go-gateway/webui/dashboard.html.
colors:
  canvas-alabaster: "#f3efe7"
  surface-card: "#fdfcf9"
  surface-sub: "#f8f6f1"
  surface-well: "#eae5da"
  surface-pure: "#ffffff"
  ink-primary: "#15171c"
  ink-secondary: "#444854"
  ink-muted: "#646976"
  ink-subtle: "#9ea4b0"
  accent-cobalt: "#2451e6"
  accent-cobalt-hover: "#1c43c7"
  series-claude: "#c85226"
  series-gpt: "#0e8562"
  series-thinking: "#863899"
  series-cache: "#148058"
  status-ok: "#147a52"
  status-warn: "#a35f0b"
  status-bad: "#c1322b"
typography:
  display:
    fontFamily: "Bahnschrift, DIN Alternate, Barlow Semi Condensed, HarmonyOS Sans SC, sans-serif"
    fontSize: "4.5rem"
    fontWeight: 600
    lineHeight: 0.96
    letterSpacing: "-0.03em"
  headline:
    fontFamily: "HarmonyOS Sans SC, PingFang SC, Microsoft YaHei UI, system-ui, sans-serif"
    fontSize: "1.0625rem"
    fontWeight: 600
    lineHeight: 1.2
    letterSpacing: "-0.015em"
  title:
    fontFamily: "HarmonyOS Sans SC, PingFang SC, Microsoft YaHei UI, system-ui, sans-serif"
    fontSize: "0.96875rem"
    fontWeight: 600
    lineHeight: 1.25
    letterSpacing: "-0.01em"
  body:
    fontFamily: "HarmonyOS Sans SC, PingFang SC, Microsoft YaHei UI, system-ui, sans-serif"
    fontSize: "0.875rem"
    fontWeight: 400
    lineHeight: 1.5
    letterSpacing: "0em"
  mono:
    fontFamily: "Cascadia Mono, JetBrains Mono, SF Mono, IBM Plex Mono, monospace"
    fontSize: "0.75rem"
    fontWeight: 500
    lineHeight: 1.35
    letterSpacing: "0em"
rounded:
  xs: "6px"
  sm: "8px"
  md: "12px"
  lg: "16px"
spacing:
  xs: "6px"
  sm: "10px"
  md: "14px"
  lg: "20px"
  xl: "24px"
components:
  button-primary:
    backgroundColor: "{colors.ink-primary}"
    textColor: "{colors.surface-card}"
    rounded: "{rounded.sm}"
    height: "32px"
    padding: "0 13px"
  button-secondary:
    backgroundColor: "{colors.surface-pure}"
    textColor: "{colors.ink-primary}"
    rounded: "{rounded.sm}"
    height: "32px"
    padding: "0 13px"
  button-small:
    backgroundColor: "{colors.surface-pure}"
    textColor: "{colors.ink-secondary}"
    rounded: "8px"
    height: "27px"
    padding: "0 10px"
  segment-well:
    backgroundColor: "{colors.surface-well}"
    rounded: "10px"
    padding: "3px"
  segment-active:
    backgroundColor: "{colors.surface-pure}"
    textColor: "{colors.ink-primary}"
    rounded: "7px"
    height: "26px"
    padding: "0 11px"
  module-card:
    backgroundColor: "{colors.surface-card}"
    rounded: "{rounded.lg}"
    padding: "20px 24px"
  nested-card:
    backgroundColor: "{colors.surface-sub}"
    rounded: "{rounded.md}"
    padding: "14px 16px"
---

# Design System: agy-acp-gateway Console

## Overview

The `agy-acp-gateway` control console runs on `:18891` and is embedded as a single HTML file (`go-gateway/webui/dashboard.html`) inside the Go binary. Operators use it to check account quota usage, 5-hour tool safety check counters, prompt cache hit rates, live sessions, and recent request logs within a few seconds.

Design priorities:
- **Exception-only badges**: Uniform healthy facts (such as every active account running the same `agy` version) appear once in the top bar. Individual account cards show extra status badges only when an account diverges (standby role, shared account, version mismatch, pending switch, cooldown, verification required, or tool safety check limit).
- **Direct press feedback**: Clickable buttons and controls apply a `100ms` active transform and visible Cobalt focus rings.
- **Measured telemetry**: Displays actual official quota spend (`5h` / `7d` USD and remaining percentage), live cgroup working-set RAM, idle hot-worker target (`AGY_GLOBAL_IDLE_TARGET`), and burst worker cap (`AGY_GLOBAL_BURST_MAX_LIVE_WORKERS`).
- **Self-contained bundle**: CSS, JavaScript, and the Lucide SVG icon sprite (`go-gateway/webui/lucide-icons.json`) are embedded into the zero-CGO Go binary via `//go:embed`. No external CDN requests are made at runtime.

## Colors

Warm off-white neutrals form the background and cards. One primary blue accent (`#2451e6`) marks interactive focus and active state. Saturated hues are reserved for model families and status indicators.

### Primary
- **Cobalt Blue** (`#2451e6`, hover `#1c43c7`, soft fill `rgba(36, 81, 230, 0.09)`, focus ring `rgba(36, 81, 230, 0.22)`): Focus rings, active links, Gemini series bars, active filter buttons, and checked states.

### Neutral
- **Page Background** (`#f3efe7`): Warm light canvas behind top-level cards.
- **Module Surface** (`#fdfcf9`): Top-level `.module` sections and `.acc-modal` dialog body.
- **Nested Card Surface** (`#f8f6f1`): Nested cards (`.inst-qcard`, `.sess`, `.acc-item`) at rest; shifts to `#ffffff` on hover.
- **Recessed Track** (`#eae5da`): Segmented control tracks (`.seg`), progress bar tracks, icon wells, and inline code tags.
- **White Control Surface** (`#ffffff`): Active segment buttons, hover cards, secondary buttons, select triggers, and text inputs.
- **Text Scale** (`#15171c` primary, `#444854` secondary, `#646976` muted, `#9ea4b0` subtle): Maintains WCAG AA contrast on all light surfaces.
- **Borders** (`rgba(20, 22, 27, 0.075)` divider, `0.13` control edge, `0.22` hover border).

### Categorical & Semantic
- **Model Families (color-vision-safe palette)**: Gemini `#2451e6`, Claude `#c85226`, GPT `#0e8562`, Thinking tokens `#863899`, Cached tokens `#148058`.
- **Status Colors**: Healthy Green `#147a52`, Warning Amber `#a35f0b`, Error Red `#c1322b` (each paired with a `9-10%` tinted background).

### Usage Rules
- **One primary fill per view**: Dark charcoal solid buttons (`.btn.solid`) are used only for the main page action (`同步`), primary modal confirmations (`保存`, `保存配置`), and unauthenticated login buttons (`网页授权`). Filter tabs and segmented controls use white raised buttons inside recessed tracks so they do not compete with primary actions.
- **No repeated healthy pills**: Do not print identical healthy labels (such as `agy 1.5.16` or `运行中`) on every card. Show badges only for non-default roles or fault states.

## Typography

- **Display / Numerals**: `Bahnschrift`, `DIN Alternate`, `Barlow Semi Condensed`, `HarmonyOS Sans SC`, sans-serif. Every KPI, percentage, USD figure, and count sets `font-variant-numeric: tabular-nums`.
- **UI Headings & Body**: `HarmonyOS Sans SC`, `MiSans`, `PingFang SC`, `Microsoft YaHei UI`, `Noto Sans CJK SC`, system-ui, sans-serif.
- **Monospace Data**: `Cascadia Mono`, `JetBrains Mono`, `SF Mono`, `IBM Plex Mono`, monospace. Used for timestamps, session IDs, USD figures, ratios, and model IDs.

### Type Scale
- **Hero Number**: `600`, `72px` (`48px` on mobile), `line-height: 0.96`, `letter-spacing: -0.03em`.
- **Summary KPI**: `600`, `34px`, `line-height: 1.08`, `letter-spacing: -0.02em`.
- **Module Title**: `600`, `15.5px`, `line-height: 1.25`, `letter-spacing: -0.01em`.
- **Card Title**: `600`, `14px`, single-line ellipsis truncation (`text-overflow: ellipsis`).
- **Body / Table Cell**: `400`, `13-14px`, `line-height: 1.5`.
- **Badges & Small Metadata**: `500-600`, `11-12px`, `tabular-nums`.

## Layout

- **Page Container**: Centered `max-width: 1460px`, padding `20px 34px 64px` (`14px` horizontal padding at `<= 820px`).
- **Top Bar**: Single row, `<= 64px` tall. Brand title and unified `agy` version tag on the left; live status + sync clock, three action buttons (`账号`, `自动`, `同步`), and conditional update button on the right.
- **Collapsible Quota Pool**: Expand/collapse state persists in `localStorage` (`acp.quotaCollapsed`). When collapsed, `#quota-mini-bar` shows a single strip of clickable account chips (status dot, name, 5h remaining %, 5h USD spend) so Model Breakdown and Active Sessions fit above the fold.
- **Aligned Multi-Account Cards**: `.quota-multi` uses 2 equal columns (`1fr` at `<= 960px`). Cards align from the top (`justify-content: flex-start`) with fixed `gap: 8px` on `.inst-qrows` (`flex: 0 0 auto`) and `margin-top: auto` on `.inst-qfoot`. Quota rows (`Gemini 5h`, `Gemini 7d`, `Claude 5h`, `Claude 7d`, `检查 5h`) align horizontally across Pro and Ultra cards.
- **Breakpoints**: `1220px` (stack hero and single-account panels), `1080px` (wrap topbar status), `960px` (1-column account cards), `820px` (compact table and mobile padding), `520px` (stacked quota rows).

## Elevation & Depth

Shadows use warm dark tints (`rgba(28, 24, 18, ...)`) instead of pure black:

- **Recessed (`--paper-dim` + `inset 0 1px 2px rgba(20, 22, 27, 0.065)`)**: Segmented control tracks, progress tracks, icon wells.
- **Card (`--sh-card`)**: `0 1px 2px rgba(28, 24, 18, 0.035), 0 10px 28px -18px rgba(28, 24, 18, 0.11), inset 0 1px 0 rgba(255, 255, 255, 0.85)`.
- **Card Hover (`--sh-card-hover`)**: `0 6px 18px -10px rgba(28, 24, 18, 0.16), 0 1px 3px rgba(28, 24, 18, 0.05)` with `translateY(-1px)` on `.inst-qcard` and `.sess`.
- **Active Segment (`--sh-keycap`)**: `0 1px 3px rgba(28, 24, 18, 0.11), 0 1px 1px rgba(28, 24, 18, 0.05), inset 0 1px 0 rgba(255, 255, 255, 0.9)`.
- **Modal Backdrop**: `rgba(20, 22, 27, 0.34)` with `backdrop-filter: blur(5px)` and `--sh-modal`.

## Shapes

The console uses a 4-step concentric corner-radius scale (`6px` / `8px` / `12px` / `16px`) across all containers and controls:

- **`--r-xs` (`6px`)**: Inline badges and tags (`.inst-tag`, `.cli-badge`, `.pool-badge`, `.state`, `.mtag .tier`, `td .tag`, `.think`, `.mon-pacing-pill`, `.up-gate`, `.miss-pill`, `.key-pill`, `.cwd-chip`, `.acc-model-bar .btn-sm`). Do not place `999px` text capsules next to `8px` buttons.
- **`--r-sm` (`8px`)**: Buttons, text inputs, `.ui-sel-trigger`, checkbox rows (`.acc-mchk`), toggle rows (`.acc-switch-row`), status pills, compact account chips (`.qmini-chip`), and memory budget hints (`.mem-budget-hint`).
- **Concentric inner radii**:
  - `10px` `.seg` track (`3px` padding) wraps `7px` `.seg button.on`.
  - `12px` `.ui-sel-menu` (`5px` padding) wraps `7px` `.ui-sel-opt`.
  - `8px` `.acc-mchk` (`3.5px` inset) wraps `4.5px` custom checkboxes and `4.5px` `.m-kind` tags.
- **`--r-md` (`12px`)**: Nested cards (`.inst-qcard`, `.sess`, `.acc-item`), `.pool-ctrl-bar`, `.alert-strip`, `.ui-sel-menu`, `.acc-more-menu`, and `.up-pop`.
- **`--r-lg` (`16px`)**: Top-level `.module` sections and `.acc-modal` dialog.
- **`999px` / `50%` (restricted)**: Used only for circular status dots (`6-8px`), progress bar tracks/fills (`5-6px`), and `.ui-switch` track and thumb.

## Components

### Buttons, Selects, Checkboxes & Switches
- **Primary Button (`.btn.solid`)**: Dark charcoal vertical gradient (`#272a33 -> #15171c`), top highlight line, `translateY(-0.5px)` on hover, `translateY(0.5px) scale(0.98)` on `:active` (`100ms`).
- **Secondary Button (`.btn`, `.btn-sm`, `.quota-collapse-btn`, `.lanes-toggle`)**: White `#ffffff` fill, `--sh-ctrl` resting shadow, subtle lift on hover, press state on `:active`.
- **Custom Select (`.ui-sel` + `#ui-sel-menu`)**: Wraps toolbar and form `<select>` controls with a `8px`-radius trigger button and a `12px`-radius floating popover menu (`7px` option rows, soft Cobalt selected highlight, Lucide `check` icon). Keeps the underlying `<select>` synchronized for change events and keyboard navigation while avoiding inconsistent native OS popups.
- **Custom Checkbox**: Uses `appearance: none` on a `15x15px` (`4.5px` radius) box with model-family fill when checked, inline SVG checkmark (`stroke-width: 2.8`, `round` caps), and a `150ms` `chkPop` scale keyframe.
- **Toggle Switch (`.ui-switch`)**: Placed inside `.acc-switch-row` for binary account flags such as `共享账号`. Uses a `36x20px` pill track (`999px` radius) and `16x16px` white thumb (`50%` radius) that transitions to Cobalt when checked.
- **Form Inputs**: Consistent `36px` height and `8px` radius. Prose labels and display names use `var(--sans)`; IDs, keys, URLs, and numbers use `var(--mono)`. Read-only ID fields use `#f4f1ea` fill with a `1px dashed` border.
- **Icon Button (`.icon-btn`)**: `26x26px` borderless button for secondary card actions (Set Active, Edit, View Details). Reveals a recessed tint on hover and scales to `0.91` on `:active`.
- **Segmented Control (`.seg`)**: Recessed `10px` track holding white `7px` active buttons.

### Motion Rules
- **Quota Pool Collapse**: CSS Grid row transition (`grid-template-rows: 1fr -> 0fr`, `240ms cubic-bezier(0.16, 1, 0.3, 1)`) paired with `180deg` chevron rotation and `#quota-mini-bar` toggle.
- **Press & Modal Timing**: `100ms` `:active` scale on buttons; `180-220ms` modal open (`fadeIn` backdrop + subtle scale/translate).
- **Live Indicator**: Single radial pulse (`livePulse`, `2.6s`) on `#live-dot` while at least one session is active; pauses when idle.
- **Reduced Motion**: `@media (prefers-reduced-motion: reduce)` turns off non-essential animations and transforms while preserving clear color/state feedback.

## Do's and Don'ts

- **Do** load all icons from `go-gateway/webui/lucide-icons.json` via `svg()` or `data-ic`.
- **Do** apply `font-variant-numeric: tabular-nums` to numeric table columns, counters, timestamps, and USD amounts.
- **Do** show real memory telemetry (live cgroup working-set RAM, `AGY_GLOBAL_IDLE_TARGET`, and `AGY_GLOBAL_BURST_MAX_LIVE_WORKERS`) alongside real `5h` / `7d` USD spend.
- **Don't** put Emoji characters or em/en dashes (`U+2014` / `U+2013`) anywhere in `dashboard.html` (Go unit tests reject them).
- **Don't** place dark solid primary buttons inside every card of a multi-account grid; reserve solid fills for urgent unauthenticated login buttons and page-level primary actions.
- **Don't** set `flex: 1` or `justify-content: space-between` on `.inst-qrows` or `.inst-qcard`; doing so misaligns quota rows between Pro and Ultra cards.
