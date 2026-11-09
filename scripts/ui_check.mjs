// Interaction check for the dashboard UI: expand a log row, filter by model family, switch window.
// Usage: node scripts/ui_check.mjs [url] [shotDir]
import { createRequire } from "node:module";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const pkgHint = process.env.PLAYWRIGHT_PACKAGE_JSON || import.meta.url;
const require = createRequire(pkgHint);
const { chromium } = require("playwright");

const url = process.argv[2] || "http://127.0.0.1:18891/";
const shotDir = process.argv[3] || "/tmp";
const defaultChrome = path.join(os.homedir(), ".local/share/browser-binaries/playwright/chromium-1234/chrome-linux64/chrome");
const execPath = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || (fs.existsSync(defaultChrome) ? defaultChrome : undefined);
const browser = await chromium.launch({
  ...(execPath ? { executablePath: execPath } : {}),
  args: ["--no-sandbox"],
});
const page = await browser.newPage({
  viewport: { width: 1600, height: 1000 },
  ...(process.env.AGY_KEY ? { httpCredentials: { username: "admin", password: process.env.AGY_KEY } } : {}),
});
const errors = [];
page.on("pageerror", (e) => errors.push(String(e)));
page.on("console", (m) => m.type() === "error" && errors.push(m.text()));
const loaded = page.waitForResponse(r => r.url().includes("/api/dashboard") && r.status() === 200);
await page.goto(url);
await loaded;
await page.waitForSelector("#log-body tr", { timeout: 20000 });

const svgCount = await page.locator("svg.ic").count();
const firstRow = page.locator("#log-body tr.row").first();
let logTab = "db";
if (await firstRow.count() === 0) {
  const emptyText = await page.locator("#log-body").innerText();
  if (!/暂无|没有|no |empty/i.test(emptyText)) throw new Error("Missing empty state");
  await page.click('#tab-seg button[data-t="gw"]');
  logTab = "gw";
}
const hasHistory = await firstRow.count() > 0;
let detail = "";
if (hasHistory) {
  await firstRow.click();
  detail = await page.locator("#log-body tr.detail .det").first().innerText();
} else {
  const emptyText = await page.locator("#log-body").innerText();
  if (!/暂无|没有|no |empty/i.test(emptyText)) throw new Error("Missing history and missing empty state");
}

await page.click('#fam-seg button[data-f="claude"]');
const famCells = await page.locator(`#log-body tr.row td:nth-child(${logTab === "gw" ? 4 : 3})`).allInnerTexts();
const allClaude = famCells.every((t) => /Claude/.test(t));

await page.click('#win-seg button[data-w="window5h"]');
await page.waitForTimeout(400);
const usageHead = await page.locator("#usage .m-note").first().innerText();
const laneCount = await page.locator("#lanes .lane:not(.head)").count();

await page.locator("section.module").last().scrollIntoViewIfNeeded();
await page.waitForTimeout(1200);
await page.screenshot({ path: `${shotDir}/ui-check-log.png`, fullPage: false });

console.log(JSON.stringify({
  historyMode: hasHistory ? "populated" : "empty",
  logTab,
  detailOpened: detail.length > 0,
  svgIcons: svgCount,
  detailHasModel: /模型/.test(detail) && /(Claude|Gemini|GPT)/.test(detail),
  detailPreview: detail.replace(/\s+/g, " ").slice(0, 160),
  claudeFilterRows: famCells.length,
  claudeFilterAllClaude: allClaude,
  usageHead,
  laneCount,
  jsErrors: errors,
}, null, 1));
await browser.close();
if (errors.length || !allClaude || (hasHistory && !(logTab === "gw" ? /会话 ID/ : /模型/).test(detail))) process.exitCode = 1;
