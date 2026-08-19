import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const testDir = dirname(fileURLToPath(import.meta.url));
const skillRoot = resolve(testDir, "..", "..");
const renderer = join(skillRoot, "scripts", "render-pdf.mjs");
const fixture = join(testDir, "fixtures", "adobe-gb1-no-tounicode.pdf");
const provenance = JSON.parse(
  readFileSync(
    join(testDir, "fixtures", "adobe-gb1-no-tounicode.provenance.json"),
    "utf8",
  ),
);
const brokenRenderer = join(testDir, "fixtures", "broken-pdftoppm.sh");
const healthyRenderer = "/usr/bin/pdftoppm";

function runRenderer(candidates) {
  const outputDir = mkdtempSync(join(tmpdir(), "tsinghua-pdf-render-"));
  const args = [
    renderer,
    "--input",
    fixture,
    "--output-dir",
    outputDir,
    "--first-page",
    "1",
    "--last-page",
    "1",
    "--dpi",
    "150",
    "--json",
  ];
  for (const candidate of candidates) {
    args.push("--candidate", candidate);
  }
  const result = spawnSync(process.execPath, args, {
    encoding: "utf8",
    env: { ...process.env, PDFTOPPM: "" },
  });
  let payload;
  try {
    payload = JSON.parse(result.stdout);
  } catch {
    payload = null;
  }
  return { result, payload, outputDir };
}

test("the regression fixture is a real embedded Adobe-GB1 PDF", () => {
  const digest = createHash("sha256").update(readFileSync(fixture)).digest("hex");
  assert.equal(digest, provenance.fixture.sha256);
  assert.equal(provenance.pdf.fontType, "CID Type 0C");
  assert.equal(provenance.pdf.toUnicodePresent, false);

  const fonts = spawnSync("pdffonts", [fixture], { encoding: "utf8" });
  assert.equal(fonts.status, 0, fonts.stderr);
  assert.match(fonts.stdout, /CID Type 0C/);
  assert.match(fonts.stdout, /Identity-H/);
  assert.match(fonts.stdout, /yes\s+yes\s+no/);
});

test("a healthy Poppler renders visible pixels from the real PDF", () => {
  const { result, payload, outputDir } = runRenderer([healthyRenderer]);
  try {
    assert.equal(result.status, 0, result.stderr || result.stdout);
    assert.equal(payload?.status, "ok");
    assert.equal(payload?.renderer, healthyRenderer);
    assert.ok(payload?.probe?.nonWhitePixels > 1000);
    assert.equal(payload?.outputs?.length, 1);
    assert.ok(existsSync(payload.outputs[0]));
  } finally {
    rmSync(outputDir, { recursive: true, force: true });
  }
});

test("a relocated poppler-data failure is not reported as thesis content loss", () => {
  const { result, payload, outputDir } = runRenderer([brokenRenderer]);
  try {
    assert.equal(result.status, 3, result.stderr || result.stdout);
    assert.equal(payload?.status, "renderer_environment_error");
    assert.equal(payload?.attempts?.length, 1);
    assert.equal(payload?.attempts?.[0]?.status, "renderer_environment_error");
    assert.ok(
      payload?.attempts?.[0]?.diagnostics?.some((item) =>
        item.includes("Missing language pack"),
      ),
    );
  } finally {
    rmSync(outputDir, { recursive: true, force: true });
  }
});

test("the renderer falls through from broken Poppler to a healthy candidate", () => {
  const { result, payload, outputDir } = runRenderer([
    brokenRenderer,
    healthyRenderer,
  ]);
  try {
    assert.equal(result.status, 0, result.stderr || result.stdout);
    assert.equal(payload?.status, "ok");
    assert.equal(payload?.renderer, healthyRenderer);
    assert.equal(payload?.attempts?.length, 2);
    assert.equal(payload?.attempts?.[0]?.status, "renderer_environment_error");
    assert.equal(payload?.attempts?.[1]?.status, "ok");
  } finally {
    rmSync(outputDir, { recursive: true, force: true });
  }
});
