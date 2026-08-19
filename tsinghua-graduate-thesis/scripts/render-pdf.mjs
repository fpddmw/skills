#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import {
  accessSync,
  constants,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, isAbsolute, join, resolve } from "node:path";
import process from "node:process";

const FATAL_RENDERER_PATTERNS = [
  /Missing language pack/i,
  /Unknown font tag/i,
  /No font in show(?:\/space)?/i,
  /Couldn['’]t find .* CMap file/i,
];
const MIN_VISIBLE_PIXELS = 100;
const MAX_BUFFER = 16 * 1024 * 1024;

function usage() {
  return `Usage: node scripts/render-pdf.mjs --input <pdf> --output-dir <dir> [options]

Options:
  --first-page <n>   First page to render (default: 1)
  --last-page <n>    Last page to render (default: first page)
  --probe-page <n>   Known nonblank page used to validate the renderer
  --dpi <n>          PNG resolution from 36 to 600 (default: 150)
  --candidate <path> Try an exact pdftoppm executable; may be repeated
  --json             Emit one machine-readable result
  --help             Show this help

Without --candidate, PDFTOPPM, every pdftoppm on PATH, and common Homebrew
locations are tried in order. A renderer that emits language-pack/font errors
or produces a blank probe is rejected before any visual conclusion is made.`;
}

function fail(message) {
  const error = new Error(message);
  error.isUsageError = true;
  throw error;
}

function parsePositiveInteger(raw, option, minimum = 1, maximum = 100000) {
  if (!/^\d+$/.test(raw ?? "")) fail(`${option} requires an integer`);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    fail(`${option} must be between ${minimum} and ${maximum}`);
  }
  return value;
}

function parseArgs(argv) {
  const options = {
    candidates: [],
    dpi: 150,
    firstPage: 1,
    input: null,
    json: false,
    lastPage: null,
    outputDir: null,
    probePage: null,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--help") return { help: true };
    if (arg === "--json") {
      options.json = true;
      continue;
    }
    const valueOptions = new Set([
      "--candidate",
      "--dpi",
      "--first-page",
      "--input",
      "--last-page",
      "--output-dir",
      "--probe-page",
    ]);
    if (!valueOptions.has(arg)) fail(`unknown option: ${arg}`);
    const value = argv[index + 1];
    if (!value || value.startsWith("--")) fail(`${arg} requires a value`);
    index += 1;
    if (arg === "--candidate") options.candidates.push(value);
    if (arg === "--dpi") options.dpi = parsePositiveInteger(value, arg, 36, 600);
    if (arg === "--first-page") {
      options.firstPage = parsePositiveInteger(value, arg);
    }
    if (arg === "--input") options.input = value;
    if (arg === "--last-page") {
      options.lastPage = parsePositiveInteger(value, arg);
    }
    if (arg === "--output-dir") options.outputDir = value;
    if (arg === "--probe-page") {
      options.probePage = parsePositiveInteger(value, arg);
    }
  }

  if (!options.input) fail("--input is required");
  if (!options.outputDir) fail("--output-dir is required");
  options.lastPage ??= options.firstPage;
  options.probePage ??= options.firstPage;
  if (options.lastPage < options.firstPage) {
    fail("--last-page must be greater than or equal to --first-page");
  }
  if (options.lastPage - options.firstPage > 999) {
    fail("a single render may contain at most 1000 pages");
  }
  return options;
}

function isExecutable(path) {
  try {
    accessSync(path, constants.X_OK);
    return statSync(path).isFile();
  } catch {
    return false;
  }
}

function discoverCandidates(explicitCandidates) {
  const raw = [];
  if (explicitCandidates.length > 0) {
    raw.push(...explicitCandidates);
  } else {
    if (process.env.PDFTOPPM?.trim()) raw.push(process.env.PDFTOPPM.trim());
    for (const directory of (process.env.PATH ?? "").split(delimiter)) {
      if (directory) raw.push(join(directory, "pdftoppm"));
    }
    raw.push("/opt/homebrew/bin/pdftoppm", "/usr/local/bin/pdftoppm", "/usr/bin/pdftoppm");
  }

  const unique = [];
  const seen = new Set();
  for (const candidate of raw) {
    const absolute = isAbsolute(candidate) ? candidate : resolve(candidate);
    if (seen.has(absolute)) continue;
    seen.add(absolute);
    if (isExecutable(absolute)) unique.push(absolute);
  }
  return unique;
}

function run(candidate, args) {
  return spawnSync(candidate, args, {
    encoding: "utf8",
    maxBuffer: MAX_BUFFER,
  });
}

function rendererVersion(candidate) {
  const result = run(candidate, ["-v"]);
  const combined = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;
  return combined
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find(Boolean) ?? "unknown";
}

function fatalDiagnostics(result) {
  const combined = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;
  return combined
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && FATAL_RENDERER_PATTERNS.some((pattern) => pattern.test(line)));
}

function parsePpm(path) {
  const buffer = readFileSync(path);
  let offset = 0;

  function skipSpaceAndComments() {
    while (offset < buffer.length) {
      const byte = buffer[offset];
      if (byte === 35) {
        while (offset < buffer.length && buffer[offset] !== 10) offset += 1;
        continue;
      }
      if (byte === 9 || byte === 10 || byte === 13 || byte === 32) {
        offset += 1;
        continue;
      }
      break;
    }
  }

  function token() {
    skipSpaceAndComments();
    const start = offset;
    while (offset < buffer.length) {
      const byte = buffer[offset];
      if (byte === 9 || byte === 10 || byte === 13 || byte === 32 || byte === 35) break;
      offset += 1;
    }
    if (start === offset) throw new Error("invalid PPM header");
    return buffer.toString("ascii", start, offset);
  }

  const magic = token();
  const width = Number(token());
  const height = Number(token());
  const maxValue = Number(token());
  if (magic !== "P6" || !Number.isInteger(width) || !Number.isInteger(height) || maxValue !== 255) {
    throw new Error("unsupported PPM output");
  }
  if (buffer[offset] === 13 && buffer[offset + 1] === 10) offset += 2;
  else if ([9, 10, 13, 32].includes(buffer[offset])) offset += 1;

  const expectedBytes = width * height * 3;
  if (buffer.length - offset < expectedBytes) throw new Error("truncated PPM output");
  let nonWhitePixels = 0;
  for (let index = offset; index < offset + expectedBytes; index += 3) {
    if (buffer[index] < 250 || buffer[index + 1] < 250 || buffer[index + 2] < 250) {
      nonWhitePixels += 1;
    }
  }
  return { height, nonWhitePixels, totalPixels: width * height, width };
}

function probeRenderer(candidate, input, page) {
  const probeDir = mkdtempSync(join(tmpdir(), "tsinghua-pdftoppm-probe-"));
  const prefix = join(probeDir, "page");
  try {
    const result = run(candidate, [
      "-f",
      String(page),
      "-l",
      String(page),
      "-singlefile",
      "-r",
      "72",
      input,
      prefix,
    ]);
    const diagnostics = fatalDiagnostics(result);
    if (result.error) {
      return {
        diagnostics: [result.error.message],
        exitCode: null,
        status: "renderer_process_error",
      };
    }
    if (diagnostics.length > 0) {
      return {
        diagnostics,
        exitCode: result.status,
        status: "renderer_environment_error",
      };
    }
    if (result.status !== 0) {
      return {
        diagnostics: [result.stderr.trim() || `pdftoppm exited ${result.status}`],
        exitCode: result.status,
        status: "renderer_process_error",
      };
    }
    const ppm = `${prefix}.ppm`;
    if (!existsSync(ppm)) {
      return {
        diagnostics: ["pdftoppm returned success without a PPM probe"],
        exitCode: result.status,
        status: "renderer_environment_error",
      };
    }
    const metrics = parsePpm(ppm);
    if (metrics.nonWhitePixels < MIN_VISIBLE_PIXELS) {
      return {
        diagnostics: [
          `probe page contains only ${metrics.nonWhitePixels} non-white pixels; choose a known nonblank --probe-page or repair the renderer`,
        ],
        exitCode: result.status,
        probe: metrics,
        status: "renderer_probe_inconclusive",
      };
    }
    return {
      diagnostics: [],
      exitCode: result.status,
      probe: metrics,
      status: "ok",
    };
  } catch (error) {
    return {
      diagnostics: [error.message],
      exitCode: null,
      status: "renderer_process_error",
    };
  } finally {
    rmSync(probeDir, { recursive: true, force: true });
  }
}

function renderPages(candidate, input, outputDir, firstPage, lastPage, dpi) {
  const outputs = [];
  for (let page = firstPage; page <= lastPage; page += 1) {
    const prefix = join(outputDir, `page-${page}`);
    const expected = `${prefix}.png`;
    if (existsSync(expected)) {
      return {
        diagnostics: [`refusing to overwrite existing output: ${expected}`],
        outputs,
        status: "output_conflict",
      };
    }
    const result = run(candidate, [
      "-f",
      String(page),
      "-l",
      String(page),
      "-singlefile",
      "-png",
      "-r",
      String(dpi),
      input,
      prefix,
    ]);
    const diagnostics = fatalDiagnostics(result);
    if (diagnostics.length > 0) {
      rmSync(expected, { force: true });
      return { diagnostics, outputs, status: "renderer_environment_error" };
    }
    if (result.error || result.status !== 0 || !existsSync(expected) || statSync(expected).size === 0) {
      rmSync(expected, { force: true });
      return {
        diagnostics: [
          result.error?.message ?? result.stderr.trim() ?? `pdftoppm did not create page ${page}`,
        ],
        outputs,
        status: "renderer_process_error",
      };
    }
    outputs.push(expected);
  }
  return { diagnostics: [], outputs, status: "ok" };
}

function cleanFailedOutputs(outputs) {
  for (const output of outputs ?? []) rmSync(output, { force: true });
}

function formatHuman(payload) {
  if (payload.status === "ok") {
    return [
      `Renderer: ${payload.renderer}`,
      `Probe page ${payload.probe.page}: ${payload.probe.nonWhitePixels} visible pixels`,
      ...payload.outputs.map((output) => `Rendered: ${output}`),
    ].join("\n");
  }
  const lines = [
    "PDF rendering is inconclusive because no healthy pdftoppm candidate was found.",
    "Treat this as a renderer environment error, not as missing thesis content.",
  ];
  for (const attempt of payload.attempts) {
    lines.push(`${attempt.candidate}: ${attempt.status}`);
    for (const diagnostic of attempt.diagnostics) lines.push(`  ${diagnostic}`);
  }
  return lines.join("\n");
}

function main() {
  let options;
  try {
    options = parseArgs(process.argv.slice(2));
  } catch (error) {
    console.error(error.message);
    console.error(usage());
    return 2;
  }
  if (options.help) {
    console.log(usage());
    return 0;
  }

  const input = resolve(options.input);
  const outputDir = resolve(options.outputDir);
  if (!existsSync(input) || !statSync(input).isFile()) {
    console.error(`input PDF does not exist or is not a regular file: ${input}`);
    return 2;
  }
  mkdirSync(outputDir, { recursive: true });

  const candidates = discoverCandidates(options.candidates);
  const attempts = [];
  for (const candidate of candidates) {
    const version = rendererVersion(candidate);
    const probe = probeRenderer(candidate, input, options.probePage);
    const attempt = {
      candidate,
      diagnostics: probe.diagnostics,
      exitCode: probe.exitCode,
      probe: probe.probe ?? null,
      status: probe.status,
      version,
    };
    attempts.push(attempt);
    if (probe.status !== "ok") continue;

    const rendered = renderPages(
      candidate,
      input,
      outputDir,
      options.firstPage,
      options.lastPage,
      options.dpi,
    );
    if (rendered.status !== "ok") {
      attempt.status = rendered.status;
      attempt.diagnostics.push(...rendered.diagnostics);
      cleanFailedOutputs(rendered.outputs);
      if (rendered.status === "output_conflict") break;
      continue;
    }

    const payload = {
      attempts,
      dpi: options.dpi,
      firstPage: options.firstPage,
      input,
      lastPage: options.lastPage,
      outputs: rendered.outputs,
      probe: { page: options.probePage, ...probe.probe },
      renderer: candidate,
      rendererVersion: version,
      schemaVersion: 1,
      status: "ok",
    };
    console.log(options.json ? JSON.stringify(payload) : formatHuman(payload));
    return 0;
  }

  const payload = {
    attempts,
    input,
    schemaVersion: 1,
    status: "renderer_environment_error",
  };
  if (options.json) console.log(JSON.stringify(payload));
  else console.error(formatHuman(payload));
  return 3;
}

process.exitCode = main();
