import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const requestedTag = process.argv[2];

if (process.argv.length > 3) {
  fail("usage: node scripts/verify-release-version.mjs [v<semver>]");
}

const client = await readJSON("packages/client/package.json");
const adapter = await readJSON("packages/ai-sdk/package.json");
const chart = await readFile(join(repositoryRoot, "deploy/helm/streamweld/Chart.yaml"), "utf8");

const versions = new Map([
  ["@streamweld/client", client.version],
  ["@streamweld/ai-sdk", adapter.version],
  ["Helm chart", chartField(chart, "version")],
  ["Helm appVersion", chartField(chart, "appVersion")],
]);

const baseline = client.version;
if (!isSemVer(baseline)) {
  fail(`@streamweld/client has an invalid semantic version: ${JSON.stringify(baseline)}`);
}
if (baseline.includes("+")) {
  fail("semantic-version build metadata is not supported by every release registry");
}

for (const [artifact, version] of versions) {
  if (!isSemVer(version)) {
    fail(`${artifact} has an invalid semantic version: ${JSON.stringify(version)}`);
  }
  if (version !== baseline) {
    fail(`${artifact} version ${version} does not match release version ${baseline}`);
  }
}

if (requestedTag !== undefined) {
  const match = /^v(.+)$/.exec(requestedTag);
  if (match === null || !isSemVer(match[1])) {
    fail(`release tag must be v<semver>; received ${JSON.stringify(requestedTag)}`);
  }
  if (match[1] !== baseline) {
    fail(`release tag ${requestedTag} does not match artifact version ${baseline}`);
  }
}

process.stdout.write(`${baseline}\n`);

async function readJSON(relativePath) {
  const contents = await readFile(join(repositoryRoot, relativePath), "utf8");
  return JSON.parse(contents);
}

function chartField(contents, field) {
  const match = new RegExp(`^${field}:\\s*["']?([^"'\\s]+)["']?\\s*$`, "m").exec(contents);
  if (match === null) {
    fail(`deploy/helm/streamweld/Chart.yaml is missing ${field}`);
  }
  return match[1];
}

function isSemVer(value) {
  return typeof value === "string" &&
    /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/.test(value);
}

function fail(message) {
  process.stderr.write(`release version check: ${message}\n`);
  process.exit(1);
}
