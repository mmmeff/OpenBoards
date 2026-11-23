#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  MAIN_PACKAGE,
  PLATFORMS,
  REPO_NAME,
  REPO_OWNER,
  WORKFLOW_FILE,
} from "./stage-npm-packages.mjs";

const BOOTSTRAP_VERSION = "0.0.0-bootstrap";
const REPOSITORY = {
  type: "git",
  url: `git+https://github.com/${REPO_OWNER}/${REPO_NAME}.git`,
};

const ALL_PACKAGES = [
  ...PLATFORMS.map((p) => ({
    name: p.pkgName,
    description: `Prebuilt openboards binary for ${p.label} (OIDC bootstrap placeholder)`,
    os: p.os,
    cpu: p.cpu,
    ...(p.libc ? { libc: p.libc } : {}),
  })),
  {
    name: MAIN_PACKAGE,
    description: "Clean-room, self-hostable single-binary implementation of Vercel Boards (OIDC bootstrap placeholder)",
  },
];

function runNpm(args, cwd) {
  return spawnSync("npm", args, { cwd, encoding: "utf8" });
}

function parseArgs(argv) {
  let publish = false;
  for (const arg of argv) {
    if (arg === "--publish") {
      publish = true;
    } else if (arg === "--dry-run") {
      publish = false;
    } else if (arg === "-h" || arg === "--help") {
      console.log(
        [
          "One-time bootstrap helper for npm Trusted Publishing (GitHub Actions OIDC).",
          "",
          "npm requires packages to exist on npmjs.com before Trusted Publishing can be",
          "configured. This script publishes a minimal 0.0.0-bootstrap placeholder for each",
          "of the 6 OpenBoards packages and prints the exact Trusted Publisher setup URLs.",
          "",
          "Usage:",
          "  node scripts/bootstrap-npm-oidc.mjs             # dry-run (default)",
          "  node scripts/bootstrap-npm-oidc.mjs --dry-run   # preview only",
          "  node scripts/bootstrap-npm-oidc.mjs --publish   # publish missing placeholders",
        ].join("\n")
      );
      process.exit(0);
    } else {
      console.error(`Unknown argument: ${arg}`);
      process.exit(1);
    }
  }
  return { publish };
}

function printTrustedPublisherInstructions() {
  console.log("\n--- Next Step: Configure npm Trusted Publishing (OIDC) ---\n");
  console.log("Open each package access page below and add a Trusted Publisher:\n");
  for (const pkg of ALL_PACKAGES) {
    console.log(`  https://www.npmjs.com/package/${pkg.name}/access`);
  }
  console.log("\nUse these exact Trusted Publisher settings on all 6 packages:\n");
  console.log("  Provider:          GitHub Actions");
  console.log(`  Repository owner:  ${REPO_OWNER}`);
  console.log(`  Repository name:   ${REPO_NAME}`);
  console.log(`  Workflow filename: ${WORKFLOW_FILE}`);
  console.log("  Environment name:  (leave blank)");
  console.log(
    "\nOnce configured on all 6 packages, cut your first real release by pushing a version tag (e.g. `git tag v0.1.0 && git push origin v0.1.0`).\n" +
      "No NPM_TOKEN secret is required in GitHub Actions."
  );
}

function main() {
  const { publish } = parseArgs(process.argv.slice(2));

  if (!publish) {
    console.log(`[dry-run] Would check and publish ${BOOTSTRAP_VERSION} placeholders for ${ALL_PACKAGES.length} packages:\n`);
    for (const pkg of ALL_PACKAGES) {
      console.log(`  - ${pkg.name}@${BOOTSTRAP_VERSION}`);
    }
    console.log("\nRe-run with --publish (after `npm login`) to create any missing packages on npmjs.com.");
    printTrustedPublisherInstructions();
    return;
  }

  const whoami = runNpm(["whoami"], process.cwd());
  if (whoami.status !== 0) {
    console.error("Not logged in to npm. Run `npm login` first, then re-run with --publish.");
    process.exit(1);
  }
  console.log(`Authenticated to npm as: ${whoami.stdout.trim()}\n`);

  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "openboards-oidc-bootstrap-"));
  try {
    for (const pkg of ALL_PACKAGES) {
      const existing = runNpm(["view", pkg.name, "version"], tmpRoot);
      if (existing.status === 0 && existing.stdout.trim()) {
        console.log(`Skipping ${pkg.name}: already exists on npm (latest: ${existing.stdout.trim()})`);
        continue;
      }

      const dir = path.join(tmpRoot, pkg.name);
      fs.mkdirSync(dir, { recursive: true });
      const pkgJson = {
        name: pkg.name,
        version: BOOTSTRAP_VERSION,
        description: pkg.description,
        ...(pkg.os ? { os: pkg.os } : {}),
        ...(pkg.cpu ? { cpu: pkg.cpu } : {}),
        ...(pkg.libc ? { libc: pkg.libc } : {}),
        repository: REPOSITORY,
      };
      fs.writeFileSync(path.join(dir, "package.json"), `${JSON.stringify(pkgJson, null, 2)}\n`);
      fs.writeFileSync(
        path.join(dir, "README.md"),
        `# ${pkg.name}\n\nPlaceholder release published to initialize npm Trusted Publishing (OIDC) for https://github.com/${REPO_OWNER}/${REPO_NAME}.\n`
      );

      console.log(`Publishing ${pkg.name}@${BOOTSTRAP_VERSION}...`);
      const res = runNpm(["publish", "--access", "public"], dir);
      if (res.status !== 0) {
        console.error(res.stdout);
        console.error(res.stderr);
        throw new Error(`Failed to publish ${pkg.name}@${BOOTSTRAP_VERSION}`);
      }
      console.log(`Published ${pkg.name}@${BOOTSTRAP_VERSION}`);
    }
  } finally {
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  }

  printTrustedPublisherInstructions();
}

main();
