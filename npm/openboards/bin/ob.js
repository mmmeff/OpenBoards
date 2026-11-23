#!/usr/bin/env node
"use strict";

const { spawn } = require("node:child_process");

const PLATFORM_PACKAGES = {
  "darwin-arm64": { pkg: "openboards-darwin-arm64", bin: "bin/ob" },
  "darwin-x64": { pkg: "openboards-darwin-x64", bin: "bin/ob" },
  "linux-x64": { pkg: "openboards-linux-x64-gnu", bin: "bin/ob" },
  "linux-arm64": { pkg: "openboards-linux-arm64-gnu", bin: "bin/ob" },
  "win32-x64": { pkg: "openboards-win32-x64-msvc", bin: "bin/ob.exe" },
};

function isMusl() {
  if (process.platform !== "linux") return false;
  try {
    const report = process.report && process.report.getReport();
    return !(report && report.header && report.header.glibcVersionRuntime);
  } catch {
    return false;
  }
}

function fail(message) {
  process.stderr.write(`openboards: ${message}\n`);
  process.exit(1);
}

function resolveBinary() {
  const override = process.env.OPENBOARDS_BINARY_PATH;
  if (override && override.trim()) {
    return override.trim();
  }

  const key = `${process.platform}-${process.arch}`;
  const entry = PLATFORM_PACKAGES[key];
  if (!entry) {
    fail(
      `unsupported platform "${key}". Supported prebuilt platforms: ${Object.keys(PLATFORM_PACKAGES).join(", ")}.\n` +
        `Build from source with "cargo build --release" (https://github.com/mmmeff/OpenBoards) ` +
        `or point OPENBOARDS_BINARY_PATH to a local "ob" binary.`
    );
  }

  try {
    return require.resolve(`${entry.pkg}/${entry.bin}`);
  } catch {
    if (isMusl()) {
      fail(
        `prebuilt Linux binaries require glibc (musl/Alpine is not currently distributed via npm).\n` +
          `Build from source with "cargo build --release" or point OPENBOARDS_BINARY_PATH to a local "ob" binary.`
      );
    }
    fail(
      `platform package "${entry.pkg}" is not installed.\n` +
        `If you installed with "--no-optional" or "--omit=optional", reinstall without skipping optional dependencies,\n` +
        `or set OPENBOARDS_BINARY_PATH to a local "ob" binary.`
    );
  }
}

const binPath = resolveBinary();
const child = spawn(binPath, process.argv.slice(2), { stdio: "inherit" });

const forwardSignals = ["SIGINT", "SIGTERM"];
const listeners = new Map();

for (const sig of forwardSignals) {
  const handler = () => {
    try {
      child.kill(sig);
    } catch {
      // Child already exited
    }
  };
  listeners.set(sig, handler);
  process.on(sig, handler);
}

function cleanupListeners() {
  for (const [sig, handler] of listeners) {
    process.removeListener(sig, handler);
  }
}

child.on("error", (err) => {
  cleanupListeners();
  if (err.code === "ENOENT" && !process.env.OPENBOARDS_BINARY_PATH && isMusl()) {
    fail(
      `failed to execute "${binPath}" (ENOENT). Prebuilt Linux binaries require glibc (musl/Alpine is unsupported).\n` +
        `Build from source with "cargo build --release" or set OPENBOARDS_BINARY_PATH.`
    );
  }
  fail(`failed to launch "${binPath}": ${err.message}`);
});

child.on("exit", (code, signal) => {
  cleanupListeners();
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code === null ? 1 : code);
});
