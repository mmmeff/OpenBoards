#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export const REPO_OWNER = "mmmeff";
export const REPO_NAME = "OpenBoards";
export const WORKFLOW_FILE = "release.yml";
export const MAIN_PACKAGE = "openboards";

export const PLATFORMS = [
  {
    pkgName: "openboards-darwin-arm64",
    slug: "darwin-arm64",
    target: "aarch64-apple-darwin",
    os: ["darwin"],
    cpu: ["arm64"],
    binName: "ob",
    label: "macOS arm64",
  },
  {
    pkgName: "openboards-darwin-x64",
    slug: "darwin-x64",
    target: "x86_64-apple-darwin",
    os: ["darwin"],
    cpu: ["x64"],
    binName: "ob",
    label: "macOS x64",
  },
  {
    pkgName: "openboards-linux-x64-gnu",
    slug: "linux-x64-gnu",
    target: "x86_64-unknown-linux-gnu",
    os: ["linux"],
    cpu: ["x64"],
    libc: ["glibc"],
    binName: "ob",
    label: "Linux x64 (glibc)",
  },
  {
    pkgName: "openboards-linux-arm64-gnu",
    slug: "linux-arm64-gnu",
    target: "aarch64-unknown-linux-gnu",
    os: ["linux"],
    cpu: ["arm64"],
    libc: ["glibc"],
    binName: "ob",
    label: "Linux arm64 (glibc)",
  },
  {
    pkgName: "openboards-win32-x64-msvc",
    slug: "win32-x64-msvc",
    target: "x86_64-pc-windows-msvc",
    os: ["win32"],
    cpu: ["x64"],
    binName: "ob.exe",
    label: "Windows x64 (MSVC)",
  },
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = path.resolve(__dirname, "..");

export function readCargoVersion(repoRoot = REPO_ROOT) {
  const cargoToml = fs.readFileSync(path.join(repoRoot, "Cargo.toml"), "utf8");
  const pkgSection = cargoToml.split(/^\[/m).find((s) => s.startsWith("package]"));
  if (!pkgSection) {
    throw new Error("Could not find [package] section in Cargo.toml");
  }
  const match = pkgSection.match(/^\s*version\s*=\s*"([^"]+)"/m);
  if (!match) {
    throw new Error("Could not find package version in Cargo.toml");
  }
  return match[1];
}

function readMainPackageJson(repoRoot = REPO_ROOT) {
  const p = path.join(repoRoot, "npm", MAIN_PACKAGE, "package.json");
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function findRootLicenseFiles(repoRoot = REPO_ROOT) {
  return fs
    .readdirSync(repoRoot)
    .filter((name) => /^LICENSE(\..+)?$/i.test(name) && fs.statSync(path.join(repoRoot, name)).isFile())
    .sort();
}

export function findArtifactBinary(artifactsDir, platform) {
  const candidates = [
    path.join(artifactsDir, `bin-${platform.slug}`, platform.binName),
    path.join(artifactsDir, platform.slug, platform.binName),
    path.join(artifactsDir, platform.pkgName, platform.binName),
    path.join(artifactsDir, platform.pkgName, "bin", platform.binName),
    path.join(artifactsDir, platform.target, platform.binName),
    path.join(artifactsDir, platform.target, "release", platform.binName),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
      return candidate;
    }
  }
  return null;
}

function verifyVersionsMatch(version, cargoVersion, mainPkg) {
  if (cargoVersion !== version) {
    throw new Error(`Version mismatch: --version=${version} does not match Cargo.toml version=${cargoVersion}`);
  }
  if (mainPkg.version !== version) {
    throw new Error(
      `Version mismatch: --version=${version} does not match npm/${MAIN_PACKAGE}/package.json version=${mainPkg.version}`
    );
  }
  const optDeps = mainPkg.optionalDependencies || {};
  for (const platform of PLATFORMS) {
    if (optDeps[platform.pkgName] !== version) {
      throw new Error(
        `Version mismatch: npm/${MAIN_PACKAGE}/package.json optionalDependencies["${platform.pkgName}"]=${optDeps[platform.pkgName]} (expected ${version})`
      );
    }
  }
}

function parseArgs(argv) {
  const opts = {
    version: null,
    artifactsDir: null,
    outDir: path.join(REPO_ROOT, "dist", "npm"),
    checkVersion: false,
    checkOnly: false,
    allowPartial: false,
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    switch (arg) {
      case "--version":
        opts.version = argv[++i];
        break;
      case "--artifacts-dir":
        opts.artifactsDir = argv[++i];
        break;
      case "--out-dir":
        opts.outDir = argv[++i];
        break;
      case "--check-version":
        opts.checkVersion = true;
        break;
      case "--check-only":
        opts.checkOnly = true;
        opts.checkVersion = true;
        break;
      case "--allow-partial":
        opts.allowPartial = true;
        break;
      case "-h":
      case "--help":
        console.log(
          [
            "Usage: node scripts/stage-npm-packages.mjs [options]",
            "",
            "Options:",
            "  --version <X.Y.Z>      Version to stamp (defaults to Cargo.toml)",
            "  --artifacts-dir <dir>  Directory containing built binaries per target",
            "  --out-dir <dir>        Output directory for staged npm packages (default: dist/npm)",
            "  --check-version        Verify Cargo.toml and npm/openboards/package.json match --version",
            "  --check-only           Only run --check-version and exit",
            "  --allow-partial        Stage only targets found in --artifacts-dir (for local/CI smoke tests)",
          ].join("\n")
        );
        process.exit(0);
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  return opts;
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  const cargoVersion = readCargoVersion();
  const mainPkg = readMainPackageJson();
  const version = (opts.version || cargoVersion).replace(/^v/, "");

  if (!/^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/.test(version)) {
    throw new Error(`Invalid semver version: "${version}"`);
  }

  if (opts.checkVersion) {
    verifyVersionsMatch(version, cargoVersion, mainPkg);
    console.log(`Version check passed: ${version}`);
    if (opts.checkOnly) {
      return;
    }
  }

  if (!opts.artifactsDir) {
    throw new Error("--artifacts-dir is required");
  }

  const artifactsDir = path.resolve(opts.artifactsDir);
  const outDir = path.resolve(opts.outDir);
  const licenseFiles = findRootLicenseFiles();

  const resolved = PLATFORMS.map((platform) => ({
    platform,
    binaryPath: findArtifactBinary(artifactsDir, platform),
  }));

  const missing = resolved.filter((r) => !r.binaryPath);
  const present = resolved.filter((r) => r.binaryPath);

  if (!opts.allowPartial && missing.length > 0) {
    throw new Error(
      `Missing built binaries in ${artifactsDir} for: ${missing.map((m) => `${m.platform.pkgName} (${m.platform.target})`).join(", ")}`
    );
  }
  if (present.length === 0) {
    throw new Error(`No platform binaries found in ${artifactsDir}`);
  }

  fs.mkdirSync(outDir, { recursive: true });

  // Stage platform packages
  for (const { platform, binaryPath } of present) {
    const pkgDir = path.join(outDir, platform.pkgName);
    fs.rmSync(pkgDir, { recursive: true, force: true });
    const binDir = path.join(pkgDir, "bin");
    fs.mkdirSync(binDir, { recursive: true });

    const destBin = path.join(binDir, platform.binName);
    fs.copyFileSync(binaryPath, destBin);
    if (platform.os[0] !== "win32") {
      fs.chmodSync(destBin, 0o755);
    }

    const files = ["bin/", "README.md", ...licenseFiles];
    const pkgJson = {
      name: platform.pkgName,
      version,
      description: `Prebuilt openboards binary for ${platform.label}`,
      os: platform.os,
      cpu: platform.cpu,
      ...(platform.libc ? { libc: platform.libc } : {}),
      files,
      preferUnplugged: true,
      repository: mainPkg.repository,
      homepage: mainPkg.homepage,
      bugs: mainPkg.bugs,
      ...(mainPkg.license ? { license: mainPkg.license } : {}),
    };
    fs.writeFileSync(path.join(pkgDir, "package.json"), `${JSON.stringify(pkgJson, null, 2)}\n`);
    fs.writeFileSync(
      path.join(pkgDir, "README.md"),
      `# ${platform.pkgName}\n\nPlatform-specific binary for [\`${MAIN_PACKAGE}\`](https://www.npmjs.com/package/${MAIN_PACKAGE}) (${platform.label}, target \`${platform.target}\`).\n\nInstall via \`npx ${MAIN_PACKAGE}\` or \`npm install -g ${MAIN_PACKAGE}\`.\n`
    );
    for (const lic of licenseFiles) {
      fs.copyFileSync(path.join(REPO_ROOT, lic), path.join(pkgDir, lic));
    }

    console.log(`Staged ${platform.pkgName}@${version} <- ${binaryPath}`);
  }

  // Stage main wrapper package
  const mainOutDir = path.join(outDir, MAIN_PACKAGE);
  fs.rmSync(mainOutDir, { recursive: true, force: true });
  fs.mkdirSync(path.join(mainOutDir, "bin"), { recursive: true });

  const launcherSrc = path.join(REPO_ROOT, "npm", MAIN_PACKAGE, "bin", "ob.js");
  const launcherDest = path.join(mainOutDir, "bin", "ob.js");
  fs.copyFileSync(launcherSrc, launcherDest);
  fs.chmodSync(launcherDest, 0o755);

  fs.copyFileSync(path.join(REPO_ROOT, "README.md"), path.join(mainOutDir, "README.md"));
  for (const lic of licenseFiles) {
    fs.copyFileSync(path.join(REPO_ROOT, lic), path.join(mainOutDir, lic));
  }

  const optDeps = {};
  const depsSource = opts.allowPartial ? present.map((r) => r.platform) : PLATFORMS;
  for (const platform of depsSource) {
    optDeps[platform.pkgName] = version;
  }

  const stagedMainPkg = {
    ...mainPkg,
    version,
    files: [...new Set([...(mainPkg.files || ["bin/", "README.md"]), ...licenseFiles])],
    optionalDependencies: optDeps,
  };
  fs.writeFileSync(path.join(mainOutDir, "package.json"), `${JSON.stringify(stagedMainPkg, null, 2)}\n`);
  console.log(
    `Staged ${MAIN_PACKAGE}@${version} (${Object.keys(optDeps).length} platform optionalDependencies${opts.allowPartial ? ", partial mode" : ""})`
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    main();
  } catch (err) {
    console.error(`stage-npm-packages: ${err.message}`);
    process.exit(1);
  }
}
