import { lstat, readFile } from "node:fs/promises";
import { isAbsolute, resolve } from "node:path";

const MAX_PROFILE_BYTES = 32 * 1024;
const MAX_CREDENTIAL_BYTES = 8 * 1024;
const LOOPBACK_HOSTS = new Set(["127.0.0.1", "[::1]"]);

export interface DesktopProfile {
  readonly schemaVersion: 1;
  readonly origin: string;
  readonly participantId: string;
  readonly requestKind: string;
  readonly resultKind: string;
  readonly channelId?: string;
  readonly operatorCredentialFile: string;
  readonly participantCredentialFile: string;
}

export interface LoadedDesktopProfile extends DesktopProfile {
  operatorCredential: string;
  participantCredential: string;
}

export function parseProfilePath(
  argv: readonly string[],
  defaultPath: string,
): string {
  const values: string[] = [];
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--profile") {
      const value = argv[index + 1];
      if (!value || value.startsWith("--")) {
        throw new Error("--profile requires an absolute path");
      }
      values.push(value);
      index += 1;
    } else if (argument?.startsWith("--profile=")) {
      values.push(argument.slice("--profile=".length));
    }
  }
  if (values.length > 1) throw new Error("--profile may be supplied only once");
  const selected = values[0] ?? defaultPath;
  if (!isAbsolute(selected)) {
    throw new Error("the desktop profile path must be absolute");
  }
  return resolve(selected);
}

export async function loadDesktopProfile(
  profilePath: string,
): Promise<LoadedDesktopProfile> {
  await requirePrivateRegularFile(profilePath, "desktop profile");
  const profileSource = await readBoundedFile(
    profilePath,
    MAX_PROFILE_BYTES,
    "desktop profile",
  );
  let parsed: unknown;
  try {
    parsed = JSON.parse(profileSource);
  } catch {
    throw new Error("desktop profile must contain valid JSON");
  }
  const profile = validateDesktopProfile(parsed);
  const [operatorCredential, participantCredential] = await Promise.all([
    readCredential(profile.operatorCredentialFile, "operator credential"),
    readCredential(profile.participantCredentialFile, "participant credential"),
  ]);
  if (operatorCredential === participantCredential) {
    throw new Error("operator and participant credentials must be distinct");
  }
  return { ...profile, operatorCredential, participantCredential };
}

export function validateDesktopProfile(value: unknown): DesktopProfile {
  const object = record(value, "desktop profile");
  exactKeys(
    object,
    [
      "schema_version",
      "origin",
      "participant_id",
      "operator_credential_file",
      "participant_credential_file",
      "request_kind",
      "result_kind",
    ],
    ["channel_id"],
    "desktop profile",
  );
  if (object["schema_version"] !== 1) {
    throw new Error("desktop profile must use schema_version 1");
  }
  const origin = loopbackOrigin(object["origin"]);
  const participantId = boundedString(
    object["participant_id"],
    "participant_id",
    256,
  );
  const requestKind = boundedString(object["request_kind"], "request_kind", 256);
  const resultKind = boundedString(object["result_kind"], "result_kind", 256);
  const channelId = optionalBoundedString(
    object["channel_id"],
    "channel_id",
    256,
  );
  const operatorCredentialFile = absolutePath(
    object["operator_credential_file"],
    "operator_credential_file",
  );
  const participantCredentialFile = absolutePath(
    object["participant_credential_file"],
    "participant_credential_file",
  );
  if (operatorCredentialFile === participantCredentialFile) {
    throw new Error("credential files must be distinct");
  }
  return {
    schemaVersion: 1,
    origin,
    participantId,
    requestKind,
    resultKind,
    ...(channelId === undefined ? {} : { channelId }),
    operatorCredentialFile,
    participantCredentialFile,
  };
}

async function readCredential(path: string, label: string): Promise<string> {
  await requirePrivateRegularFile(path, label);
  let value = await readBoundedFile(path, MAX_CREDENTIAL_BYTES, label);
  if (value.endsWith("\r\n")) value = value.slice(0, -2);
  else if (value.endsWith("\n")) value = value.slice(0, -1);
  if (!value || /\s/u.test(value)) {
    throw new Error(`${label} must contain one non-whitespace bearer value`);
  }
  return value;
}

async function requirePrivateRegularFile(
  path: string,
  label: string,
): Promise<void> {
  if (!isAbsolute(path)) throw new Error(`${label} path must be absolute`);
  const metadata = await lstat(path);
  if (!metadata.isFile() || metadata.isSymbolicLink()) {
    throw new Error(`${label} must be a regular file, not a link`);
  }
  if ((metadata.mode & 0o077) !== 0) {
    throw new Error(`${label} must not grant group or other permissions`);
  }
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error(`${label} must be owned by the current user`);
  }
}

async function readBoundedFile(
  path: string,
  maximumBytes: number,
  label: string,
): Promise<string> {
  const bytes = await readFile(path);
  if (bytes.byteLength > maximumBytes) {
    throw new Error(`${label} exceeds ${maximumBytes} bytes`);
  }
  return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
}

function loopbackOrigin(value: unknown): string {
  const input = boundedString(value, "origin", 2_048);
  let url: URL;
  try {
    url = new URL(input);
  } catch {
    throw new Error("origin must be an absolute loopback HTTP origin");
  }
  if (
    url.protocol !== "http:" ||
    !LOOPBACK_HOSTS.has(url.hostname) ||
    url.username ||
    url.password ||
    !url.port ||
    url.pathname !== "/" ||
    url.search ||
    url.hash ||
    url.origin !== input
  ) {
    throw new Error("origin must be an exact loopback HTTP origin with a port");
  }
  return url.origin;
}

function absolutePath(value: unknown, label: string): string {
  const path = boundedString(value, label, 4_096);
  if (!isAbsolute(path)) throw new Error(`${label} must be absolute`);
  return resolve(path);
}

function optionalBoundedString(
  value: unknown,
  label: string,
  maximumBytes: number,
): string | undefined {
  return value === undefined ? undefined : boundedString(value, label, maximumBytes);
}

function boundedString(
  value: unknown,
  label: string,
  maximumBytes: number,
): string {
  if (
    typeof value !== "string" ||
    !value.trim() ||
    new TextEncoder().encode(value).byteLength > maximumBytes
  ) {
    throw new Error(`${label} must be a non-empty string of at most ${maximumBytes} bytes`);
  }
  return value;
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} must be a JSON object`);
  }
  return value as Record<string, unknown>;
}

function exactKeys(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[],
  label: string,
): void {
  const allowed = new Set([...required, ...optional]);
  const unknown = Object.keys(value).filter((key) => !allowed.has(key));
  const missing = required.filter((key) => !(key in value));
  if (unknown.length || missing.length) {
    throw new Error(`${label} has missing or unknown fields`);
  }
}
