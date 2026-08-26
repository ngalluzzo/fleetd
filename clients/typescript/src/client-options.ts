/**
 * Shared validation for the options both Fleetd clients accept.
 *
 * The operator client and the conversation transport take the same kinds of
 * input -- an origin, a credential, a request budget -- and each rejected them
 * with its own copy of these checks. `exactHttpOrigin` in particular is a
 * security boundary: hardening it in one client while the other kept an older
 * copy is exactly the drift this module exists to prevent.
 *
 * This module is internal to the package. It is deliberately absent from
 * `package.json` exports and from `index.ts`, so tightening a rule here is
 * never a breaking change for a consumer.
 */

/** Default budget for one HTTP operation when the caller does not set one. */
export const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;

export function boundedRequestTimeout(value: number | undefined): number {
  const timeout = value ?? DEFAULT_REQUEST_TIMEOUT_MS;
  if (!Number.isSafeInteger(timeout) || timeout < 100 || timeout > 60_000) {
    throw new Error("requestTimeoutMs must be between 100 and 60000");
  }
  return timeout;
}

/**
 * Accepts only a bare HTTP(S) authority.
 *
 * Embedded credentials, a path, a query, or a fragment are all rejected rather
 * than silently dropped, so a caller cannot believe it addressed one origin
 * while requests are built against another.
 */
export function exactHttpOrigin(value: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch (cause) {
    throw new Error("origin must be an absolute HTTP(S) origin", { cause });
  }
  if (
    !["http:", "https:"].includes(parsed.protocol) ||
    parsed.username ||
    parsed.password ||
    (parsed.pathname !== "/" && parsed.pathname !== "") ||
    parsed.search ||
    parsed.hash
  ) {
    throw new Error("origin must contain only an HTTP(S) authority");
  }
  return parsed.origin;
}

export function boundedIdentifier(value: string, name: string): string {
  if (
    typeof value !== "string" ||
    value.trim().length === 0 ||
    value.length > 256
  ) {
    throw new Error(`${name} must contain between 1 and 256 characters`);
  }
  return value;
}

export function boundedCredential(value: string, name: string): string {
  if (typeof value !== "string" || value.length === 0 || value.length > 4_096) {
    throw new Error(`${name} must contain between 1 and 4096 characters`);
  }
  return value;
}
