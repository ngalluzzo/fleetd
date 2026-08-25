import { afterEach, describe, expect, test } from "bun:test";
import { chmod, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  loadDesktopProfile,
  parseProfilePath,
  validateDesktopProfile,
} from "../src/desktop-profile.ts";

const directories: string[] = [];

afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

describe("desktop profile", () => {
  test("normalizes the exact supported profile", () => {
    expect(
      validateDesktopProfile({
        schema_version: 1,
        origin: "http://127.0.0.1:4317",
        participant_id: "human-id",
        operator_credential_file: "/private/operator.token",
        participant_credential_file: "/private/human.token",
        request_kind: "conversation.prompt/v1",
        result_kind: "conversation.result/v1",
        channel_id: "channel-id",
      }),
    ).toEqual({
      schemaVersion: 1,
      origin: "http://127.0.0.1:4317",
      participantId: "human-id",
      operatorCredentialFile: "/private/operator.token",
      participantCredentialFile: "/private/human.token",
      requestKind: "conversation.prompt/v1",
      resultKind: "conversation.result/v1",
      channelId: "channel-id",
    });
  });

  test("rejects expanded authority and unknown fields", () => {
    const base = {
      schema_version: 1,
      origin: "http://127.0.0.1:4317",
      participant_id: "human-id",
      operator_credential_file: "/private/operator.token",
      participant_credential_file: "/private/human.token",
      request_kind: "conversation.prompt/v1",
      result_kind: "conversation.result/v1",
    };
    expect(() =>
      validateDesktopProfile({ ...base, origin: "https://fleet.example" }),
    ).toThrow("exact loopback HTTP origin");
    expect(() => validateDesktopProfile({ ...base, token: "secret" })).toThrow(
      "missing or unknown fields",
    );
    expect(() =>
      validateDesktopProfile({
        ...base,
        participant_credential_file: base.operator_credential_file,
      }),
    ).toThrow("credential files must be distinct");
  });

  test("loads only private regular credential files", async () => {
    const directory = await privateDirectory();
    const operatorPath = join(directory, "operator.token");
    const participantPath = join(directory, "participant.token");
    const profilePath = join(directory, "profile.json");
    await writeFile(operatorPath, "operator-secret\n", { mode: 0o600 });
    await writeFile(participantPath, "participant-secret", { mode: 0o600 });
    await writeFile(
      profilePath,
      JSON.stringify({
        schema_version: 1,
        origin: "http://127.0.0.1:4317",
        participant_id: "human-id",
        operator_credential_file: operatorPath,
        participant_credential_file: participantPath,
        request_kind: "conversation.prompt/v1",
        result_kind: "conversation.result/v1",
      }),
      { mode: 0o600 },
    );

    const profile = await loadDesktopProfile(profilePath);
    expect(profile.operatorCredential).toBe("operator-secret");
    expect(profile.participantCredential).toBe("participant-secret");

    await chmod(participantPath, 0o644);
    await expect(loadDesktopProfile(profilePath)).rejects.toThrow(
      "must not grant group or other permissions",
    );
  });

  test("rejects linked credential files and whitespace-bearing values", async () => {
    const directory = await privateDirectory();
    const operatorPath = join(directory, "operator.token");
    const participantTarget = join(directory, "participant-target.token");
    const participantPath = join(directory, "participant.token");
    const profilePath = join(directory, "profile.json");
    await writeFile(operatorPath, "operator-secret", { mode: 0o600 });
    await writeFile(participantTarget, "participant-secret", { mode: 0o600 });
    await symlink(participantTarget, participantPath);
    await writeProfile(profilePath, operatorPath, participantPath);
    await expect(loadDesktopProfile(profilePath)).rejects.toThrow(
      "regular file, not a link",
    );

    await rm(participantPath);
    await writeFile(participantPath, "participant secret", { mode: 0o600 });
    await expect(loadDesktopProfile(profilePath)).rejects.toThrow(
      "one non-whitespace bearer value",
    );
  });
});

describe("profile argument", () => {
  test("uses one absolute override or the absolute default", () => {
    expect(parseProfilePath([], "/default/profile.json")).toBe(
      "/default/profile.json",
    );
    expect(
      parseProfilePath(["--profile", "/chosen/profile.json"], "/default.json"),
    ).toBe("/chosen/profile.json");
    expect(() =>
      parseProfilePath(["--profile=relative.json"], "/default.json"),
    ).toThrow("must be absolute");
    expect(() =>
      parseProfilePath(
        ["--profile=/one.json", "--profile=/two.json"],
        "/default.json",
      ),
    ).toThrow("only once");
  });
});

async function privateDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "fleetd-desktop-profile-"));
  directories.push(directory);
  await chmod(directory, 0o700);
  return directory;
}

async function writeProfile(
  profilePath: string,
  operatorPath: string,
  participantPath: string,
): Promise<void> {
  await writeFile(
    profilePath,
    JSON.stringify({
      schema_version: 1,
      origin: "http://127.0.0.1:4317",
      participant_id: "human-id",
      operator_credential_file: operatorPath,
      participant_credential_file: participantPath,
      request_kind: "conversation.prompt/v1",
      result_kind: "conversation.result/v1",
    }),
    { mode: 0o600 },
  );
}
