import { describe, expect, it } from "vitest";
import type { Profile } from "../bridge";
import { cleanDraft, passwordChange } from "./draft";

const profile: Profile = {
  id: "p1",
  name: " Home ",
  host: " broker.local ",
  port: 8883,
  protocol: "mqtt5",
  clientId: " maqtt-1 ",
  username: "  ",
  tls: {
    caFile: " /certs/ca.crt ",
    clientCertFile: "",
    clientKeyFile: null,
    insecureSkipVerify: false,
  },
  cleanStart: true,
  keepAliveSecs: 30,
  subscriptions: [
    { filter: " home/# ", qos: 1 },
    { filter: "  ", qos: 0 },
  ],
  historyPerTopic: 100,
};

describe("passwordChange", () => {
  it("keeps the stored password when nothing was typed", () => {
    expect(passwordChange("", false)).toEqual({ kind: "keep" });
  });

  it("sets what was typed", () => {
    expect(passwordChange("hunter2", false)).toEqual({
      kind: "set",
      value: "hunter2",
    });
  });

  it("clears when asked, whatever was typed", () => {
    expect(passwordChange("hunter2", true)).toEqual({ kind: "clear" });
  });
});

describe("cleanDraft", () => {
  it("trims text and turns blank optional fields into null", () => {
    const cleaned = cleanDraft(profile);
    expect(cleaned.name).toBe("Home");
    expect(cleaned.host).toBe("broker.local");
    expect(cleaned.clientId).toBe("maqtt-1");
    expect(cleaned.username).toBeNull();
    expect(cleaned.tls).toEqual({
      caFile: "/certs/ca.crt",
      clientCertFile: null,
      clientKeyFile: null,
      insecureSkipVerify: false,
    });
  });

  it("drops blank Subscriptions and trims the rest", () => {
    expect(cleanDraft(profile).subscriptions).toEqual([
      { filter: "home/#", qos: 1 },
    ]);
  });

  it("leaves TLS off when it is off", () => {
    expect(cleanDraft({ ...profile, tls: null }).tls).toBeNull();
  });
});
