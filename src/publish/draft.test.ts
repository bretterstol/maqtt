import { describe, expect, it } from "vitest";
import {
  draftError,
  emptyDraft,
  formatJson,
  fromMessage,
  toMessage,
} from "./draft";

const draft = (change: Partial<ReturnType<typeof emptyDraft>> = {}) => ({
  ...emptyDraft("p1", "home/lamp/set"),
  ...change,
});

describe("draftError", () => {
  it("accepts a plain message", () => {
    expect(draftError(draft({ payload: "on" }), true)).toBeNull();
  });

  it("needs a Connection and a topic without wildcards", () => {
    expect(draftError(draft({ profileId: null }), true)).toMatch(/Connection/);
    expect(draftError(draft({ topic: "" }), true)).toMatch(/topic/);
    expect(draftError(draft({ topic: "home/+/set" }), true)).toMatch(/\+ or #/);
  });

  it("refuses properties on MQTT 3.1.1, but not an empty advanced section", () => {
    expect(draftError(draft({ advanced: true }), false)).toBeNull();
    expect(
      draftError(draft({ advanced: true, contentType: "text/plain" }), false),
    ).toMatch(/3\.1\.1/);
  });

  it("wants expiry in whole seconds", () => {
    expect(
      draftError(draft({ advanced: true, expirySecs: "1.5" }), true),
    ).toMatch(/whole number/);
  });
});

describe("toMessage", () => {
  it("leaves properties out unless the advanced section has some", () => {
    expect(toMessage(draft({ advanced: true })).properties).toBeNull();
    expect(
      toMessage(draft({ contentType: "text/plain" })).properties,
    ).toBeNull();
  });

  it("maps the advanced fields, dropping blank user properties", () => {
    const message = toMessage(
      draft({
        advanced: true,
        contentType: " application/json ",
        expirySecs: "60",
        payloadIsUtf8: true,
        userProperties: [
          ["unit", "celsius"],
          [" ", "ignored"],
        ],
      }),
    );
    expect(message.properties).toEqual({
      contentType: "application/json",
      responseTopic: null,
      correlationData: null,
      messageExpiryInterval: 60,
      payloadIsUtf8: true,
      userProperties: [["unit", "celsius"]],
    });
  });

  it("round-trips through the Publish History", () => {
    const original = draft({
      payload: "{}",
      qos: 1,
      retain: true,
      advanced: true,
      correlationData: "req-1",
    });
    expect(fromMessage("p1", toMessage(original))).toEqual(original);
  });
});

describe("formatJson", () => {
  it("pretty-prints JSON and rejects the rest", () => {
    expect(formatJson('{"a":1}')).toBe('{\n  "a": 1\n}');
    expect(formatJson("on")).toBeNull();
  });
});
