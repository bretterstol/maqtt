import { describe, expect, it } from "vitest";
import {
  detectFormat,
  formatPayload,
  hexDump,
  payloadAsText,
  payloadBytes,
} from "./format";

const text = (t: string) => ({ kind: "text" as const, text: t });
const binary = (bytes: number[]) => ({
  kind: "binary" as const,
  base64: btoa(String.fromCharCode(...bytes)),
});

describe("detectFormat", () => {
  it("picks JSON for objects and arrays", () => {
    expect(detectFormat(text('{"a":1}'))).toBe("json");
    expect(detectFormat(text(" [1, 2] "))).toBe("json");
  });

  it("keeps scalars and broken JSON as text", () => {
    expect(detectFormat(text("42"))).toBe("text");
    expect(detectFormat(text('{"a":'))).toBe("text");
    expect(detectFormat(text("online"))).toBe("text");
  });

  it("uses hex for binary", () => {
    expect(detectFormat(binary([0xff, 0]))).toBe("hex");
  });
});

describe("formatPayload", () => {
  it("pretty-prints JSON", () => {
    expect(formatPayload(text('{"a":[1]}'), "json")).toBe(
      '{\n  "a": [\n    1\n  ]\n}',
    );
  });

  it("shows text as it is, and binary decoded when asked for text", () => {
    expect(formatPayload(text("a\nb"), "text")).toBe("a\nb");
    expect(formatPayload(binary([0x68, 0x69]), "text")).toBe("hi");
  });
});

describe("hexDump", () => {
  it("shows offset, grouped hex and printable ASCII", () => {
    const bytes = new Uint8Array([...Array(17).keys()].map((i) => 0x41 + i));
    bytes[1] = 0x00;
    expect(hexDump(bytes).split("\n")).toEqual([
      "00000000  41 00 43 44 45 46 47 48  49 4a 4b 4c 4d 4e 4f 50  A.CDEFGHIJKLMNOP",
      "00000010  51                                                Q",
    ]);
  });

  it("dumps text payloads by their UTF-8 bytes", () => {
    expect(payloadBytes(text("å"))).toEqual(new Uint8Array([0xc3, 0xa5]));
  });
});

describe("payloadAsText", () => {
  it("copies text as is and binary as hex bytes", () => {
    expect(payloadAsText(text("on"))).toBe("on");
    expect(payloadAsText(binary([0xff, 0x00, 0x10]))).toBe("ff 00 10");
  });
});
