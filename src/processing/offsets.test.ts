import { describe, expect, it } from "vitest";
import { byteToIndex } from "./offsets";

describe("byteToIndex", () => {
  it("is the identity for ASCII", () => {
    expect(byteToIndex("from all", 5)).toBe(5);
  });

  it("counts multi-byte characters once", () => {
    // "å" is 2 bytes, "°" 2 bytes, "🌡" 4 bytes (2 UTF-16 units).
    const text = "måqtt °C 🌡 x";
    expect(byteToIndex(text, 3)).toBe(2); // after "må"
    expect(byteToIndex(text, 10)).toBe(8); // after "måqtt °C "... up to the space
    expect(byteToIndex(text, text.length + 10)).toBe(text.length);
  });

  it("puts an offset past a 4-byte character after both UTF-16 units", () => {
    expect(byteToIndex("🌡x", 4)).toBe(2);
  });
});
