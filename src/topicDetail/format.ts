import type { PayloadData } from "../bridge";

export type PayloadFormat = "json" | "text" | "hex";

/** The bytes of a payload, whichever way the backend sent it. */
export function payloadBytes(payload: PayloadData): Uint8Array {
  if (payload.kind === "text") return new TextEncoder().encode(payload.text);
  const binary = atob(payload.base64);
  return Uint8Array.from(binary, (c) => c.charCodeAt(0));
}

/** The payload parsed as JSON, or undefined if it isn't JSON. */
export function parseJson(payload: PayloadData): unknown {
  if (payload.kind !== "text") return undefined;
  const text = payload.text.trim();
  // Only objects and arrays: "42" or "true" read better as plain text.
  if (!text.startsWith("{") && !text.startsWith("[")) return undefined;
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}

/** The best way to show a payload: JSON if it parses, text if it is text, hex otherwise. */
export function detectFormat(payload: PayloadData): PayloadFormat {
  if (payload.kind === "binary") return "hex";
  return parseJson(payload) === undefined ? "text" : "json";
}

/** A classic hex dump: offset, 16 bytes in hex, and their printable ASCII. */
export function hexDump(bytes: Uint8Array): string {
  const lines: string[] = [];
  for (let offset = 0; offset < bytes.length; offset += 16) {
    const row = bytes.subarray(offset, offset + 16);
    const hex = Array.from(row, (b) => b.toString(16).padStart(2, "0"));
    const halves = `${hex.slice(0, 8).join(" ")}  ${hex.slice(8).join(" ")}`;
    const ascii = Array.from(row, (b) =>
      b >= 0x20 && b < 0x7f ? String.fromCharCode(b) : ".",
    ).join("");
    lines.push(
      `${offset.toString(16).padStart(8, "0")}  ${halves.padEnd(48)}  ${ascii}`,
    );
  }
  return lines.join("\n");
}

export function formatPayload(
  payload: PayloadData,
  format: PayloadFormat,
): string {
  switch (format) {
    case "json": {
      const value = parseJson(payload);
      return value === undefined
        ? "Not valid JSON"
        : JSON.stringify(value, null, 2);
    }
    case "text":
      return payload.kind === "text"
        ? payload.text
        : new TextDecoder().decode(payloadBytes(payload));
    case "hex":
      return hexDump(payloadBytes(payload));
  }
}

/** A payload as plain text for the clipboard: the text itself, or its bytes as hex. */
export function payloadAsText(payload: PayloadData): string {
  if (payload.kind === "text") return payload.text;
  return Array.from(payloadBytes(payload), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join(" ");
}
