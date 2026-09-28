/**
 * The backend reports positions as UTF-8 byte offsets; the editor counts UTF-16 code units.
 * This maps one to the other.
 */
export function byteToIndex(text: string, byteOffset: number): number {
  let bytes = 0;
  let index = 0;
  for (const char of text) {
    if (bytes >= byteOffset) break;
    const code = char.codePointAt(0)!;
    bytes += code < 0x80 ? 1 : code < 0x800 ? 2 : code < 0x10000 ? 3 : 4;
    index += char.length;
  }
  return index;
}
