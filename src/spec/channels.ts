// Topics as an API Spec describes them: a channel's address, with `{name}` parameters standing
// for one level each.
import type { SpecChannel } from "../bridge";

export interface ChannelMatch {
  channel: SpecChannel;
  /** The parameters' values in the topic. */
  params: Record<string, string>;
}

const PARAM = /^\{([^{}]+)\}$/;

/** The parameter a level stands for, or null for a literal level. */
export function paramName(level: string): string | null {
  return PARAM.exec(level)?.[1] ?? null;
}

/** The channel a topic is on, preferring the one with the most literal levels. */
export function matchChannel(
  channels: SpecChannel[],
  topic: string,
): ChannelMatch | null {
  const levels = topic.split("/");
  let best: { match: ChannelMatch; literal: number } | null = null;
  for (const channel of channels) {
    const pattern = channel.address.split("/");
    if (pattern.length !== levels.length) continue;
    const params: Record<string, string> = {};
    let literal = 0;
    const fits = pattern.every((level, i) => {
      const name = paramName(level);
      if (name === null) {
        literal++;
        return level === levels[i];
      }
      params[name] = levels[i];
      return levels[i] !== "" && !/[+#]/.test(levels[i]);
    });
    if (fits && (!best || literal > best.literal)) {
      best = { match: { channel, params }, literal };
    }
  }
  return best?.match ?? null;
}

/**
 * Values for each of a channel's parameters: the ones the spec names, then the ones in topics
 * already seen on it.
 */
export function knownValues(
  channel: SpecChannel,
  seenTopics: string[],
): Map<string, string[]> {
  const values = new Map<string, string[]>();
  for (const p of channel.parameters) values.set(p.name, [...p.values]);
  for (const topic of seenTopics) {
    const match = matchChannel([channel], topic);
    if (!match) continue;
    for (const [name, value] of Object.entries(match.params)) {
      const list = values.get(name) ?? [];
      if (!list.includes(value) && list.length < 50) list.push(value);
      values.set(name, list);
    }
  }
  return values;
}

/**
 * The channel's address as a CodeMirror snippet: each parameter a field to tab through,
 * filled in with its first known value.
 */
export function topicSnippet(
  channel: SpecChannel,
  values: Map<string, string[]>,
): string {
  const escape = (text: string) => text.replace(/[\\$}]/g, "\\$&");
  let field = 0;
  return channel.address
    .split("/")
    .map((level) => {
      const name = paramName(level);
      if (name === null) return escape(level);
      const first = values.get(name)?.[0] ?? name;
      return `\${${++field}:${escape(first)}}`;
    })
    .join("/");
}

/**
 * When the cursor is in a level that stands for a parameter of a channel the topic so far
 * fits: that channel's parameter, and where the level starts.
 */
export function paramAt(
  channels: SpecChannel[],
  before: string,
): { channel: SpecChannel; name: string; from: number } | null {
  const levels = before.split("/");
  const index = levels.length - 1;
  const from = before.length - levels[index].length;
  for (const channel of channels) {
    const pattern = channel.address.split("/");
    if (pattern.length <= index) continue;
    const name = paramName(pattern[index]);
    if (name === null) continue;
    const fits = levels.slice(0, index).every((level, i) => {
      const literal = paramName(pattern[i]) === null;
      return literal ? level === pattern[i] : level !== "";
    });
    if (fits) return { channel, name, from };
  }
  return null;
}
