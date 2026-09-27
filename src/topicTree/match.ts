/** Whether a search query should be read as an MQTT topic filter rather than a substring. */
export function isTopicFilter(query: string): boolean {
  return query.includes("+") || query.includes("#");
}

/** Why `filter` is not a valid MQTT topic filter, or null if it is. */
export function topicFilterError(filter: string): string | null {
  if (filter === "") return "A topic filter can't be empty";
  const levels = filter.split("/");
  for (const [i, level] of levels.entries()) {
    if (level.includes("#") && (level !== "#" || i !== levels.length - 1)) {
      return "`#` must be the whole last level";
    }
    if (level.includes("+") && level !== "+") {
      return "`+` must be a whole level";
    }
  }
  return null;
}

/**
 * MQTT topic filter matching: `+` matches exactly one level, `#` the rest (including none), and
 * topics starting with `$` are not matched by a filter starting with a wildcard.
 */
export function topicMatches(filter: string, topic: string): boolean {
  const filterLevels = filter.split("/");
  const topicLevels = topic.split("/");
  if (
    topic.startsWith("$") &&
    (filterLevels[0] === "+" || filterLevels[0] === "#")
  ) {
    return false;
  }
  for (const [i, level] of filterLevels.entries()) {
    if (level === "#") return true;
    if (i >= topicLevels.length) return false;
    if (level !== "+" && level !== topicLevels[i]) return false;
  }
  return filterLevels.length === topicLevels.length;
}
