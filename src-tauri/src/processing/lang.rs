//! The pipeline language: a `from` line, then Stages joined with `|>`, with jq expressions
//! inside. Parsing turns source text into Stages, and every problem into a [`Diagnostic`] with
//! a line and column, so the editor can mark it.

use std::time::Duration;

use serde::Serialize;

use super::jq;
use crate::profile::validate_topic_filter;

/// A problem in a Pipeline's source, for the editor.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Diagnostic {
    pub message: String,
    /// 1-based.
    pub line: usize,
    /// 1-based, in characters.
    pub column: usize,
    /// Byte range in the source.
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Connections {
    All,
    Named(Vec<String>),
}

/// A Stage as written, with its jq expressions already rewritten for jq (see [`to_jq`]).
#[derive(Debug, Clone, PartialEq)]
pub enum Stage {
    FilterTopic {
        filter: String,
        negate: bool,
    },
    Filter(String),
    Map(String),
    /// With an optional key (a jq expression); without one, state is per incoming topic.
    Changes(Option<String>),
    Throttle(Duration, Option<String>),
    Debounce(Duration, Option<String>),
    /// A start value and an update, both jq; the update is run wrapped in [`jq::SCAN_WRAP`].
    Scan {
        start: String,
        update: String,
        key: Option<String>,
    },
    Window(WindowSize, Option<String>),
    Take(usize, Option<String>),
    Inspect(Option<String>),
    /// The Derived Topic, as a jq expression that yields a string.
    ToApp(String),
    /// Republishing on a Connection, named as in `from`; the topic is a jq expression that
    /// yields a string.
    ToBroker {
        broker: String,
        topic: String,
    },
}

/// When a window closes: on the clock every interval, or every N values.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WindowSize {
    Time(Duration),
    Count(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pipeline {
    pub from: Connections,
    pub stages: Vec<Stage>,
    /// Where the `from` line and then each Stage end in the source (byte offsets), for Preview
    /// to show what each Stage did.
    pub ends: Vec<usize>,
}

const STAGES: &str = "filter, map, changes, throttle, debounce, scan, window, take, inspect, to";

/// Parses and checks a Pipeline, reporting every problem found.
pub fn parse(source: &str) -> Result<Pipeline, Vec<Diagnostic>> {
    let mut problems = Problems::new(source);
    let code = strip_comments(source);
    let mut segments = split_stages(&code).into_iter();

    let mut from = None;
    let mut ends = Vec::new();
    let end_of = |span: Span| span.start + code[span.start..span.end].trim_end().len();
    if let Some(first) = segments.next() {
        ends.push(end_of(first));
        let (keyword, arg) = keyword(&code, first);
        if keyword.text == "from" {
            from = parse_from(&code, arg, &mut problems);
        } else {
            problems.at(
                first.start,
                first.end.max(first.start + 1),
                "A Pipeline starts with `from`, for example `from \"Dev Broker\"` or `from all`",
            );
        }
    }

    let mut stages = Vec::new();
    for segment in segments {
        let (keyword, arg) = keyword(&code, segment);
        if let Some(stage) = parse_stage(&code, keyword, arg, &mut problems) {
            stages.push(stage);
            ends.push(end_of(segment));
        }
    }

    match (from, problems.list.is_empty()) {
        (Some(from), true) => Ok(Pipeline { from, stages, ends }),
        _ => {
            if problems.list.is_empty() {
                problems.at(0, 1, "A Pipeline needs a `from` line");
            }
            Err(problems.list)
        }
    }
}

/// A byte range in the (comment-stripped) source.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Span {
    start: usize,
    end: usize,
}

struct Word<'a> {
    text: &'a str,
    span: Span,
}

struct Problems<'a> {
    source: &'a str,
    list: Vec<Diagnostic>,
}

impl<'a> Problems<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            source,
            list: Vec::new(),
        }
    }

    fn at(&mut self, start: usize, end: usize, message: impl Into<String>) {
        let start = start.min(self.source.len());
        let before = &self.source[..start];
        let line = before.matches('\n').count() + 1;
        let column = before.rsplit('\n').next().unwrap_or("").chars().count() + 1;
        self.list.push(Diagnostic {
            message: message.into(),
            line,
            column,
            start,
            end: end.clamp(start, self.source.len()),
        });
    }
}

fn keyword<'a>(code: &'a str, segment: Span) -> (Word<'a>, Span) {
    let text = &code[segment.start..segment.end];
    let leading = text.len() - text.trim_start().len();
    let start = segment.start + leading;
    let word_len = code[start..segment.end]
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(segment.end - start);
    let word = Word {
        text: &code[start..start + word_len],
        span: Span {
            start,
            end: start + word_len,
        },
    };
    (
        word,
        Span {
            start: start + word_len,
            end: segment.end,
        },
    )
}

fn parse_from(code: &str, arg: Span, problems: &mut Problems) -> Option<Connections> {
    let text = code[arg.start..arg.end].trim();
    if text == "all" {
        return Some(Connections::All);
    }
    let mut names = Vec::new();
    let mut rest = arg;
    loop {
        match string_literal(code, rest) {
            Some((name, after)) => {
                names.push(name);
                let tail = code[after.start..after.end].trim_start();
                if let Some(next) = tail.strip_prefix(',') {
                    rest = Span {
                        start: after.end - next.len(),
                        end: after.end,
                    };
                } else if tail.is_empty() {
                    return Some(Connections::Named(names));
                } else {
                    problems.at(
                        after.end - tail.len(),
                        after.end,
                        "Separate Connection names with commas",
                    );
                    return None;
                }
            }
            None => {
                problems.at(
                    rest.start,
                    rest.end.max(rest.start + 1),
                    "`from` takes `all` or Connection names in quotes, for example `from \"Dev Broker\"`",
                );
                return None;
            }
        }
    }
}

fn parse_stage(code: &str, word: Word, arg: Span, problems: &mut Problems) -> Option<Stage> {
    let arg_text = code[arg.start..arg.end].trim();
    let arg_start = arg.end - code[arg.start..arg.end].trim_start().len();
    match word.text {
        "filter" => {
            let (negate, rest) = match arg_text.strip_prefix("not") {
                Some(rest) if rest.starts_with(char::is_whitespace) => (true, rest.trim_start()),
                _ => (false, arg_text),
            };
            if let Some(after) = rest.strip_prefix("topic")
                && (after.is_empty() || after.starts_with(char::is_whitespace))
            {
                let topic_span = Span {
                    start: offset_in(code, after),
                    end: arg.end,
                };
                let Some((filter, trailing)) = string_literal(code, topic_span) else {
                    problems.at(
                        topic_span.start,
                        arg.end,
                        "`filter topic` takes a topic filter in quotes, like `\"home/#\"`",
                    );
                    return None;
                };
                if !code[trailing.start..trailing.end].trim().is_empty() {
                    problems.at(
                        trailing.start,
                        trailing.end,
                        "Unexpected text after the topic filter",
                    );
                    return None;
                }
                if let Err(err) = validate_topic_filter(&filter) {
                    problems.at(topic_span.start, arg.end, err);
                    return None;
                }
                return Some(Stage::FilterTopic { filter, negate });
            }
            if negate {
                problems.at(
                    arg_start,
                    arg.end,
                    "`filter not` is only for topics (`filter not topic \"…\"`); negate a condition with `filter (… | not)`",
                );
                return None;
            }
            expression(code, arg, problems, "filter").map(Stage::Filter)
        }
        "map" => expression(code, arg, problems, "map").map(Stage::Map),
        "changes" => {
            let rest = Span {
                start: arg_start,
                end: arg.end,
            };
            key_clause(code, rest, problems, "changes by .room").map(Stage::Changes)
        }
        "throttle" => {
            let (interval, rest) = leading_duration(code, arg, problems, "throttle")?;
            key_clause(code, rest, problems, "throttle 5s by .room")
                .map(|key| Stage::Throttle(interval, key))
        }
        "debounce" => {
            let (interval, rest) = leading_duration(code, arg, problems, "debounce")?;
            key_clause(code, rest, problems, "debounce 2s by .room")
                .map(|key| Stage::Debounce(interval, key))
        }
        "window" => {
            let size_len = arg_text.find(char::is_whitespace).unwrap_or(arg_text.len());
            let size_text = &arg_text[..size_len];
            let size = if size_text.bytes().all(|b| b.is_ascii_digit()) {
                size_text
                    .parse()
                    .ok()
                    .filter(|&n: &usize| n > 0)
                    .map(WindowSize::Count)
            } else {
                parse_duration(size_text).map(WindowSize::Time)
            };
            let Some(size) = size else {
                problems.at(
                    arg_start,
                    (arg_start + size_len).max(arg_start + 1),
                    "`window` takes a duration such as `1m`, or a number of values such as `10`",
                );
                return None;
            };
            let rest = Span {
                start: arg_start + size_len,
                end: arg.end,
            };
            key_clause(code, rest, problems, "window 1m by .room")
                .map(|key| Stage::Window(size, key))
        }
        "take" => {
            let count_len = arg_text.find(char::is_whitespace).unwrap_or(arg_text.len());
            let Some(count) = arg_text[..count_len]
                .parse()
                .ok()
                .filter(|&n: &usize| n > 0)
            else {
                problems.at(
                    arg_start,
                    (arg_start + count_len).max(arg_start + 1),
                    "`take` takes a number of values, such as `take 10`",
                );
                return None;
            };
            let rest = Span {
                start: arg_start + count_len,
                end: arg.end,
            };
            key_clause(code, rest, problems, "take 1 by .room").map(|key| Stage::Take(count, key))
        }
        "scan" => parse_scan(code, arg, problems),
        "inspect" => {
            if arg_text.is_empty() {
                return Some(Stage::Inspect(None));
            }
            match string_literal(code, arg) {
                Some((label, trailing)) if code[trailing.start..trailing.end].trim().is_empty() => {
                    Some(Stage::Inspect(Some(label)))
                }
                _ => {
                    problems.at(
                        arg_start,
                        arg.end,
                        "`inspect` takes an optional label in quotes",
                    );
                    None
                }
            }
        }
        "to" => {
            let (sink, sink_arg) = keyword(code, arg);
            match sink.text {
                "app" => expression(code, sink_arg, problems, "to app").map(Stage::ToApp),
                "broker" => {
                    let Some((broker, rest)) = string_literal(code, sink_arg) else {
                        problems.at(
                            sink.span.start,
                            sink_arg.end.max(sink.span.end),
                            "`to broker` takes a Broker's name in quotes and a topic, for example `to broker \"Office\" \"hot/{.room}\"`",
                        );
                        return None;
                    };
                    expression(code, rest, problems, "to broker")
                        .map(|topic| Stage::ToBroker { broker, topic })
                }
                "" => {
                    problems.at(
                        arg_start,
                        arg.end.max(arg_start + 1),
                        "`to` needs a Sink: `to app \"hot/{$levels[2]}\"` or `to broker \"Office\" \"hot/{$levels[2]}\"`",
                    );
                    None
                }
                other => {
                    problems.at(
                        sink.span.start,
                        sink.span.end,
                        format!(
                            "`to {other}` isn't available yet; only `to app` and `to broker` are"
                        ),
                    );
                    None
                }
            }
        }
        "from" => {
            problems.at(
                word.span.start,
                word.span.end,
                "`from` can only be the first line",
            );
            None
        }
        "" => {
            problems.at(
                word.span.start,
                (word.span.start + 2).min(arg.end.max(word.span.start + 1)),
                format!("Expected a Stage after `|>`: {STAGES}"),
            );
            None
        }
        other => {
            problems.at(
                word.span.start,
                word.span.end,
                format!("Unknown Stage `{other}`. Stages are: {STAGES}"),
            );
            None
        }
    }
}

/// The duration a Stage starts with, and the span after it.
fn leading_duration(
    code: &str,
    arg: Span,
    problems: &mut Problems,
    stage: &str,
) -> Option<(Duration, Span)> {
    let text = code[arg.start..arg.end].trim();
    let start = arg.end - code[arg.start..arg.end].trim_start().len();
    let len = text.find(char::is_whitespace).unwrap_or(text.len());
    let Some(interval) = parse_duration(&text[..len]) else {
        problems.at(
            start,
            (start + len).max(start + 1),
            format!("`{stage}` takes a duration such as `500ms`, `5s`, `1m` or `1h`"),
        );
        return None;
    };
    Some((
        interval,
        Span {
            start: start + len,
            end: arg.end,
        },
    ))
}

/// `scan <start> <update> [by <key>]`. The start is one value: everything up to the first
/// space outside brackets and strings.
fn parse_scan(code: &str, arg: Span, problems: &mut Problems) -> Option<Stage> {
    const EXAMPLE: &str = "for example `scan 0 ($acc + .)`";
    let text = &code[arg.start..arg.end];
    let lead = text.len() - text.trim_start().len();
    let body = Span {
        start: arg.start + lead,
        end: arg.start + text.trim_end().len(),
    };
    let body_text = &code[body.start..body.end];
    let mut start_end = body_text.len();
    let mut by_at = None;
    let mut previous = ' ';
    scan_code(body_text, |i, c, top| {
        if top && c.is_whitespace() && start_end == body_text.len() {
            start_end = i;
        }
        if top
            && by_at.is_none()
            && start_end < i
            && previous.is_whitespace()
            && body_text[i..].starts_with("by")
            && body_text[i + 2..].starts_with(|c: char| c.is_whitespace())
        {
            by_at = Some(i);
        }
        previous = c;
    });
    if body_text.is_empty() {
        problems.at(
            arg.start.saturating_sub(1),
            arg.start,
            format!("`scan` needs a start value and an update, {EXAMPLE}"),
        );
        return None;
    }
    let update_end = by_at.map_or(body.end, |at| body.start + at);
    if body_text[start_end..update_end - body.start]
        .trim()
        .is_empty()
    {
        problems.at(
            body.start,
            body.end,
            format!("`scan` needs an update after its start value, {EXAMPLE}"),
        );
        return None;
    }
    let start = expression(
        code,
        Span {
            start: body.start,
            end: body.start + start_end,
        },
        problems,
        "scan",
    );
    let update = expression_in(
        code,
        Span {
            start: body.start + start_end,
            end: update_end,
        },
        problems,
        "scan",
        jq::SCAN_WRAP,
    );
    let key = key_clause(
        code,
        Span {
            start: update_end,
            end: body.end,
        },
        problems,
        "scan 0 ($acc + 1) by .room",
    );
    Some(Stage::Scan {
        start: start?,
        update: update?,
        key: key?,
    })
}

/// Walks `text`, telling for each character whether it's at the top level: outside strings and
/// brackets.
fn scan_code(text: &str, mut visit: impl FnMut(usize, char, bool)) {
    let mut modes = vec![Mode::Code { depth: 0 }];
    let mut iter = text.char_indices().peekable();
    while let Some((i, c)) = iter.next() {
        let top = *modes.last().unwrap();
        visit(i, c, modes.len() == 1 && top == Mode::Code { depth: 0 });
        if top == Mode::Str && c == '\\' {
            iter.next();
            continue;
        }
        let mut chars = iter.clone().map(|(_, c)| c).peekable();
        let before = modes.len();
        step(&mut modes, c, &mut chars);
        // `{{` in a string: skip the second brace too.
        if top == Mode::Str && c == '{' && modes.len() == before {
            iter.next();
        }
    }
}

/// The first jq construct in `text` that can recurse without end, which would overflow the
/// stack and abort the app: `def`, and `recurse`, `repeat`, `while` and `until` with arguments.
/// Plain `recurse` (into the data) is fine. Returns its offset and name.
fn endless_construct(text: &str) -> Option<(usize, &'static str)> {
    const WITH_ARGUMENTS: [&str; 4] = ["recurse", "repeat", "while", "until"];
    let mut found = None;
    let mut modes = vec![Mode::Code { depth: 0 }];
    let mut iter = text.char_indices().peekable();
    let mut previous = ' ';
    while let Some((i, c)) = iter.next() {
        let top = *modes.last().unwrap();
        let word_start = top != Mode::Str
            && found.is_none()
            && c.is_ascii_alphabetic()
            && !(previous.is_alphanumeric() || matches!(previous, '_' | '$' | '.'));
        if word_start {
            let rest = &text[i..];
            let len = rest
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(rest.len());
            let after = rest[len..].trim_start();
            let word = &rest[..len];
            if word == "def" && !after.starts_with(':') {
                found = Some((i, "def"));
            } else if let Some(name) = WITH_ARGUMENTS.iter().find(|&&w| w == word)
                && after.starts_with('(')
            {
                found = Some((i, *name));
            }
        }
        previous = c;
        if top == Mode::Str && c == '\\' {
            iter.next();
            continue;
        }
        let mut chars = iter.clone().map(|(_, c)| c).peekable();
        let before = modes.len();
        step(&mut modes, c, &mut chars);
        if top == Mode::Str && c == '{' && modes.len() == before {
            iter.next();
        }
    }
    found
}

/// Replaces a comma before a closing `}` or `]` with a space, as jq doesn't allow one there;
/// keeps every offset.
fn drop_trailing_commas(text: &str) -> String {
    let mut out = text.to_string();
    let mut commas = Vec::new();
    let mut modes = vec![Mode::Code { depth: 0 }];
    let mut iter = text.char_indices().peekable();
    while let Some((i, c)) = iter.next() {
        let top = *modes.last().unwrap();
        if c == ',' && top != Mode::Str {
            commas.push(i);
        }
        if top == Mode::Str && c == '\\' {
            iter.next();
            continue;
        }
        let mut chars = iter.clone().map(|(_, c)| c).peekable();
        let before = modes.len();
        step(&mut modes, c, &mut chars);
        if top == Mode::Str && c == '{' && modes.len() == before {
            iter.next();
        }
    }
    for i in commas {
        if text[i + 1..].trim_start().starts_with(['}', ']']) {
            out.replace_range(i..i + 1, " ");
        }
    }
    out
}

/// An optional `by <key>` after a Stage: `Some(None)` when absent, `None` when it's wrong.
fn key_clause(
    code: &str,
    rest: Span,
    problems: &mut Problems,
    example: &str,
) -> Option<Option<String>> {
    let text = code[rest.start..rest.end].trim();
    if text.is_empty() {
        return Some(None);
    }
    let start = rest.end - code[rest.start..rest.end].trim_start().len();
    match text.strip_prefix("by") {
        Some(key) if key.is_empty() || key.starts_with(char::is_whitespace) => {
            let key_span = Span {
                start: start + 2,
                end: rest.end,
            };
            expression(code, key_span, problems, "by").map(Some)
        }
        _ => {
            problems.at(
                start,
                rest.end,
                format!("Expected nothing more here, or a key with `by`, for example `{example}`"),
            );
            None
        }
    }
}

/// Where `part`, a slice of `code`, starts in it.
fn offset_in(code: &str, part: &str) -> usize {
    part.as_ptr() as usize - code.as_ptr() as usize
}

/// Checks a jq expression and returns it rewritten for jq; problems point into the source.
fn expression(code: &str, arg: Span, problems: &mut Problems, stage: &str) -> Option<String> {
    expression_in(code, arg, problems, stage, ("", ""))
}

/// Like [`expression`], checking it inside `wrap` (a prefix and a suffix), as it will run.
fn expression_in(
    code: &str,
    arg: Span,
    problems: &mut Problems,
    stage: &str,
    wrap: (&str, &str),
) -> Option<String> {
    let text = &code[arg.start..arg.end];
    let leading = text.len() - text.trim_start().len();
    let text = text.trim();
    let start = arg.start + leading;
    if text.is_empty() {
        problems.at(
            start.saturating_sub(1),
            start,
            format!("`{stage}` needs an expression"),
        );
        return None;
    }
    if let Some((at, word)) = endless_construct(text) {
        let message = if word == "def" {
            "Defining functions with `def` isn't available in Pipelines: one that calls itself without end would take måqtt down with it".to_string()
        } else {
            format!(
                "`{word}(…)` isn't available in Pipelines: it can loop without end, which would take måqtt down with it"
            )
        };
        problems.at(start + at, start + at + word.len(), message);
        return None;
    }
    let (jq_code, original_offsets) = to_jq(&drop_trailing_commas(text));
    let (prefix, suffix) = wrap;
    match jq::compile(&format!("{prefix}{jq_code}{suffix}")) {
        Ok(_) => Some(jq_code),
        Err(err) => {
            let at = start
                + original_offsets
                    .get(err.offset.saturating_sub(prefix.len()))
                    .copied()
                    .unwrap_or(text.len());
            problems.at(at, (at + 1).min(start + text.len()).max(at), err.message);
            None
        }
    }
}

/// A `"…"` string literal at the start of `span`, and the span after it.
fn string_literal(code: &str, span: Span) -> Option<(String, Span)> {
    let text = &code[span.start..span.end];
    let trimmed = text.trim_start();
    let start = span.start + (text.len() - trimmed.len());
    let mut chars = trimmed.char_indices();
    if chars.next()?.1 != '"' {
        return None;
    }
    let mut value = String::new();
    let mut escaped = false;
    for (i, c) in chars {
        match (escaped, c) {
            (true, 'n') => value.push('\n'),
            (true, 't') => value.push('\t'),
            (true, c) => value.push(c),
            (false, '\\') => {
                escaped = true;
                continue;
            }
            (false, '"') => {
                let end = start + i + 1;
                return Some((
                    value,
                    Span {
                        start: end,
                        end: span.end,
                    },
                ));
            }
            (false, c) => value.push(c),
        }
        escaped = false;
    }
    None
}

fn parse_duration(text: &str) -> Option<Duration> {
    let split = text.find(|c: char| !c.is_ascii_digit())?;
    let (number, unit) = text.split_at(split);
    let n: u64 = number.parse().ok()?;
    let duration = match unit {
        "ms" => Duration::from_millis(n),
        "s" => Duration::from_secs(n),
        "m" => Duration::from_secs(n * 60),
        "h" => Duration::from_secs(n * 3600),
        _ => return None,
    };
    (n > 0).then_some(duration)
}

/// Where the scanner is: in code, in a string, or in a `{…}` inside a string.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Code { depth: usize },
    Str,
    Fill { depth: usize },
}

/// Replaces `#` comments (outside strings) with spaces, keeping every offset.
fn strip_comments(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let mut modes = vec![Mode::Code { depth: 0 }];
    let mut chars = source.chars().peekable();
    let mut in_comment = false;
    while let Some(c) = chars.next() {
        if in_comment {
            if c == '\n' {
                in_comment = false;
                out.push('\n');
            } else {
                // Keep byte offsets: one space per byte.
                out.extend(std::iter::repeat_n(' ', c.len_utf8()));
            }
            continue;
        }
        match (*modes.last().unwrap(), c) {
            (Mode::Code { .. } | Mode::Fill { .. }, '#') => {
                in_comment = true;
                out.push(' ');
                continue;
            }
            (Mode::Str, '\\') => {
                out.push(c);
                if let Some(next) = chars.next() {
                    out.push(next);
                }
                continue;
            }
            _ => step(&mut modes, c, &mut chars),
        }
        out.push(c);
    }
    out
}

/// Advances the mode stack past `c` (`{{` in a string is consumed as one literal brace).
fn step(
    modes: &mut Vec<Mode>,
    c: char,
    chars: &mut std::iter::Peekable<impl Iterator<Item = char>>,
) {
    let top = modes.last_mut().unwrap();
    match (*top, c) {
        (Mode::Code { .. } | Mode::Fill { .. }, '"') => modes.push(Mode::Str),
        (Mode::Code { depth }, '(' | '[' | '{') => *top = Mode::Code { depth: depth + 1 },
        (Mode::Code { depth }, ')' | ']' | '}') => {
            *top = Mode::Code {
                depth: depth.saturating_sub(1),
            }
        }
        (Mode::Fill { depth }, '(' | '[' | '{') => *top = Mode::Fill { depth: depth + 1 },
        (Mode::Fill { depth: 0 }, '}') => {
            modes.pop();
        }
        (Mode::Fill { depth }, ')' | ']' | '}') => {
            *top = Mode::Fill {
                depth: depth.saturating_sub(1),
            }
        }
        (Mode::Str, '"') => {
            modes.pop();
        }
        (Mode::Str, '{') => {
            if chars.peek() == Some(&'{') {
                chars.next();
            } else {
                modes.push(Mode::Fill { depth: 0 });
            }
        }
        _ => {}
    }
}

/// Splits comment-free code into Stages at each `|>` outside strings and brackets.
fn split_stages(code: &str) -> Vec<Span> {
    let mut spans = Vec::new();
    let mut modes = vec![Mode::Code { depth: 0 }];
    let mut start = 0;
    let mut iter = code.char_indices().peekable();
    while let Some((i, c)) = iter.next() {
        if modes.len() == 1
            && modes[0] == (Mode::Code { depth: 0 })
            && c == '|'
            && code[i + 1..].starts_with('>')
        {
            spans.push(Span { start, end: i });
            iter.next();
            start = i + 2;
            continue;
        }
        if *modes.last().unwrap() == Mode::Str && c == '\\' {
            iter.next();
            continue;
        }
        let mut chars = iter.clone().map(|(_, c)| c).peekable();
        let before = modes.len();
        let was_str = *modes.last().unwrap() == Mode::Str;
        step(&mut modes, c, &mut chars);
        // `{{` in a string: skip the second brace too.
        if was_str && c == '{' && modes.len() == before {
            iter.next();
        }
    }
    spans.push(Span {
        start,
        end: code.len(),
    });
    spans
}

/// Rewrites måqtt's `{…}` string filling into jq's `\(…)`, and `{{` and `}}` into literal braces
/// (as in Python's f-strings). Also returns, for every byte of the result, the byte offset in
/// `text` it came from, to map jq's errors back.
pub fn to_jq(text: &str) -> (String, Vec<usize>) {
    let mut out = String::with_capacity(text.len() + 8);
    let mut origin = Vec::with_capacity(text.len() + 8);
    let mut modes = vec![Mode::Code { depth: 0 }];
    let mut iter = text.char_indices().peekable();
    let push = |out: &mut String, origin: &mut Vec<usize>, s: &str, at: usize| {
        out.push_str(s);
        origin.extend(std::iter::repeat_n(at, s.len()));
    };
    while let Some((i, c)) = iter.next() {
        let top = *modes.last().unwrap();
        match (top, c) {
            (Mode::Str, '\\') => {
                push(&mut out, &mut origin, "\\", i);
                if let Some((j, next)) = iter.next() {
                    push(&mut out, &mut origin, next.encode_utf8(&mut [0; 4]), j);
                }
            }
            (Mode::Str, '{') if iter.peek().map(|(_, c)| *c) == Some('{') => {
                iter.next();
                push(&mut out, &mut origin, "{", i);
            }
            (Mode::Str, '{') => {
                modes.push(Mode::Fill { depth: 0 });
                push(&mut out, &mut origin, "\\(", i);
            }
            (Mode::Str, '}') if iter.peek().map(|(_, c)| *c) == Some('}') => {
                iter.next();
                push(&mut out, &mut origin, "}", i);
            }
            (Mode::Fill { depth: 0 }, '}') => {
                modes.pop();
                push(&mut out, &mut origin, ")", i);
            }
            _ => {
                let mut none = std::iter::empty::<char>().peekable();
                step(&mut modes, c, &mut none);
                push(&mut out, &mut origin, c.encode_utf8(&mut [0; 4]), i);
            }
        }
    }
    origin.push(text.len());
    (out, origin)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stages(source: &str) -> Vec<Stage> {
        parse(source).unwrap_or_else(|e| panic!("{e:?}")).stages
    }

    fn first_problem(source: &str) -> (usize, usize, String) {
        let d = &parse(source).unwrap_err()[0];
        (d.line, d.column, d.message.clone())
    }

    #[test]
    fn parses_the_plan_example() {
        let pipeline = parse(
            r#"# Rooms that are too warm
from "Dev Broker"
|> filter topic "home/+/climate-1"
|> map { temp: .temperature, room: $levels[2] }
|> filter .temp > 22
|> changes
|> throttle 10s
|> to app "hot/{.room}""#,
        )
        .unwrap();
        assert_eq!(pipeline.from, Connections::Named(vec!["Dev Broker".into()]));
        assert_eq!(
            pipeline.stages,
            vec![
                Stage::FilterTopic {
                    filter: "home/+/climate-1".into(),
                    negate: false
                },
                Stage::Map("{ temp: .temperature, room: $levels[2] }".into()),
                Stage::Filter(".temp > 22".into()),
                Stage::Changes(None),
                Stage::Throttle(Duration::from_secs(10), None),
                Stage::ToApp(r#""hot/\(.room)""#.into()),
            ]
        );
    }

    #[test]
    fn from_takes_all_or_several_names() {
        assert_eq!(parse("from all").unwrap().from, Connections::All);
        assert_eq!(
            parse(r#"from "A", "B""#).unwrap().from,
            Connections::Named(vec!["A".into(), "B".into()])
        );
    }

    #[test]
    fn hash_and_pipes_inside_strings_are_not_special() {
        assert_eq!(
            stages(r#"from all |> filter topic "home/#" |> map "a |> b # c""#),
            vec![
                Stage::FilterTopic {
                    filter: "home/#".into(),
                    negate: false
                },
                Stage::Map(r#""a |> b # c""#.into()),
            ]
        );
    }

    #[test]
    fn jq_pipes_and_brackets_stay_inside_a_stage() {
        assert_eq!(
            stages("from all |> map (.a | .b) |> map [.x, .y | tostring]"),
            vec![
                Stage::Map("(.a | .b)".into()),
                Stage::Map("[.x, .y | tostring]".into())
            ]
        );
    }

    #[test]
    fn negated_topic_filter_and_inspect_labels() {
        assert_eq!(
            stages(r#"from all |> filter not topic "+/status" |> inspect |> inspect "after""#),
            vec![
                Stage::FilterTopic {
                    filter: "+/status".into(),
                    negate: true
                },
                Stage::Inspect(None),
                Stage::Inspect(Some("after".into())),
            ]
        );
    }

    #[test]
    fn durations() {
        for (text, ms) in [
            ("500ms", 500),
            ("5s", 5_000),
            ("2m", 120_000),
            ("1h", 3_600_000),
        ] {
            assert_eq!(
                stages(&format!("from all |> throttle {text}")),
                vec![Stage::Throttle(Duration::from_millis(ms), None)]
            );
        }
        assert!(parse("from all |> throttle 0s").is_err());
        assert!(parse("from all |> throttle 5").is_err());
    }

    #[test]
    fn string_filling_becomes_jq_interpolation() {
        assert_eq!(to_jq(r#""hot/{$levels[2]}""#).0, r#""hot/\($levels[2])""#);
        assert_eq!(to_jq(r#""{{literal}}""#).0, r#""{literal}""#);
        assert_eq!(to_jq(r#"{ a: "x{.b}" }"#).0, r#"{ a: "x\(.b)" }"#);
        assert_eq!(to_jq(r#""{ .a + "-{.b}" }""#).0, r#""\( .a + "-\(.b)" )""#);
        assert_eq!(to_jq(r#""esc \" {.a}""#).0, r#""esc \" \(.a)""#);
    }

    #[test]
    fn problems_have_lines_and_columns() {
        assert_eq!(
            first_problem("filter .a"),
            (
                1,
                1,
                "A Pipeline starts with `from`, for example `from \"Dev Broker\"` or `from all`"
                    .into()
            )
        );
        assert_eq!(
            first_problem("from all\n|> sort .a").2,
            "Unknown Stage `sort`. Stages are: filter, map, changes, throttle, debounce, scan, window, take, inspect, to"
        );
        assert_eq!(first_problem("from all\n|> sort .a").0, 2);
        assert_eq!(first_problem("from all\n|> sort .a").1, 4);
        assert_eq!(
            first_problem("from all |> to file \"x\"").2,
            "`to file` isn't available yet; only `to app` and `to broker` are"
        );
        assert_eq!(
            first_problem("from all |> filter topic \"a/#/b\"").2,
            "`#` must be the whole last level in a/#/b"
        );
    }

    #[test]
    fn jq_errors_point_into_the_source() {
        // `.a | foo(1)` starts at column 8 on line 2; `foo` is 5 bytes further in.
        let (line, column, message) = first_problem("from all\n|> map .a | foo(1)");
        assert_eq!(
            (line, column, message.as_str()),
            (2, 13, "unknown function `foo/1`")
        );
        // Filling shifts the jq code; the position still maps back to the source.
        let (_, column, message) = first_problem(r#"from all |> map "{.a}{nope}""#);
        assert_eq!(
            (column, message.as_str()),
            (23, "unknown function `nope/0`")
        );
    }

    #[test]
    fn stages_split_over_lines() {
        assert_eq!(
            stages("from all\n  |> filter topic \"a/#\"\n  |> changes\n"),
            vec![
                Stage::FilterTopic {
                    filter: "a/#".into(),
                    negate: false
                },
                Stage::Changes(None)
            ]
        );
    }

    #[test]
    fn keys_for_changes_and_throttle() {
        assert_eq!(
            stages("from all |> changes by .room |> throttle 5s by $levels[2]"),
            vec![
                Stage::Changes(Some(".room".into())),
                Stage::Throttle(Duration::from_secs(5), Some("$levels[2]".into())),
            ]
        );
        assert_eq!(
            first_problem("from all |> changes .room").2,
            "Expected nothing more here, or a key with `by`, for example `changes by .room`"
        );
        assert_eq!(
            first_problem("from all |> throttle 5s by").2,
            "`by` needs an expression"
        );
        assert_eq!(
            first_problem("from all |> throttle 5s by .a +").2,
            "expected term at the end"
        );
    }

    #[test]
    fn every_problem_is_reported() {
        let problems = parse("from all |> sort |> map |> throttle soon").unwrap_err();
        assert_eq!(problems.len(), 3);
    }

    #[test]
    fn records_where_each_stage_ends() {
        let source = "from all   # all of it\n|> map .a\n\n|> inspect \"x\"  \n";
        let ends = parse(source).unwrap().ends;
        let text: Vec<&str> = ends.iter().map(|&end| &source[..end]).collect();
        assert!(text[0].ends_with("from all"), "{text:?}");
        assert!(text[1].ends_with("map .a"), "{text:?}");
        assert!(text[2].ends_with("inspect \"x\""), "{text:?}");
        assert_eq!(ends.len(), 3);
    }

    #[test]
    fn parses_the_stateful_stages() {
        assert_eq!(
            stages("from all |> debounce 2s by .room |> window 1m |> window 10 by .room |> take 3"),
            vec![
                Stage::Debounce(Duration::from_secs(2), Some(".room".into())),
                Stage::Window(WindowSize::Time(Duration::from_secs(60)), None),
                Stage::Window(WindowSize::Count(10), Some(".room".into())),
                Stage::Take(3, None),
            ]
        );
    }

    #[test]
    fn scan_splits_its_start_update_and_key() {
        assert_eq!(
            stages("from all |> scan 0 ($acc + 1) by .room"),
            vec![Stage::Scan {
                start: "0".into(),
                update: "($acc + 1)".into(),
                key: Some(".room".into()),
            }]
        );
        assert_eq!(
            stages("from all |> scan { n: 0 } { n: ($acc.n + 1) }"),
            vec![Stage::Scan {
                start: "{ n: 0 }".into(),
                update: "{ n: ($acc.n + 1) }".into(),
                key: None,
            }]
        );
        // `sort_by` isn't a `by` clause.
        assert!(matches!(
            &stages("from all |> scan [] ($acc + [.] | sort_by(.t))")[0],
            Stage::Scan { key: None, .. }
        ));
    }

    #[test]
    fn scan_problems_point_at_the_update() {
        let (line, column, message) = first_problem("from all\n|> scan 0 ($acc + nope)");
        assert_eq!((line, column), (2, 19), "{message}");
        assert!(message.contains("nope"), "{message}");
        assert!(
            first_problem("from all |> scan 0")
                .2
                .contains("needs an update")
        );
        assert!(
            first_problem("from all |> map $acc").2.contains("$acc"),
            "$acc is only for scan"
        );
    }

    #[test]
    fn stateful_stage_arguments_are_checked() {
        assert!(
            first_problem("from all |> window soon")
                .2
                .contains("duration")
        );
        assert!(
            first_problem("from all |> window 0")
                .2
                .contains("number of values")
        );
        assert!(
            first_problem("from all |> take lots")
                .2
                .contains("number of values")
        );
        assert!(
            first_problem("from all |> debounce 2")
                .2
                .contains("duration")
        );
    }

    #[test]
    fn objects_may_span_lines_and_end_with_a_comma() {
        assert_eq!(
            stages("from all |> map {\n  a: .x,\n  b: [1, 2,],\n}"),
            vec![Stage::Map("{\n  a: .x,\n  b: [1, 2 ] \n}".into())]
        );
        // A comma inside a string stays.
        assert_eq!(
            stages(r#"from all |> map ",}""#),
            vec![Stage::Map(r#"",}""#.into())]
        );
    }

    #[test]
    fn constructs_that_can_recurse_forever_are_refused() {
        for source in [
            "from all |> map (def f: 1 + f; f)",
            "from all |> map [recurse(. + 1)]",
            "from all |> filter repeat(.) | . > 1",
            "from all |> map [.[] | while(. < 10; . + 1)]",
            "from all |> map until(. > 100; . * 2)",
            r#"from all |> to app "x/{recurse(.a) | .b}""#,
        ] {
            let (_, _, message) = first_problem(source);
            assert!(
                message.contains("isn't available in Pipelines"),
                "{source}: {message}"
            );
        }
        // Plain recurse, fields and keys with those names, and strings are fine.
        stages(r#"from all |> map [recurse | numbers] |> map { def: .while, until: "repeat(" }"#);
    }

    #[test]
    fn an_unmatched_bracket_in_a_string_fill_is_a_problem_not_a_panic() {
        for source in [
            r#"from all |> map "{)}""#,
            r#"from all |> map "{.tags(0]}""#,
        ] {
            assert!(parse(source).is_err(), "{source}");
        }
    }

    #[test]
    fn to_broker_takes_a_name_and_a_topic() {
        assert_eq!(
            stages(r#"from all |> to broker "Office" "hot/{.room}""#),
            vec![Stage::ToBroker {
                broker: "Office".into(),
                topic: r#""hot/\(.room)""#.into(),
            }]
        );
        for source in [
            "from all |> to broker",
            "from all |> to broker Office \"x\"",
        ] {
            assert!(
                first_problem(source)
                    .2
                    .starts_with("`to broker` takes a Broker's name"),
                "{source}"
            );
        }
        assert_eq!(
            first_problem(r#"from all |> to broker "Office""#).2,
            "`to broker` needs an expression"
        );
    }
}
