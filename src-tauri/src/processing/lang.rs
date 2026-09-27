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
    Changes,
    Throttle(Duration),
    Inspect(Option<String>),
    /// The Derived Topic, as a jq expression that yields a string.
    ToApp(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pipeline {
    pub from: Connections,
    pub stages: Vec<Stage>,
}

const STAGES: &str = "filter, map, changes, throttle, inspect, to";

/// Parses and checks a Pipeline, reporting every problem found.
pub fn parse(source: &str) -> Result<Pipeline, Vec<Diagnostic>> {
    let mut problems = Problems::new(source);
    let code = strip_comments(source);
    let mut segments = split_stages(&code).into_iter();

    let mut from = None;
    if let Some(first) = segments.next() {
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
        }
    }

    match (from, problems.list.is_empty()) {
        (Some(from), true) => Ok(Pipeline { from, stages }),
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
    let none_expected = |problems: &mut Problems, name: &str| {
        problems.at(
            arg_start,
            arg.end,
            format!("`{name}` takes nothing after it"),
        );
    };
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
            if !arg_text.is_empty() {
                none_expected(problems, "changes");
                return None;
            }
            Some(Stage::Changes)
        }
        "throttle" => match parse_duration(arg_text) {
            Some(interval) => Some(Stage::Throttle(interval)),
            None => {
                problems.at(
                    arg_start,
                    arg.end.max(arg_start + 1),
                    "`throttle` takes a duration such as `500ms`, `5s`, `1m` or `1h`",
                );
                None
            }
        },
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
                "" => {
                    problems.at(
                        arg_start,
                        arg.end.max(arg_start + 1),
                        "`to` needs a Sink, for example `to app \"hot/{$levels[2]}\"`",
                    );
                    None
                }
                other => {
                    problems.at(
                        sink.span.start,
                        sink.span.end,
                        format!("`to {other}` isn't available yet; only `to app` is"),
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

/// Where `part`, a slice of `code`, starts in it.
fn offset_in(code: &str, part: &str) -> usize {
    part.as_ptr() as usize - code.as_ptr() as usize
}

/// Checks a jq expression and returns it rewritten for jq; problems point into the source.
fn expression(code: &str, arg: Span, problems: &mut Problems, stage: &str) -> Option<String> {
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
    let (jq_code, original_offsets) = to_jq(text);
    match jq::compile(&jq_code) {
        Ok(_) => Some(jq_code),
        Err(err) => {
            let at = start
                + original_offsets
                    .get(err.offset)
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
        (Mode::Fill { depth }, ')' | ']' | '}') => *top = Mode::Fill { depth: depth - 1 },
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
                Stage::Changes,
                Stage::Throttle(Duration::from_secs(10)),
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
                vec![Stage::Throttle(Duration::from_millis(ms))]
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
            "Unknown Stage `sort`. Stages are: filter, map, changes, throttle, inspect, to"
        );
        assert_eq!(first_problem("from all\n|> sort .a").0, 2);
        assert_eq!(first_problem("from all\n|> sort .a").1, 4);
        assert_eq!(
            first_problem("from all |> to file \"x\"").2,
            "`to file` isn't available yet; only `to app` is"
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
                Stage::Changes
            ]
        );
    }

    #[test]
    fn every_problem_is_reported() {
        let problems = parse("from all |> sort |> map |> throttle soon").unwrap_err();
        assert_eq!(problems.len(), 3);
    }
}
