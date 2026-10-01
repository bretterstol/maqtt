//! Temporary: the parser's scanning functions as they were before the single scanner, kept
//! only to prove each pass that moves onto `scanner` gives the same answers. Deleted once the
//! last pass has moved.
#![allow(dead_code)]

use super::Span;

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
            // `{{` is a literal brace: keep both, where `step` would consume the second.
            (Mode::Str, '{') if chars.peek() == Some(&'{') => {
                chars.next();
                out.push_str("{{");
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

/// Where a quoted string at the start of `text` ends (just past its closing quote), following
/// `{…}` fills and the strings inside them.
fn quoted_end(text: &str) -> Option<usize> {
    if !text.starts_with('"') {
        return None;
    }
    let mut modes = vec![Mode::Code { depth: 0 }];
    let mut iter = text.char_indices().peekable();
    while let Some((i, c)) = iter.next() {
        let top = *modes.last().unwrap();
        if top == Mode::Str
            && (c == '\\' || (c == '{' && iter.peek().is_some_and(|(_, n)| *n == '{')))
        {
            iter.next();
            continue;
        }
        step(&mut modes, c, &mut std::iter::empty().peekable());
        if modes.len() == 1 {
            return Some(i + c.len_utf8());
        }
    }
    None
}

/// Rewrites måqtt's `{…}` string filling into jq's `\(…)`, and `{{` and `}}` into literal braces
/// (as in Python's f-strings). Also returns, for every byte of the result, the byte offset in
/// `text` it came from, to map jq's errors back.
fn to_jq(text: &str) -> (String, Vec<usize>) {
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

/// Every prefix of these is compared, old against new.
const CORPUS: [&str; 6] = [
    r#"# Rooms
from "Dev Broker", "Office"
|> filter topic "maqtt/home/+room/#rest", "x/+"
|> map { temp: .temperature, room: $room, s: "{.a}°{{x}}", t: "a}b" }
|> map [.[] | select(. > 1)] # keep the big ones
|> scan 0 ($acc + 1) by .room
|> window 1m by $room
|> map { room: $key, avg: avg(.temp), n: count, }
|> join by $room as $rest needs "a", "b" within 5m
|> to app "hot/{.room}"
|> to broker "Office" "copy/{$topic}"
|> to file "rooms/{$room // "x"}.csv" rotate 10MB"#,
    r#"from file "rec.jsonl", "b.csv" follow replay .at |> map .payload"#,
    r#"from all |> map "{ .a + "-{.b}" }" |> filter (.a | not) |> map "esc \" {.a}""#,
    r#"from all |> map "{.a # c}" |> inspect "x""#,
    r#"from all |> map { a: [1, (2, 3)], b: "{[1,2] | .[0]}", } |> map .a"#,
    r#"from all |> map recurse(.a) |> map def f: 1; f |> map "{{" |> map "}}""#,
];

fn prefixes() -> impl Iterator<Item = &'static str> {
    CORPUS.into_iter().flat_map(|text| {
        text.char_indices()
            .map(|(i, _)| i)
            .chain([text.len()])
            .map(move |end| &text[..end])
    })
}

#[test]
fn new_passes_give_the_old_answers() {
    let mut differ = Vec::new();
    let mut check = |pass: &str, text: &str, old: String, new: String| {
        if old != new && differ.len() < 20 {
            differ.push(format!("{pass} on {text:?}:\n  old {old}\n  new {new}"));
        }
    };
    for text in prefixes() {
        check(
            "strip_comments",
            text,
            strip_comments(text),
            super::strip_comments(text),
        );
        let code = strip_comments(text);
        check(
            "split_stages",
            &code,
            format!("{:?}", split_stages(&code)),
            format!("{:?}", super::split_stages(&code)),
        );
        // Intended differences: a closing bracket now counts as the level outside it, and both
        // quotes of a string as inside it. Neither is a space or a word, which is all callers
        // look for.
        let closing = |c: char| matches!(c, ')' | ']' | '}' | '"');
        // The old walk also skipped `{{`'s second brace; callers only use top-level characters,
        // so those are what's compared.
        let mut old = Vec::new();
        scan_code(&code, |i, c, top| {
            if top && !closing(c) {
                old.push((i, c));
            }
        });
        let mut new = Vec::new();
        super::scan_code(&code, |i, c, top| {
            if top && !closing(c) {
                new.push((i, c));
            }
        });
        check("scan_code", &code, format!("{old:?}"), format!("{new:?}"));
        check(
            "endless_construct",
            &code,
            format!("{:?}", endless_construct(&code)),
            format!("{:?}", super::endless_construct(&code)),
        );
        check(
            "drop_trailing_commas",
            &code,
            drop_trailing_commas(&code),
            super::drop_trailing_commas(&code),
        );
        check(
            "to_jq",
            &code,
            format!("{:?}", to_jq(&code)),
            format!("{:?}", super::to_jq(&code)),
        );
        // Names are read where a string opens in the Pipeline's own code, as callers do.
        let mut opening = Vec::new();
        scan_code(&code, |i, c, top| {
            if top && c == '"' {
                opening.push(i);
            }
        });
        for at in opening {
            let span = Span {
                start: at,
                end: code.len(),
            };
            // Intended difference: a name's end now follows `{…}` fills (the old reader
            // stopped at a quote inside one), so only names without a brace are compared.
            let old = string_literal(&code, span);
            if old.as_ref().is_none_or(|(name, _)| !name.contains('{')) {
                check(
                    "string_literal",
                    &code[at..],
                    format!("{old:?}"),
                    format!("{:?}", super::string_literal(&code, span)),
                );
            }
            check(
                "quoted_end",
                &code[at..],
                format!("{:?}", quoted_end(&code[at..])),
                format!("{:?}", super::quoted_end(&code[at..])),
            );
        }
    }
    assert!(differ.is_empty(), "{}", differ.join("\n"));
}
