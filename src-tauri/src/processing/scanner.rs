//! The one place that decides how a Pipeline's text divides up: what is code, what is inside a
//! string, what is a `{…}` fill, what is a comment. Every pass in the parser reads these spans
//! instead of walking characters itself, so they can't disagree. The editor's
//! `src/processing/scanner.ts` follows the same rules, and both run `scanCases.json`.
//!
//! The rules:
//! - `"` opens a string in code, and closes it inside one.
//! - In a string, `{…}` is a fill (code, which may hold strings of its own), as is jq's own
//!   `\(…)`. `{{` and `}}` are literal braces. `\` escapes the next character.
//! - In code, `( [ {` and `) ] }` nest; a bracket belongs to the level outside it.
//! - `#` in code (fills included) starts a comment, to the end of the line.
//! - Any text scans, and never panics; what is still open at the end is reported, not fixed.

/// What a stretch of text is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Code: Stages, jq, and inside fills.
    Code,
    /// From `#` to the end of the line (the line break isn't part of it).
    Comment,
    /// A `"` opening or closing a string.
    Quote,
    /// Literal text inside a string.
    Text,
    /// `\` and the character it escapes, inside a string.
    Escape,
    /// `{{` or `}}` inside a string: one literal brace.
    LiteralBrace,
    /// The `{` opening a fill, or jq's `\(`.
    FillOpen,
    /// The `}` or `)` closing a fill.
    FillClose,
}

/// A stretch of one kind, in source order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Piece {
    pub kind: Kind,
    /// Byte range in the text.
    pub start: usize,
    pub end: usize,
    /// How many strings it's inside: 0 is the Pipeline's own code.
    pub strings: usize,
    /// Bracket depth in its code (the Pipeline's, or the fill's it's in). Runs of code break at
    /// brackets, so a piece has one depth; a bracket itself has the depth outside it.
    pub depth: usize,
}

impl Piece {
    /// Code at the Pipeline's top level: outside every string and bracket.
    pub fn is_top(&self) -> bool {
        self.kind == Kind::Code && self.strings == 0 && self.depth == 0
    }
}

/// What is still open when the text ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Open {
    /// A string, opened at this byte.
    String(usize),
    /// A `{…}` or `\(…)` fill, opened at this byte.
    Fill(usize),
    /// A bracket in code, opened at this byte.
    Bracket(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scan {
    pub pieces: Vec<Piece>,
    /// Innermost last.
    pub open: Vec<Open>,
}

impl Scan {
    /// Every character with the piece it's in.
    pub fn chars<'a>(&'a self, text: &'a str) -> impl Iterator<Item = (usize, char, &'a Piece)> {
        self.pieces.iter().flat_map(move |piece| {
            text[piece.start..piece.end]
                .char_indices()
                .map(move |(i, c)| (piece.start + i, c, piece))
        })
    }

    /// Where the string opening at byte `start` ends: just past its closing quote. None if it
    /// doesn't open a string there, or never closes.
    pub fn string_end(&self, start: usize) -> Option<usize> {
        let open = self
            .pieces
            .iter()
            .position(|p| p.kind == Kind::Quote && p.start == start)?;
        let level = self.pieces[open].strings;
        self.pieces[open + 1..]
            .iter()
            .find(|p| p.kind == Kind::Quote && p.strings == level)
            .map(|p| p.end)
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Context {
    /// The Pipeline's code, or a fill's: brackets nest; a fill also remembers how it closes.
    Code {
        depth: usize,
        closes: Option<char>,
    },
    Str,
}

/// Divides `text` into pieces by the rules above.
pub fn scan(text: &str) -> Scan {
    let mut stack = vec![Context::Code {
        depth: 0,
        closes: None,
    }];
    // Where each open string, fill or bracket started, for reporting what's left open.
    let mut opened: Vec<Open> = Vec::new();
    let mut pieces: Vec<Piece> = Vec::new();
    let strings = |stack: &[Context]| stack.iter().filter(|c| **c == Context::Str).count();
    let push = |pieces: &mut Vec<Piece>, kind, start, end, strings, depth| {
        // Runs of code, text and comment grow; the rest stand alone.
        if let Some(last) = pieces.last_mut()
            && matches!(kind, Kind::Code | Kind::Text)
            && last.kind == kind
            && last.end == start
            && last.strings == strings
            && last.depth == depth
        {
            last.end = end;
        } else {
            pieces.push(Piece {
                kind,
                start,
                end,
                strings,
                depth,
            });
        }
    };

    let mut iter = text.char_indices().peekable();
    while let Some((i, c)) = iter.next() {
        let next = iter.peek().map(|&(_, n)| n);
        let n = strings(&stack);
        let end = i + c.len_utf8();
        match *stack.last().unwrap() {
            Context::Str => match c {
                '"' => {
                    push(&mut pieces, Kind::Quote, i, end, n, 0);
                    stack.pop();
                    opened.pop();
                }
                '\\' if next == Some('(') => {
                    iter.next();
                    push(&mut pieces, Kind::FillOpen, i, end + 1, n, 0);
                    stack.push(Context::Code {
                        depth: 0,
                        closes: Some(')'),
                    });
                    opened.push(Open::Fill(i));
                }
                '\\' => {
                    let escaped = iter.next().map_or(0, |(_, e)| e.len_utf8());
                    push(&mut pieces, Kind::Escape, i, end + escaped, n, 0);
                }
                '{' | '}' if next == Some(c) => {
                    iter.next();
                    push(&mut pieces, Kind::LiteralBrace, i, end + 1, n, 0);
                }
                '{' => {
                    push(&mut pieces, Kind::FillOpen, i, end, n, 0);
                    stack.push(Context::Code {
                        depth: 0,
                        closes: Some('}'),
                    });
                    opened.push(Open::Fill(i));
                }
                _ => push(&mut pieces, Kind::Text, i, end, n, 0),
            },
            Context::Code { depth, closes } => match c {
                '#' => {
                    let line_end = text[i..].find('\n').map_or(text.len(), |at| i + at);
                    while iter.peek().is_some_and(|&(j, _)| j < line_end) {
                        iter.next();
                    }
                    push(&mut pieces, Kind::Comment, i, line_end, n, depth);
                }
                '"' => {
                    push(&mut pieces, Kind::Quote, i, end, n + 1, 0);
                    stack.push(Context::Str);
                    opened.push(Open::String(i));
                }
                _ if depth == 0 && closes == Some(c) => {
                    push(&mut pieces, Kind::FillClose, i, end, n, 0);
                    stack.pop();
                    opened.pop();
                }
                '(' | '[' | '{' => {
                    push(&mut pieces, Kind::Code, i, end, n, depth);
                    set_depth(&mut stack, depth + 1);
                    opened.push(Open::Bracket(i));
                }
                ')' | ']' | '}' => {
                    let outside = depth.saturating_sub(1);
                    push(&mut pieces, Kind::Code, i, end, n, outside);
                    set_depth(&mut stack, outside);
                    if depth > 0 {
                        opened.pop();
                    }
                }
                _ => push(&mut pieces, Kind::Code, i, end, n, depth),
            },
        }
    }
    Scan {
        pieces,
        open: opened,
    }
}

fn set_depth(stack: &mut [Context], depth: usize) {
    if let Some(Context::Code { depth: d, .. }) = stack.last_mut() {
        *d = depth;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    /// The pieces as `kind:text` words, which is what scanCases.json writes down.
    pub(super) fn shown(text: &str) -> Vec<String> {
        scan(text)
            .pieces
            .iter()
            .map(|p| {
                let kind = match p.kind {
                    Kind::Code => "code",
                    Kind::Comment => "comment",
                    Kind::Quote => "quote",
                    Kind::Text => "text",
                    Kind::Escape => "escape",
                    Kind::LiteralBrace => "brace",
                    Kind::FillOpen => "fill",
                    Kind::FillClose => "end",
                };
                format!("{kind}{}{}:{}", p.strings, p.depth, &text[p.start..p.end])
            })
            .collect()
    }

    pub(super) fn open(text: &str) -> Vec<String> {
        scan(text)
            .open
            .iter()
            .map(|o| match o {
                Open::String(at) => format!("string@{at}"),
                Open::Fill(at) => format!("fill@{at}"),
                Open::Bracket(at) => format!("bracket@{at}"),
            })
            .collect()
    }

    /// The cases the editor's scanner runs too.
    #[test]
    fn the_shared_cases() {
        let cases: Value =
            serde_json::from_str(include_str!("../../../src/processing/scanCases.json")).unwrap();
        let mut wrong = Vec::new();
        for case in cases.as_array().unwrap() {
            let text = case["text"].as_str().unwrap();
            let pieces: Vec<String> = case["pieces"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| p.as_str().unwrap().to_string())
                .collect();
            let opens: Vec<String> = case["open"]
                .as_array()
                .map(|o| o.iter().map(|v| v.as_str().unwrap().to_string()).collect())
                .unwrap_or_default();
            if shown(text) != pieces || open(text) != opens {
                wrong.push(format!(
                    "{text:?}\n  pieces {:?}\n  open {:?}",
                    shown(text),
                    open(text)
                ));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    #[test]
    fn any_text_scans_and_the_pieces_cover_it() {
        let texts = [
            r#"from "a" |> map { s: "x{.a + "y{.b}"}z", t: "\(.c + "d")" } # done"#,
            "\"{{}}\\\"\\",
            "}}}{{{\"\"\"",
            "map \"é{.ü}ø\" # ≥",
        ];
        for text in texts {
            for (end, _) in text.char_indices().chain([(text.len(), ' ')]) {
                let prefix = &text[..end];
                let scan = scan(prefix);
                let mut at = 0;
                for piece in &scan.pieces {
                    assert_eq!(piece.start, at, "{prefix:?}: a gap or overlap");
                    at = piece.end;
                }
                assert_eq!(at, prefix.len(), "{prefix:?}: not all covered");
            }
        }
    }

    #[test]
    fn a_strings_end_follows_its_fills() {
        let text = r#""a{.b + "}"}c" rest"#;
        assert_eq!(scan(text).string_end(0), Some(14));
        assert_eq!(scan(r#""open"#).string_end(0), None);
        assert_eq!(scan("x").string_end(0), None);
    }
}

#[cfg(test)]
mod print_cases {
    #[test]
    #[ignore = "prints the shared cases' pieces, for writing scanCases.json"]
    fn print() {
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("../../../src/processing/scanCases.json")).unwrap();
        let mut out = Vec::new();
        for case in cases.as_array().unwrap() {
            let text = case["text"].as_str().unwrap();
            out.push(serde_json::json!({
                "text": text,
                "pieces": super::tests::shown(text),
                "open": super::tests::open(text),
            }));
        }
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    }
}
