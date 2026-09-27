//! jq expressions inside Stages, compiled once per Pipeline and run per message with the
//! message's variables bound.

use jaq_core::load::{Arena, File, Loader};
use jaq_core::{Compiler, Ctx, Vars, data, unwrap_valr};
use jaq_json::Val;

/// The variables every expression can use, in the order their values are supplied.
pub const VARIABLES: [&str; 8] = [
    "$payload",
    "$topic",
    "$levels",
    "$connection",
    "$time",
    "$qos",
    "$retained",
    "$properties",
];

pub type Filter = jaq_core::compile::Filter<jaq_core::Native<data::JustLut<Val>>>;

/// What is wrong with a jq expression, and where (a byte offset into the expression).
#[derive(Debug, Clone, PartialEq)]
pub struct CompileError {
    pub offset: usize,
    pub message: String,
}

/// Compiles one jq expression, or says what is wrong with it.
pub fn compile(code: &str) -> Result<Filter, CompileError> {
    use jaq_core::load::Error;

    let defs = jaq_core::defs()
        .chain(jaq_std::defs())
        .chain(jaq_json::defs());
    let funs = jaq_core::funs()
        .chain(jaq_std::funs())
        .chain(jaq_json::funs());
    // jaq reports where things go wrong as the rest of the code from that point.
    let at = |rest: &str| {
        (rest.as_ptr() as usize)
            .saturating_sub(code.as_ptr() as usize)
            .min(code.len())
    };
    let arena = Arena::default();
    let modules = Loader::new(defs)
        .load(&arena, File { code, path: () })
        .map_err(|errs| {
            let (offset, message) = match errs.into_iter().next().map(|(_, e)| e) {
                Some(Error::Lex(e)) => e
                    .first()
                    .map_or((0, "can't read this".into()), |(expect, rest)| {
                        (at(rest), format!("expected {}", expect.as_str()))
                    }),
                Some(Error::Parse(e)) => {
                    e.first()
                        .map_or((0, "can't read this".into()), |(expect, rest)| {
                            let message = if rest.is_empty() {
                                format!("expected {} at the end", expect.as_str())
                            } else {
                                format!("expected {}", expect.as_str())
                            };
                            (at(rest), message)
                        })
                }
                Some(Error::Io(e)) => {
                    (0, e.first().map_or("can't load".into(), |(_, m)| m.clone()))
                }
                _ => (0, "can't read this expression".into()),
            };
            CompileError { offset, message }
        })?;
    Compiler::default()
        .with_funs(funs)
        .with_global_vars(VARIABLES)
        .compile(modules)
        .map_err(|errs| {
            let (name, undefined) = errs
                .into_iter()
                .flat_map(|(_, e)| e)
                .next()
                .expect("compile errors are never empty");
            let message = match undefined {
                jaq_core::compile::Undefined::Filter(arity) => {
                    format!("unknown function `{name}/{arity}`")
                }
                other => format!("unknown {} `{name}`", other.as_str()),
            };
            CompileError {
                offset: at(name),
                message,
            }
        })
}

/// Runs a compiled expression on `input` with the message variables; every output, or the first error.
pub fn run(filter: &Filter, input: Val, vars: &[Val; 8]) -> Result<Vec<Val>, String> {
    let ctx = Ctx::<data::JustLut<Val>>::new(&filter.lut, Vars::new(vars.iter().cloned()));
    let results: Vec<Val> = filter
        .id
        .run((ctx, input))
        .map(unwrap_valr)
        .map(|r| r.map_err(|err| err.to_string()))
        // One past the limit is enough to know it's over, and stops an endless stream.
        .take(MAX_RESULTS + 1)
        .collect::<Result<_, _>>()?;
    if results.len() > MAX_RESULTS {
        return Err(format!("it gave more than {MAX_RESULTS} results"));
    }
    Ok(results)
}

/// The most results one expression may give for one value.
pub const MAX_RESULTS: usize = 10_000;

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(topic: &str) -> [Val; 8] {
        let levels: Val = topic.split('/').map(|l| Val::from(l.to_string())).collect();
        [
            Val::Null,
            Val::from(topic.to_string()),
            levels,
            Val::from("Dev Broker".to_string()),
            Val::from(0usize),
            Val::from(0usize),
            Val::Bool(false),
            Val::Null,
        ]
    }

    fn json(s: &str) -> Val {
        jaq_json::read::parse_single(s.as_bytes()).unwrap()
    }

    #[test]
    fn spike_conditions_objects_and_variables() {
        let input = json(r#"{"temperature": 23.5}"#);
        let cond = compile(".temperature > 22").unwrap();
        assert_eq!(
            run(&cond, input.clone(), &vars("a/b")).unwrap()[0].to_string(),
            "true"
        );

        let shape = compile(r#"{temp: .temperature, room: $levels[1]}"#).unwrap();
        assert_eq!(
            run(&shape, input, &vars("home/kitchen/t")).unwrap()[0].to_string(),
            r#"{"temp":23.5,"room":"kitchen"}"#
        );
    }

    #[test]
    fn spike_many_results_and_errors() {
        let fan = compile(".[]").unwrap();
        assert_eq!(run(&fan, json("[1,2,3]"), &vars("t")).unwrap().len(), 3);
        assert_eq!(
            compile(".temperature >").err().map(|e| e.message),
            Some("expected term at the end".into())
        );
        assert_eq!(
            compile("$nope").err(),
            Some(CompileError {
                offset: 0,
                message: "unknown variable `$nope`".into()
            })
        );
        assert_eq!(
            compile(".a | foo(1)").err().map(|e| (e.offset, e.message)),
            Some((5, "unknown function `foo/1`".into()))
        );
        let bad = compile(".a + 1").unwrap();
        assert!(run(&bad, json(r#"{"a":"x"}"#), &vars("t")).is_err());
    }

    #[test]
    fn an_endless_stream_stops_at_the_limit() {
        let endless = compile("range(1; infinite)").unwrap();
        let err = run(&endless, Val::Null, &vars("a")).unwrap_err();
        assert!(err.contains("more than 10000 results"), "{err}");
    }
}
