//! Keeps the in-app reference (`src/processing/reference.json`) honest: every example in it
//! parses, and every jq function example gives the output it claims.

use jaq_json::Val;
use serde_json::Value;

use super::{jq, lang};

fn reference() -> Value {
    serde_json::from_str(include_str!("../../../src/processing/reference.json"))
        .expect("reference.json is valid JSON")
}

fn strings<'a>(value: &'a Value, key: &str) -> impl Iterator<Item = &'a str> {
    value[key]
        .as_array()
        .unwrap_or_else(|| panic!("{key} is a list"))
        .iter()
        .map(|v| v.as_str().expect("a string"))
}

fn parses(source: &str) {
    if let Err(problems) = lang::parse(source) {
        panic!("{source:?} doesn't parse: {problems:?}");
    }
}

#[test]
fn every_stage_example_parses() {
    let reference = reference();
    for stage in reference["stages"].as_array().unwrap() {
        let name = stage["name"].as_str().unwrap();
        for example in strings(stage, "examples") {
            assert!(example.starts_with(name), "{example:?} is under {name}");
            if name == "from" {
                parses(example);
            } else {
                parses(&format!("from all |> {example}"));
            }
        }
    }
}

#[test]
fn every_variable_exists() {
    let reference = reference();
    let names: Vec<&str> = reference["variables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, jq::VARIABLES);
}

#[test]
fn every_function_example_gives_its_output() {
    let reference = reference();
    let vars: [Val; 8] = std::array::from_fn(|_| Val::Null);
    let value = |json: &str| {
        jaq_json::read::parse_single(json.as_bytes())
            .unwrap_or_else(|e| panic!("{json:?} is not JSON: {e:?}"))
    };
    let mut wrong = Vec::new();
    for function in reference["functions"].as_array().unwrap() {
        let name = function["name"].as_str().unwrap();
        let example = function["example"].as_str().unwrap();
        assert!(example.contains(name), "the example for {name} uses it");
        let filter = match jq::compile(example) {
            Ok(filter) => filter,
            Err(e) => {
                wrong.push(format!(
                    "{name}: {example:?} doesn't compile: {}",
                    e.message
                ));
                continue;
            }
        };
        let input = value(function["input"].as_str().unwrap());
        let expected = value(function["output"].as_str().unwrap());
        match jq::run(&filter, input, &vars) {
            Ok(results) if results.first() == Some(&expected) => {}
            other => wrong.push(format!(
                "{name}: {example:?} gave {other:?}, not {expected}"
            )),
        }
        parses(&format!("from all |> map {example}"));
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
