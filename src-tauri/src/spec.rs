//! API Specs: AsyncAPI documents (2.x and 3.x, YAML or JSON) attached to a Broker Profile,
//! describing its topics (Channels) and their messages' payloads. mÅqtt reads them for
//! suggestions when publishing, and checks payloads against their schemas.
//!
//! A spec is read from where it lives, and read again when the file changes. `$ref`s are
//! inlined: within the document, and to other files relative to it. References to the web
//! aren't followed; what they point at is taken as "anything".

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::Serialize;
use serde_json::{Map, Value};

/// How deep `$ref`s are followed; a schema that refers to itself stops here.
const MAX_REF_DEPTH: usize = 32;

/// One spec file, as read.
#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "generated/")]
pub struct SpecView {
    pub path: String,
    /// None when it couldn't be read; `problem` says why.
    pub spec: Option<Spec>,
    pub problem: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "generated/")]
pub struct Spec {
    pub title: String,
    pub version: String,
    /// `2.6.0`, `3.0.0`.
    pub asyncapi: String,
    pub channels: Vec<SpecChannel>,
}

/// A topic the spec describes. Its address may hold parameters, `{room}`, one level each.
#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "generated/")]
pub struct SpecChannel {
    pub address: String,
    pub description: Option<String>,
    pub parameters: Vec<SpecParameter>,
    pub messages: Vec<SpecMessage>,
}

#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "generated/")]
pub struct SpecParameter {
    pub name: String,
    pub description: Option<String>,
    /// Values the spec names for it: its enum, default and examples.
    pub values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "generated/")]
pub struct SpecMessage {
    pub name: String,
    pub summary: Option<String>,
    /// The payload's JSON Schema, `$ref`s inlined. None when the spec gives none, or gives it
    /// in another format (Avro, Protobuf).
    #[ts(type = "unknown")]
    pub payload: Option<Value>,
    /// Example payloads.
    #[ts(type = "Array<unknown>")]
    pub examples: Vec<Value>,
}

/// Reads one spec file.
pub fn load(path: &Path) -> Result<Spec, String> {
    let mut docs = Docs::default();
    let root = docs.read(path)?;
    let doc = docs.get(&root).clone();
    let asyncapi = doc
        .get("asyncapi")
        .and_then(Value::as_str)
        .ok_or("This isn't an AsyncAPI document: it has no `asyncapi` version")?
        .to_string();
    let info = doc.get("info");
    let text = |key: &str| {
        info.and_then(|i| i.get(key))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let mut resolver = Resolver {
        docs,
        stack: Vec::new(),
    };
    let channels = match asyncapi.split('.').next() {
        Some("2") => channels_v2(&doc, &root, &mut resolver),
        Some("3") => channels_v3(&doc, &root, &mut resolver),
        _ => {
            return Err(format!(
                "AsyncAPI {asyncapi} isn't supported; 2.x and 3.x are"
            ));
        }
    };
    Ok(Spec {
        title: text("title"),
        version: text("version"),
        asyncapi,
        channels,
    })
}

fn channels_v2(doc: &Value, file: &Path, r: &mut Resolver) -> Vec<SpecChannel> {
    let Some(channels) = doc.get("channels").and_then(Value::as_object) else {
        return Vec::new();
    };
    channels
        .iter()
        .map(|(address, item)| {
            let item = r.resolve(item, file);
            let mut messages = Vec::new();
            for op in ["publish", "subscribe"] {
                let Some(message) = item.get(op).and_then(|o| o.get("message")) else {
                    continue;
                };
                let message = r.resolve(message, file);
                match message.get("oneOf").and_then(Value::as_array) {
                    Some(each) => {
                        for m in each {
                            add_message(&mut messages, message_of(&r.resolve(m, file), 2));
                        }
                    }
                    None => add_message(&mut messages, message_of(&message, 2)),
                }
            }
            SpecChannel {
                address: address.clone(),
                description: string(&item, "description"),
                parameters: parameters(&item, file, r, 2),
                messages,
            }
        })
        .collect()
}

fn channels_v3(doc: &Value, file: &Path, r: &mut Resolver) -> Vec<SpecChannel> {
    let Some(channels) = doc.get("channels").and_then(Value::as_object) else {
        return Vec::new();
    };
    channels
        .iter()
        .filter_map(|(id, channel)| {
            let channel = r.resolve(channel, file);
            // A null address is set at run time; there's no topic to offer.
            let address = match channel.get("address") {
                Some(Value::String(address)) => address.clone(),
                Some(Value::Null) => return None,
                _ => id.clone(),
            };
            let mut messages = Vec::new();
            if let Some(each) = channel.get("messages").and_then(Value::as_object) {
                for (key, m) in each {
                    let mut message = message_of(&r.resolve(m, file), 3);
                    if message.name.is_empty() {
                        message.name = key.clone();
                    }
                    add_message(&mut messages, message);
                }
            }
            Some(SpecChannel {
                address,
                description: string(&channel, "description"),
                parameters: parameters(&channel, file, r, 3),
                messages,
            })
        })
        .collect()
}

fn parameters(channel: &Value, file: &Path, r: &mut Resolver, major: u8) -> Vec<SpecParameter> {
    let Some(each) = channel.get("parameters").and_then(Value::as_object) else {
        return Vec::new();
    };
    each.iter()
        .map(|(name, p)| {
            let p = r.resolve(p, file);
            // 2.x describes a parameter with a schema; 3.x lists its values directly.
            let source = if major == 2 {
                p.get("schema").cloned().unwrap_or_default()
            } else {
                p.clone()
            };
            let mut values = Vec::new();
            let mut add = |v: &Value| {
                let text = match v {
                    Value::String(s) => s.clone(),
                    Value::Null => return,
                    other => other.to_string(),
                };
                if !values.contains(&text) {
                    values.push(text);
                }
            };
            for key in ["enum", "examples"] {
                if let Some(list) = source.get(key).and_then(Value::as_array) {
                    list.iter().for_each(&mut add);
                }
            }
            if let Some(default) = source.get("default") {
                add(default);
            }
            SpecParameter {
                name: name.clone(),
                description: string(&p, "description"),
                values,
            }
        })
        .collect()
}

fn message_of(message: &Value, major: u8) -> SpecMessage {
    // A payload in another schema format (Avro, Protobuf) can't be checked here.
    let json_schema = |format: Option<&str>| {
        format.is_none_or(|f| {
            f.starts_with("application/vnd.aai.asyncapi")
                || f.starts_with("application/schema+json")
                || f.starts_with("application/schema+yaml")
        })
    };
    let payload = message.get("payload").and_then(|payload| {
        if major == 3
            && let Some(schema) = payload.get("schema")
            && payload.get("schemaFormat").is_some()
        {
            let format = payload.get("schemaFormat").and_then(Value::as_str);
            return json_schema(format).then(|| schema.clone());
        }
        let format = message.get("schemaFormat").and_then(Value::as_str);
        json_schema(format).then(|| payload.clone())
    });
    let examples = message
        .get("examples")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|e| e.get("payload").cloned())
                .collect()
        })
        .unwrap_or_default();
    SpecMessage {
        name: string(message, "name")
            .or_else(|| string(message, "title"))
            .unwrap_or_default(),
        summary: string(message, "summary").or_else(|| string(message, "title")),
        payload,
        examples,
    }
}

/// Adds a message unless the channel has it already (2.x often names one for both ways).
fn add_message(messages: &mut Vec<SpecMessage>, message: SpecMessage) {
    if !messages.contains(&message) {
        messages.push(message);
    }
}

fn string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_string)
}

/// The documents read so far, by path.
#[derive(Default)]
struct Docs {
    docs: HashMap<PathBuf, Value>,
}

impl Docs {
    fn read(&mut self, path: &Path) -> Result<PathBuf, String> {
        let path = path
            .canonicalize()
            .map_err(|e| format!("Couldn't read {}: {e}", path.display()))?;
        if !self.docs.contains_key(&path) {
            let text = std::fs::read_to_string(&path)
                .map_err(|e| format!("Couldn't read {}: {e}", path.display()))?;
            let doc = parse(&text).map_err(|e| format!("{}: {e}", path.display()))?;
            self.docs.insert(path.clone(), doc);
        }
        Ok(path)
    }

    fn get(&self, path: &Path) -> &Value {
        &self.docs[path]
    }
}

/// YAML or JSON (which is also YAML) into JSON values.
fn parse(text: &str) -> Result<Value, String> {
    serde_norway::from_str(text).map_err(|e| e.to_string())
}

/// Inlines `$ref`s, following them into other files relative to the one they're in.
struct Resolver {
    docs: Docs,
    /// The references being followed, to stop at one that refers back to itself.
    stack: Vec<String>,
}

impl Resolver {
    /// `value` with every `$ref` in it inlined; `file` is the document it's in.
    fn resolve(&mut self, value: &Value, file: &Path) -> Value {
        match value {
            Value::Object(map) => {
                if let Some(Value::String(reference)) = map.get("$ref") {
                    return self.follow(reference, file);
                }
                let resolved: Map<String, Value> = map
                    .iter()
                    .map(|(k, v)| (k.clone(), self.resolve(v, file)))
                    .collect();
                Value::Object(resolved)
            }
            Value::Array(items) => {
                Value::Array(items.iter().map(|v| self.resolve(v, file)).collect())
            }
            other => other.clone(),
        }
    }

    fn follow(&mut self, reference: &str, file: &Path) -> Value {
        let (target, pointer) = reference.split_once('#').unwrap_or((reference, ""));
        if target.contains("://") {
            return Value::Object(Map::new());
        }
        let target = if target.is_empty() {
            file.to_path_buf()
        } else {
            match file.parent().map(|dir| self.docs.read(&dir.join(target))) {
                Some(Ok(path)) => path,
                _ => return Value::Object(Map::new()),
            }
        };
        let key = format!("{}#{pointer}", target.display());
        if self.stack.contains(&key) || self.stack.len() >= MAX_REF_DEPTH {
            return Value::Object(Map::new());
        }
        let Some(found) = self.docs.get(&target).pointer(pointer).cloned() else {
            return Value::Object(Map::new());
        };
        self.stack.push(key);
        let resolved = self.resolve(&found, &target);
        self.stack.pop();
        resolved
    }
}

/// Reads specs again only when their files change.
#[derive(Default)]
pub struct SpecCache {
    read: HashMap<String, (Option<SystemTime>, SpecView)>,
}

impl SpecCache {
    pub fn get(&mut self, path: &str) -> SpecView {
        let modified = std::fs::metadata(path).and_then(|m| m.modified()).ok();
        if let Some((at, view)) = self.read.get(path)
            && *at == modified
            && modified.is_some()
        {
            return view.clone();
        }
        let view = match load(Path::new(path)) {
            Ok(spec) => SpecView {
                path: path.to_string(),
                spec: Some(spec),
                problem: None,
            },
            Err(problem) => SpecView {
                path: path.to_string(),
                spec: None,
                problem: Some(problem),
            },
        };
        self.read.insert(path.to_string(), (modified, view.clone()));
        view
    }
}

/// Something in a payload its schema doesn't allow.
#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "generated/")]
pub struct PayloadProblem {
    /// Where in the payload: the keys and indexes down to it; empty for the whole payload.
    pub path: Vec<String>,
    pub message: String,
}

/// A message schema made ready to check payloads with; None for one that doesn't compile.
pub fn compile(schema: &Value) -> Option<jsonschema::Validator> {
    jsonschema::draft7::new(schema).ok()
}

/// What a payload does against each of a channel's message validators: the problems with the
/// one it fits best, none when it fits one. Problems with the payload as a whole (a missing
/// field, the wrong type) say it's another message more than problems inside it do.
pub fn best_fit(validators: &[jsonschema::Validator], instance: &Value) -> Vec<PayloadProblem> {
    let mut best: Option<Vec<PayloadProblem>> = None;
    for validator in validators {
        let problems: Vec<PayloadProblem> = validator
            .iter_errors(instance)
            .map(|error| PayloadProblem {
                path: error
                    .instance_path()
                    .into_iter()
                    .map(|segment| match segment {
                        jsonschema::paths::LocationSegment::Property(p) => p.to_string(),
                        jsonschema::paths::LocationSegment::Index(i) => i.to_string(),
                    })
                    .collect(),
                message: error.to_string(),
            })
            .collect();
        if problems.is_empty() {
            return problems;
        }
        let fit = |p: &[PayloadProblem]| (p.iter().filter(|p| p.path.is_empty()).count(), p.len());
        if best.as_ref().is_none_or(|b| fit(&problems) < fit(b)) {
            best = Some(problems);
        }
    }
    best.unwrap_or_default()
}

/// Checks a payload against a channel's message schemas, as [`best_fit`] does. A payload that
/// isn't JSON has no problems here; the editor reports that itself.
pub fn check_payload(schemas: &[Value], payload: &str) -> Vec<PayloadProblem> {
    let Ok(instance) = serde_json::from_str::<Value>(payload) else {
        return Vec::new();
    };
    let validators: Vec<_> = schemas.iter().filter_map(compile).collect();
    best_fit(&validators, &instance)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("maqtt-spec-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    const V2: &str = r##"
asyncapi: 2.6.0
info: { title: Home, version: 1.2.0 }
channels:
  home/{room}/climate:
    description: A room's climate
    parameters:
      room:
        description: The room
        schema: { type: string, enum: [kitchen, office], default: kitchen }
    publish:
      message: { $ref: "#/components/messages/Climate" }
    subscribe:
      message: { $ref: "#/components/messages/Climate" }
  home/{room}/light:
    subscribe:
      message:
        oneOf:
          - { name: On, payload: { type: object, properties: { on: { const: true } } } }
          - { name: Level, payload: { $ref: "shared.yaml#/Level" } }
  home/avro:
    publish:
      message: { schemaFormat: application/vnd.apache.avro;version=1.9.0, payload: { type: record } }
components:
  messages:
    Climate:
      name: Climate
      summary: Temperature and humidity
      payload: { $ref: "#/components/schemas/Climate" }
      examples:
        - payload: { temperature: 21.5, humidity: 40 }
  schemas:
    Climate:
      type: object
      required: [temperature]
      properties:
        temperature: { type: number }
        humidity: { type: integer, minimum: 0, maximum: 100 }
        parent: { $ref: "#/components/schemas/Climate" }
"##;

    #[test]
    fn reads_asyncapi_2_with_refs_across_files_and_back_to_itself() {
        let d = dir("v2");
        std::fs::write(d.join("home.yaml"), V2).unwrap();
        std::fs::write(
            d.join("shared.yaml"),
            "Level: { type: object, properties: { level: { type: integer } } }\n",
        )
        .unwrap();
        let spec = load(&d.join("home.yaml")).unwrap();
        assert_eq!(
            (spec.title.as_str(), spec.version.as_str()),
            ("Home", "1.2.0")
        );
        let addresses: Vec<&str> = spec.channels.iter().map(|c| c.address.as_str()).collect();
        assert_eq!(
            addresses,
            ["home/{room}/climate", "home/{room}/light", "home/avro"]
        );

        let climate = &spec.channels[0];
        assert_eq!(climate.description.as_deref(), Some("A room's climate"));
        assert_eq!(climate.parameters[0].name, "room");
        assert_eq!(climate.parameters[0].values, ["kitchen", "office"]);
        // The same message both ways is listed once.
        assert_eq!(climate.messages.len(), 1);
        let message = &climate.messages[0];
        assert_eq!(message.name, "Climate");
        assert_eq!(
            message.examples,
            [json!({ "temperature": 21.5, "humidity": 40 })]
        );
        let payload = message.payload.as_ref().unwrap();
        assert_eq!(
            payload["properties"]["temperature"],
            json!({ "type": "number" })
        );
        // A schema that refers to itself stops instead of going on forever.
        assert_eq!(payload["properties"]["parent"], json!({}));

        let light = &spec.channels[1];
        let names: Vec<&str> = light.messages.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, ["On", "Level"]);
        assert_eq!(
            light.messages[1].payload.as_ref().unwrap()["properties"]["level"],
            json!({ "type": "integer" })
        );
        assert_eq!(
            spec.channels[2].messages[0].payload, None,
            "Avro isn't checked"
        );
    }

    #[test]
    fn reads_asyncapi_3() {
        let d = dir("v3");
        std::fs::write(
            d.join("spec.json"),
            json!({
                "asyncapi": "3.0.0",
                "info": { "title": "Plant", "version": "0.1.0" },
                "channels": {
                    "meter": {
                        "address": "plant/{line}/meter",
                        "parameters": { "line": { "enum": ["a", "b"], "examples": ["c"] } },
                        "messages": { "reading": { "$ref": "#/components/messages/Reading" } }
                    },
                    "dynamic": { "address": null },
                    "status": { "messages": { "up": { "payload": { "type": "boolean" } } } }
                },
                "components": { "messages": { "Reading": {
                    "payload": {
                        "schemaFormat": "application/vnd.aai.asyncapi+json;version=3.0.0",
                        "schema": { "type": "object", "properties": { "kwh": { "type": "number" } } }
                    },
                    "examples": [{ "payload": { "kwh": 1.5 } }]
                } } }
            })
            .to_string(),
        )
        .unwrap();
        let spec = load(&d.join("spec.json")).unwrap();
        let addresses: Vec<&str> = spec.channels.iter().map(|c| c.address.as_str()).collect();
        assert_eq!(addresses, ["plant/{line}/meter", "status"]);
        let meter = &spec.channels[0];
        assert_eq!(meter.parameters[0].values, ["a", "b", "c"]);
        assert_eq!(meter.messages[0].name, "reading");
        assert_eq!(
            meter.messages[0].payload.as_ref().unwrap()["properties"]["kwh"],
            json!({ "type": "number" })
        );
        assert_eq!(meter.messages[0].examples, [json!({ "kwh": 1.5 })]);
        assert_eq!(
            spec.channels[1].messages[0].payload,
            Some(json!({ "type": "boolean" }))
        );
    }

    #[test]
    fn says_why_a_spec_cant_be_read() {
        let d = dir("bad");
        std::fs::write(d.join("openapi.yaml"), "openapi: 3.1.0\n").unwrap();
        std::fs::write(d.join("broken.yaml"), "asyncapi: [\n").unwrap();
        std::fs::write(d.join("old.yaml"), "asyncapi: 1.2.0\n").unwrap();
        assert!(
            load(&d.join("openapi.yaml"))
                .unwrap_err()
                .contains("isn't an AsyncAPI")
        );
        assert!(
            load(&d.join("broken.yaml"))
                .unwrap_err()
                .contains("broken.yaml")
        );
        assert!(
            load(&d.join("old.yaml"))
                .unwrap_err()
                .contains("isn't supported")
        );
        assert!(
            load(&d.join("gone.yaml"))
                .unwrap_err()
                .contains("Couldn't read")
        );
    }

    #[test]
    fn a_spec_is_read_again_when_it_changes() {
        let d = dir("cache");
        let path = d.join("s.yaml");
        std::fs::write(
            &path,
            "asyncapi: 2.6.0\ninfo: { title: One, version: '1' }\n",
        )
        .unwrap();
        let path = path.to_str().unwrap();
        let mut cache = SpecCache::default();
        assert_eq!(cache.get(path).spec.unwrap().title, "One");
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(
            path,
            "asyncapi: 2.6.0\ninfo: { title: Two, version: '1' }\n",
        )
        .unwrap();
        assert_eq!(cache.get(path).spec.unwrap().title, "Two");
        std::fs::remove_file(path).unwrap();
        assert!(cache.get(path).problem.is_some());
    }

    #[test]
    fn a_payload_is_checked_against_the_message_it_fits_best() {
        let on = json!({ "type": "object", "properties": { "on": { "type": "boolean" } }, "required": ["on"] });
        let level = json!({
            "type": "object",
            "properties": { "level": { "type": "integer", "maximum": 100 } },
            "required": ["level"]
        });
        let schemas = [on, level];
        assert_eq!(check_payload(&schemas, r#"{"on": true}"#), []);
        assert_eq!(check_payload(&schemas, r#"{"level": 5}"#), []);
        let problems = check_payload(&schemas, r#"{"level": 500}"#);
        assert_eq!(problems.len(), 1);
        assert_eq!(problems[0].path, ["level"]);
        assert!(
            problems[0].message.contains("100"),
            "{}",
            problems[0].message
        );
        let nested = json!({ "type": "array", "items": { "type": "number" } });
        assert_eq!(check_payload(&[nested], r#"[1, "x"]"#)[0].path, ["1"]);
        // Not JSON: the editor says so itself.
        assert_eq!(check_payload(&schemas, "{oops"), []);
    }
}
