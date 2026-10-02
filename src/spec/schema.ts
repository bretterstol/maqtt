// Reading a payload's JSON Schema (with `$ref`s already inlined by the backend) for
// suggestions: what fits at a point in the payload, and a payload to start from.

/** A JSON Schema, loosely: only what suggestions read. */
export interface Schema {
  type?: string | string[];
  properties?: Record<string, Schema>;
  additionalProperties?: Schema | boolean;
  required?: string[];
  items?: Schema | Schema[];
  prefixItems?: Schema[];
  enum?: unknown[];
  const?: unknown;
  default?: unknown;
  examples?: unknown[];
  example?: unknown;
  allOf?: Schema[];
  anyOf?: Schema[];
  oneOf?: Schema[];
  description?: string;
  title?: string;
  minimum?: number;
  maximum?: number;
  format?: string;
}

const isSchema = (value: unknown): value is Schema =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** A schema with what `allOf`, `anyOf` and `oneOf` hold beside it, flattened. */
export function expand(schema: unknown): Schema[] {
  if (!isSchema(schema)) return [];
  return [
    schema,
    ...(schema.allOf ?? []).flatMap(expand),
    ...(schema.anyOf ?? []).flatMap(expand),
    ...(schema.oneOf ?? []).flatMap(expand),
  ];
}

/** The schemas for what's at `path` (keys and indexes) in a payload of one of `schemas`. */
export function schemasAt(
  schemas: unknown[],
  path: (string | number)[],
): Schema[] {
  let current = schemas.flatMap(expand);
  for (const step of path) {
    current = current.flatMap((s) => {
      if (typeof step === "number") {
        const tuple =
          s.prefixItems ?? (Array.isArray(s.items) ? s.items : null);
        if (tuple) return expand(tuple[step] ?? {});
        return expand(s.items);
      }
      const property = s.properties?.[step];
      if (property !== undefined) return expand(property);
      return isSchema(s.additionalProperties)
        ? expand(s.additionalProperties)
        : [];
    });
  }
  return current;
}

const types = (s: Schema): string[] =>
  Array.isArray(s.type) ? s.type : s.type ? [s.type] : [];

export interface PropertyOption {
  name: string;
  required: boolean;
  schema: Schema;
}

/** The properties objects of these schemas have, required ones first. */
export function propertyOptions(schemas: Schema[]): PropertyOption[] {
  const options = new Map<string, PropertyOption>();
  for (const s of schemas) {
    for (const [name, schema] of Object.entries(s.properties ?? {})) {
      const required = s.required?.includes(name) ?? false;
      const known = options.get(name);
      if (!known) options.set(name, { name, required, schema });
      else known.required ||= required;
    }
  }
  return [...options.values()].sort(
    (a, b) => Number(b.required) - Number(a.required),
  );
}

/** Values these schemas name: enums, constants, true and false, null, defaults, examples. */
export function valueOptions(schemas: Schema[]): unknown[] {
  const values: unknown[] = [];
  const add = (v: unknown) => {
    const text = JSON.stringify(v);
    if (!values.some((known) => JSON.stringify(known) === text)) values.push(v);
  };
  for (const s of schemas) {
    s.enum?.forEach(add);
    if ("const" in s) add(s.const);
    if (types(s).includes("boolean")) [true, false].forEach(add);
    if (types(s).includes("null")) add(null);
    if ("default" in s) add(s.default);
    s.examples?.forEach(add);
    if ("example" in s) add(s.example);
  }
  return values;
}

/** A short line on what a schema takes: its type and limits, or its values. */
export function describe(schema: Schema): string {
  const all = expand(schema);
  const named = all.flatMap((s) => s.enum ?? ("const" in s ? [s.const] : []));
  if (named.length > 0) {
    return `one of ${named.map((v) => JSON.stringify(v)).join(", ")}`;
  }
  const kind = [...new Set(all.flatMap(types))].join(" or ") || "any";
  const min = all.find((s) => s.minimum !== undefined)?.minimum;
  const max = all.find((s) => s.maximum !== undefined)?.maximum;
  const range =
    min !== undefined && max !== undefined
      ? ` ${min}–${max}`
      : min !== undefined
        ? ` ≥ ${min}`
        : max !== undefined
          ? ` ≤ ${max}`
          : "";
  const format = all.find((s) => s.format)?.format;
  return `${kind}${range}${format ? ` (${format})` : ""}`;
}

/**
 * A payload to start from: the schema's own example or default where it has one, otherwise
 * its required properties (all of them when none are required) with values of their types.
 */
export function skeleton(schema: unknown, depth = 0): unknown {
  const [s, ...rest] = expand(schema);
  if (!s || depth > 8) return null;
  if (s.examples?.length) return s.examples[0];
  if ("example" in s) return s.example;
  if ("default" in s) return s.default;
  if ("const" in s) return s.const;
  if (s.enum?.length) return s.enum[0];
  const choice = s.oneOf?.[0] ?? s.anyOf?.[0];
  if (!s.type && !s.properties && choice) return skeleton(choice, depth + 1);
  const properties = Object.assign(
    {},
    ...[s, ...rest.filter((r) => s.allOf?.includes(r))].map(
      (x) => x.properties ?? {},
    ),
  ) as Record<string, Schema>;
  const type = types(s)[0] ?? (s.properties ? "object" : undefined);
  switch (type) {
    case "object": {
      const required = new Set(
        [s, ...(s.allOf ?? [])].flatMap((x) => x.required ?? []),
      );
      const names = Object.keys(properties).filter(
        (name) => required.size === 0 || required.has(name),
      );
      return Object.fromEntries(
        names.map((name) => [name, skeleton(properties[name], depth + 1)]),
      );
    }
    case "array":
      return [];
    case "string":
      return "";
    case "number":
    case "integer":
      return s.minimum ?? 0;
    case "boolean":
      return false;
    default:
      return null;
  }
}
