import { describe, expect, it } from "vitest";
import {
  describe as describeSchema,
  propertyOptions,
  schemasAt,
  skeleton,
  valueOptions,
  type Schema,
} from "./schema";

const climate: Schema = {
  type: "object",
  required: ["temperature"],
  properties: {
    temperature: { type: "number", minimum: -40, maximum: 60 },
    unit: { enum: ["C", "F"], default: "C" },
    sensors: {
      type: "array",
      items: {
        type: "object",
        properties: { id: { type: "string" }, ok: { type: "boolean" } },
      },
    },
  },
};

describe("schemas", () => {
  it("finds the schema at a path, through arrays and allOf", () => {
    expect(schemasAt([climate], ["sensors", 0, "ok"])[0].type).toBe("boolean");
    const merged = {
      allOf: [climate, { properties: { extra: { type: "null" } } }],
    };
    expect(schemasAt([merged], ["extra"])[0].type).toBe("null");
    expect(schemasAt([climate], ["nothing"])).toEqual([]);
  });

  it("offers properties, required first, and values", () => {
    expect(
      propertyOptions(schemasAt([climate], [])).map((p) => p.name),
    ).toEqual(["temperature", "unit", "sensors"]);
    expect(valueOptions(schemasAt([climate], ["unit"]))).toEqual(["C", "F"]);
    expect(valueOptions(schemasAt([climate], ["sensors", 0, "ok"]))).toEqual([
      true,
      false,
    ]);
  });

  it("says briefly what a schema takes", () => {
    expect(describeSchema(climate.properties!.temperature)).toBe(
      "number -40–60",
    );
    expect(describeSchema(climate.properties!.unit)).toBe('one of "C", "F"');
  });

  it("builds a payload to start from", () => {
    expect(skeleton(climate)).toEqual({ temperature: -40 });
    expect(
      skeleton({ type: "object", properties: { a: { type: "string" } } }),
    ).toEqual({ a: "" });
    expect(skeleton({ type: "object", examples: [{ a: 1 }] })).toEqual({
      a: 1,
    });
    expect(skeleton({ oneOf: [{ type: "boolean" }, { type: "string" }] })).toBe(
      false,
    );
  });
});
