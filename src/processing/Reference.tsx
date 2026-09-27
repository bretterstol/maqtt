import { useState } from "react";
import { FUNCTIONS, STAGES, SYNTAX, VARIABLES } from "./reference";

function matches(query: string, ...texts: (string | undefined)[]): boolean {
  const q = query.trim().toLowerCase();
  return q === "" || texts.some((t) => t?.toLowerCase().includes(q));
}

/** The language at a glance: every Stage, value and jq function, searchable. */
export function Reference({ onClose }: { onClose: () => void }) {
  const [query, setQuery] = useState("");

  const stages = STAGES.filter((s) =>
    matches(
      query,
      s.name,
      s.summary,
      s.details,
      ...s.forms.flatMap((f) => [f.syntax, f.does]),
    ),
  );
  const variables = VARIABLES.filter((v) => matches(query, v.name, v.does));
  const syntax = SYNTAX.filter((v) => matches(query, v.name, v.does));
  const functions = FUNCTIONS.filter((f) =>
    matches(query, f.name, f.does, f.example),
  );
  const nothing =
    stages.length + variables.length + syntax.length + functions.length === 0;

  return (
    <aside className="reference" aria-label="Reference">
      <header className="reference__header">
        <h2>Reference</h2>
        <button className="icon-button" onClick={onClose} title="Close">
          ×
        </button>
      </header>
      <input
        className="reference__search"
        type="search"
        placeholder="Search, like “round” or “topic”"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        aria-label="Search the reference"
      />
      <div className="reference__body">
        {query === "" && (
          <p className="reference__intro">
            A Pipeline is a <code>from</code> line and Stages joined with{" "}
            <code>|&gt;</code>. Each message flows down through them.
            Expressions are jq. Press <kbd>Ctrl</kbd>+<kbd>Space</kbd> in the
            editor for suggestions.
          </p>
        )}
        {stages.length > 0 && (
          <section>
            <h3>Stages</h3>
            {stages.map((stage) => (
              <article key={stage.name} className="reference__entry">
                <h4>
                  <code>{stage.name}</code>
                </h4>
                <p>{stage.summary}</p>
                <dl>
                  {stage.forms.map((form) => (
                    <div key={form.syntax}>
                      <dt>
                        <code>{form.syntax}</code>
                      </dt>
                      <dd>{form.does}</dd>
                    </div>
                  ))}
                </dl>
                {stage.details && (
                  <p className="reference__details">{stage.details}</p>
                )}
                <pre>
                  {stage.examples
                    .map((e) => (stage.name === "from" ? e : `|> ${e}`))
                    .join("\n")}
                </pre>
              </article>
            ))}
          </section>
        )}
        {variables.length > 0 && (
          <section>
            <h3>About the message</h3>
            <dl className="reference__names">
              {variables.map((v) => (
                <div key={v.name}>
                  <dt>
                    <code>{v.name}</code>
                  </dt>
                  <dd>{v.does}</dd>
                </div>
              ))}
            </dl>
          </section>
        )}
        {syntax.length > 0 && (
          <section>
            <h3>Writing expressions</h3>
            <dl className="reference__names">
              {syntax.map((v) => (
                <div key={v.name}>
                  <dt>
                    <code>{v.name}</code>
                  </dt>
                  <dd>{v.does}</dd>
                </div>
              ))}
            </dl>
          </section>
        )}
        {functions.length > 0 && (
          <section>
            <h3>jq functions</h3>
            <dl className="reference__names">
              {functions.map((f) => (
                <div key={f.name}>
                  <dt>
                    <code>{f.name}</code>
                  </dt>
                  <dd>
                    {f.does}
                    <code className="reference__example">
                      {f.input} | {f.example} → {f.output}
                    </code>
                  </dd>
                </div>
              ))}
            </dl>
            <p className="reference__details">
              These are the everyday ones; jq has many more.
            </p>
          </section>
        )}
        {nothing && (
          <p className="reference__intro">Nothing matches “{query}”.</p>
        )}
      </div>
    </aside>
  );
}
