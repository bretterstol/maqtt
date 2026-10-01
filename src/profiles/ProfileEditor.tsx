import { ask } from "@tauri-apps/plugin-dialog";
import { useState, type ReactNode } from "react";
import {
  deleteProfile,
  saveProfile,
  type Profile,
  type ProfileView,
  type TlsSettings,
} from "../bridge";
import { chooseFile } from "../ui/dialogs";
import { cleanDraft, emptyTls, passwordChange } from "./draft";
import { useSettings } from "../settings/context";
import "./ProfileEditor.css";

interface Props {
  profile: Profile;
  /** Whether a password is stored; false for a new Profile. */
  hasPassword: boolean;
  isNew: boolean;
  onSaved: (profiles: ProfileView[]) => void;
  onClose: () => void;
}

export function ProfileEditor({
  profile,
  hasPassword,
  isNew,
  onSaved,
  onClose,
}: Props) {
  const [draft, setDraft] = useState<Profile>(profile);
  const defaultQos = useSettings().subscriptions.defaultQos;
  const [password, setPassword] = useState("");
  const [clearPassword, setClearPassword] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const set = (change: Partial<Profile>) =>
    setDraft((d) => ({ ...d, ...change }));
  const setTls = (change: Partial<TlsSettings>) =>
    setDraft((d) => ({ ...d, tls: { ...(d.tls ?? emptyTls), ...change } }));

  const save = async () => {
    setError(null);
    try {
      onSaved(
        await saveProfile(
          cleanDraft(draft),
          passwordChange(password, clearPassword),
        ),
      );
      onClose();
    } catch (err) {
      setError(String(err));
    }
  };

  const remove = async () => {
    const confirmed = await ask(
      `Delete ${profile.name}? Its stored password is deleted too.`,
      { title: "Delete Broker Profile", kind: "warning" },
    );
    if (!confirmed) return;
    try {
      onSaved(await deleteProfile(profile.id));
      onClose();
    } catch (err) {
      setError(String(err));
    }
  };

  const tls = draft.tls;

  return (
    <div className="editor-backdrop" onMouseDown={onClose}>
      <form
        className="editor"
        onMouseDown={(e) => e.stopPropagation()}
        onSubmit={(e) => {
          e.preventDefault();
          void save();
        }}
      >
        <h2>{isNew ? "New Broker Profile" : `Edit ${profile.name}`}</h2>

        <Section title="Broker">
          <Field label="Name">
            <input
              required
              value={draft.name}
              onChange={(e) => set({ name: e.target.value })}
            />
          </Field>
          <div className="editor__row">
            <Field label="Host" grow>
              <input
                required
                value={draft.host}
                onChange={(e) => set({ host: e.target.value })}
              />
            </Field>
            <Field label="Port">
              <input
                type="number"
                min={1}
                max={65535}
                required
                value={draft.port}
                onChange={(e) => set({ port: Number(e.target.value) })}
              />
            </Field>
            <Field label="Protocol">
              <select
                value={draft.protocol}
                onChange={(e) =>
                  set({ protocol: e.target.value as Profile["protocol"] })
                }
              >
                <option value="mqtt5">MQTT 5</option>
                <option value="mqtt311">MQTT 3.1.1</option>
              </select>
            </Field>
          </div>
          <Field label="Client ID">
            <input
              required
              value={draft.clientId}
              onChange={(e) => set({ clientId: e.target.value })}
            />
          </Field>
        </Section>

        <Section title="Credentials">
          <div className="editor__row">
            <Field label="Username" grow>
              <input
                value={draft.username ?? ""}
                onChange={(e) => set({ username: e.target.value })}
              />
            </Field>
            <Field label="Password" grow>
              <input
                type="password"
                autoComplete="new-password"
                disabled={clearPassword}
                placeholder={hasPassword ? "Stored in the keyring" : ""}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
            </Field>
          </div>
          {hasPassword && (
            <label className="editor__check">
              <input
                type="checkbox"
                checked={clearPassword}
                onChange={(e) => setClearPassword(e.target.checked)}
              />
              Remove the stored password
            </label>
          )}
        </Section>

        <Section title="TLS">
          <label className="editor__check">
            <input
              type="checkbox"
              checked={!!tls}
              onChange={(e) => set({ tls: e.target.checked ? emptyTls : null })}
            />
            Connect over TLS
          </label>
          {tls && (
            <>
              <FileField
                label="CA certificate"
                hint="Leave empty to trust the system's certificates"
                value={tls.caFile}
                onChange={(caFile) => setTls({ caFile })}
              />
              <FileField
                label="Client certificate"
                value={tls.clientCertFile}
                onChange={(clientCertFile) => setTls({ clientCertFile })}
              />
              <FileField
                label="Client key"
                value={tls.clientKeyFile}
                onChange={(clientKeyFile) => setTls({ clientKeyFile })}
              />
              <label className="editor__check">
                <input
                  type="checkbox"
                  checked={tls.insecureSkipVerify}
                  onChange={(e) =>
                    setTls({ insecureSkipVerify: e.target.checked })
                  }
                />
                Skip certificate verification
              </label>
              {tls.insecureSkipVerify && (
                <p className="editor__warning">
                  måqtt will accept any certificate this Broker presents, so
                  anyone between you and the Broker can read and change the
                  traffic, credentials included. Only use this on networks you
                  trust.
                </p>
              )}
            </>
          )}
        </Section>

        <Section title="Session">
          <label className="editor__check">
            <input
              type="checkbox"
              checked={draft.connectAtStartup}
              onChange={(e) => set({ connectAtStartup: e.target.checked })}
            />
            Connect when måqtt starts
          </label>
          <label className="editor__check">
            <input
              type="checkbox"
              checked={!draft.cleanStart}
              onChange={(e) => set({ cleanStart: !e.target.checked })}
            />
            Persistent session (the Broker keeps queued messages while
            disconnected)
          </label>
          <div className="editor__row">
            <Field label="Keep alive (s)">
              <input
                type="number"
                min={5}
                value={draft.keepAliveSecs}
                onChange={(e) => set({ keepAliveSecs: Number(e.target.value) })}
              />
            </Field>
            <Field label="History per topic">
              <input
                type="number"
                min={1}
                value={draft.historyPerTopic}
                onChange={(e) =>
                  set({ historyPerTopic: Number(e.target.value) })
                }
              />
            </Field>
          </div>
        </Section>

        <Section title="Subscriptions on connect">
          {draft.subscriptions.map((sub, i) => (
            <div className="editor__row" key={i}>
              <input
                className="editor__grow"
                placeholder="Topic filter, e.g. home/#"
                value={sub.filter}
                onChange={(e) =>
                  set({
                    subscriptions: draft.subscriptions.map((s, j) =>
                      j === i ? { ...s, filter: e.target.value } : s,
                    ),
                  })
                }
              />
              <select
                value={sub.qos}
                onChange={(e) =>
                  set({
                    subscriptions: draft.subscriptions.map((s, j) =>
                      j === i ? { ...s, qos: Number(e.target.value) } : s,
                    ),
                  })
                }
              >
                <option value={0}>QoS 0</option>
                <option value={1}>QoS 1</option>
                <option value={2}>QoS 2</option>
              </select>
              <button
                type="button"
                onClick={() =>
                  set({
                    subscriptions: draft.subscriptions.filter(
                      (_, j) => j !== i,
                    ),
                  })
                }
              >
                Remove
              </button>
            </div>
          ))}
          <button
            type="button"
            onClick={() =>
              set({
                subscriptions: [
                  ...draft.subscriptions,
                  { filter: "", qos: defaultQos },
                ],
              })
            }
          >
            Add Subscription
          </button>
        </Section>

        {error && <p className="editor__error">{error}</p>}

        <div className="editor__actions">
          {!isNew && (
            <button
              type="button"
              className="editor__delete"
              onClick={() => void remove()}
            >
              Delete
            </button>
          )}
          <span className="editor__grow" />
          <button type="button" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="primary">
            Save
          </button>
        </div>
      </form>
    </div>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <fieldset className="editor__section">
      <legend>{title}</legend>
      {children}
    </fieldset>
  );
}

function Field({
  label,
  grow,
  children,
}: {
  label: string;
  grow?: boolean;
  children: ReactNode;
}) {
  return (
    <label className={`editor__field${grow ? " editor__grow" : ""}`}>
      <span>{label}</span>
      {children}
    </label>
  );
}

function FileField({
  label,
  hint,
  value,
  onChange,
}: {
  label: string;
  hint?: string;
  value: string | null | undefined;
  onChange: (path: string | null) => void;
}) {
  const browse = async () => {
    const picked = await chooseFile({ title: label });
    if (picked) onChange(picked);
  };
  return (
    <Field label={label}>
      <div className="editor__row">
        <input
          className="editor__grow"
          placeholder={hint}
          value={value ?? ""}
          onChange={(e) => onChange(e.target.value || null)}
        />
        <button type="button" onClick={() => void browse()}>
          Browse…
        </button>
      </div>
    </Field>
  );
}
