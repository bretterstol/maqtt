import { useState } from "react";
import { toast } from "../ui/toast";
import { setSettings, type Settings, type SettingsView } from "./api";
import "../profiles/ProfileEditor.css";
import "./SettingsView.css";

/** How måqtt behaves. Changes apply at once and are saved to settings.toml. */
export function SettingsDialog({
  view,
  onChanged,
  onClose,
}: {
  view: SettingsView;
  onChanged: (view: SettingsView) => void;
  onClose: () => void;
}) {
  const [saving, setSaving] = useState(false);
  const { settings } = view;

  /** Saves and applies; says whether it worked. */
  const change = async (next: Settings): Promise<boolean> => {
    setSaving(true);
    try {
      onChanged(await setSettings(next));
      return true;
    } catch (error) {
      toast(`Couldn't save the settings: ${String(error)}`, true);
      return false;
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="editor-backdrop" onMouseDown={onClose}>
      <div
        className="editor settings"
        role="dialog"
        aria-label="Settings"
        onMouseDown={(e) => e.stopPropagation()}
        onKeyDown={(e) => e.key === "Escape" && onClose()}
      >
        <h2>Settings</h2>
        {view.problem && <p className="editor__warning">{view.problem}</p>}

        <fieldset className="editor__section">
          <legend>Pipelines</legend>
          <label className="editor__check">
            <input
              type="checkbox"
              checked={settings.pipelines.enabled}
              disabled={saving}
              onChange={(e) =>
                void change({
                  ...settings,
                  pipelines: { enabled: e.target.checked },
                })
              }
            />
            Use Pipelines
          </label>
          <p className="settings__does">
            Shows the Pipelines tab and their branch in the Topic Tree. Turned
            off, running Pipelines stop and none start, not even those marked to
            start with måqtt. Turned on again, those start.
          </p>
        </fieldset>

        <fieldset className="editor__section">
          <legend>History</legend>
          <NumberSetting
            label="Messages kept per Connection"
            value={settings.history.perConnection}
            disabled={saving}
            onCommit={(perConnection) =>
              change({ ...settings, history: { perConnection } })
            }
          />
          <p className="settings__does">
            The oldest go first, and lowering it trims at once. How many each
            topic keeps is set on its Broker Profile.
          </p>
        </fieldset>

        <fieldset className="editor__section">
          <legend>Message Log</legend>
          <div className="editor__row">
            <NumberSetting
              label="Kept while live"
              value={settings.messageLog.live}
              disabled={saving}
              onCommit={(live) =>
                change({
                  ...settings,
                  messageLog: { ...settings.messageLog, live },
                })
              }
            />
            <NumberSetting
              label="Scrollback"
              value={settings.messageLog.scrollback}
              disabled={saving}
              onCommit={(scrollback) =>
                change({
                  ...settings,
                  messageLog: { ...settings.messageLog, scrollback },
                })
              }
            />
          </div>
          <p className="settings__does">
            How many messages the Message Log holds while following live
            traffic, and how far back it can be scrolled.
          </p>
        </fieldset>

        <fieldset className="editor__section">
          <legend>Subscriptions</legend>
          <label className="editor__field">
            <span>QoS for new Subscriptions</span>
            <select
              value={settings.subscriptions.defaultQos}
              disabled={saving}
              onChange={(e) =>
                void change({
                  ...settings,
                  subscriptions: { defaultQos: Number(e.target.value) },
                })
              }
            >
              <option value={0}>0: at most once</option>
              <option value={1}>1: at least once</option>
              <option value={2}>2: exactly once</option>
            </select>
          </label>
          <p className="settings__does">
            What the Subscribe field and a Profile's new Subscriptions start
            with. Connecting at startup is set on each Broker Profile.
          </p>
        </fieldset>

        <p className="settings__where" title={view.path}>
          Saved in {view.path}
        </p>
        <div className="editor__actions">
          <button className="primary" autoFocus onClick={onClose}>
            Done
          </button>
        </div>
      </div>
    </div>
  );
}

/** A whole number saved when the field is left or Enter pressed; refused ones go back. */
function NumberSetting({
  label,
  value,
  disabled,
  onCommit,
}: {
  label: string;
  value: number;
  disabled: boolean;
  onCommit: (value: number) => Promise<boolean>;
}) {
  const [text, setText] = useState(String(value));
  const [shown, setShown] = useState(value);
  // A value saved elsewhere replaces what's typed.
  if (shown !== value) {
    setShown(value);
    setText(String(value));
  }
  const commit = async () => {
    const n = Number(text);
    if (text.trim() === "" || !Number.isInteger(n)) {
      setText(String(value));
      return;
    }
    if (n !== value && !(await onCommit(n))) setText(String(value));
  };
  return (
    <label className="editor__field">
      <span>{label}</span>
      <input
        type="number"
        min={1}
        step={1}
        value={text}
        disabled={disabled}
        onChange={(e) => setText(e.target.value)}
        onBlur={() => void commit()}
        onKeyDown={(e) => e.key === "Enter" && void commit()}
      />
    </label>
  );
}
