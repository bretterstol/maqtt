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

  const change = async (next: Settings) => {
    setSaving(true);
    try {
      onChanged(await setSettings(next));
    } catch (error) {
      toast(`Couldn't save the settings: ${String(error)}`, true);
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
