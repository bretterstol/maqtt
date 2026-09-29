# Settings plan

A place to change how måqtt behaves, instead of environment variables, constants in the code and preferences scattered across browser storage. Vocabulary follows [CONTEXT.md](../CONTEXT.md).

Status: **a draft to decide on together** before any of it is built. The open questions at the end come first.

## What is adjustable today

| What                                                         | Where it lives now                              | Who can change it                  |
| ------------------------------------------------------------ | ----------------------------------------------- | ---------------------------------- |
| Config folder                                                | `MAQTT_CONFIG_DIR` environment variable         | Only when starting from a terminal |
| Brokers to connect at startup                                | `MAQTT_AUTOCONNECT` environment variable        | Only when starting from a terminal |
| History kept: 100 messages per topic, 100,000 per Connection | `HistoryLimits::default()` in `history.rs`      | Nobody                             |
| Message Log scrollback: 5,000 live, 100,000 in all           | Constants in `App.tsx`                          | Nobody                             |
| History per topic, per Broker Profile                        | The Profile (`history_per_topic`)               | The Profile editor                 |
| How the Topic Tree opens                                     | Browser storage (`expandPreference.ts`)         | The tree's own control             |
| Whether the Pipelines Reference is open                      | Browser storage (`PipelinesTab.tsx`)            | The Reference button               |
| Pipelines that start with the app                            | `pipelines/settings.json`                       | The Pipeline's header              |
| Light or dark                                                | Follows the system (`color-scheme: light dark`) | The operating system               |

Browser storage is per machine and invisible: it isn't in the config folder, so it doesn't follow a copied or synced config, and it can't be edited or reset in one place.

## Principles

- **One file, beside the Profiles.** `settings.toml` in the config folder, next to `profiles.toml`: plain text, readable and editable by hand, and carried with the config folder (which also keeps the sandbox, `MAQTT_CONFIG_DIR`, separate).
- **Defaults are today's behaviour.** A missing file or a missing key means exactly what måqtt does now, so nobody's setup changes on upgrade.
- **Settings describe the app, not a Pipeline or a Profile.** What belongs to one Broker stays on its Profile; what belongs to one Pipeline stays with it (its start-with-app mark stays in `pipelines/settings.json`).
- **Changes apply at once** where they can; the few that need a restart (the history limits, which size the stores) say so beside the control.
- **Environment variables still win** for the session they're set in, so the sandbox and scripted runs keep working.

## First settings

| Setting                                                           | Default | Notes                                                                                                                                        |
| ----------------------------------------------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| **Pipelines**: show the Pipelines tab and their Topic Tree branch | On      | The "Later" item from the Pipelines plan. Off hides both entry points (ADR 0004 keeps them one each) and starts no Pipelines, marked or not. |
| **Connect at startup**: which Brokers                             | None    | Replaces `MAQTT_AUTOCONNECT` for everyday use; could instead be a per-Profile "connect at startup" tick, see the questions.                  |
| **History per topic**                                             | 100     | Needs a restart.                                                                                                                             |
| **History per Connection**                                        | 100,000 | Needs a restart.                                                                                                                             |
| **Theme**: system, light or dark                                  | System  | Today only the system decides.                                                                                                               |
| **Topic Tree opens**: by hand, groups, everything                 | By hand | Moves the tree's preference out of browser storage, into the file.                                                                           |

## Where it's changed

A **Settings** view, opened from a gear button at the right of the title bar, next to the message rate. One column of sections (General, Pipelines, History), each setting a label, a control and a line saying what it does. A **Reset to defaults** per section. The file's location is shown at the bottom, as the Pipelines tab shows its folder.

## Milestones

### S1: The file and the view

`settings.toml` loaded at startup with today's defaults, a `get_settings` / `set_setting` pair of commands, and the Settings view with the **Pipelines** switch as its first setting.

**Done when:** turning Pipelines off hides the tab and the Topic Tree branch at once, survives a restart, and turning it back on restores both.

### S2: Startup and history

Connect at startup and the two history limits, with the restart note. `MAQTT_AUTOCONNECT` keeps overriding for its session.

**Done when:** a Broker marked in Settings connects on the next start without the environment variable, and a changed history limit applies after a restart.

### S3: Appearance and preferences

Theme, and the Topic Tree's opening preference moved from browser storage into the file (read once from browser storage, so nobody's current choice is lost).

**Done when:** the theme follows the setting in both directions, and nothing måqtt remembers lives in browser storage any more except what's purely per window.

## Open questions

1. **Connect at startup: in Settings, or on each Profile?** A per-Profile tick ("connect when måqtt starts") keeps it beside the Broker it's about, like start-with-app is on each Pipeline. A list in Settings keeps all startup behaviour in one place.
2. **Should hiding Pipelines also stop running ones**, or only hide them while they carry on (e.g. an unattended recording)?
3. **Anything missing from the first settings?** For example the Message Log's scrollback, or a default QoS for new Subscriptions.
