# Settings plan

A place to change how måqtt behaves, instead of environment variables, constants in the code and preferences scattered across browser storage. Vocabulary follows [CONTEXT.md](../CONTEXT.md).

Status: done. S1, S2 and S3 are built. The decisions are at the end.

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

| Setting                                                           | Default           | Notes                                                                                                                                                                                          |
| ----------------------------------------------------------------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Pipelines**: show the Pipelines tab and their Topic Tree branch | On                | The "Later" item from the Pipelines plan. Off hides both entry points (ADR 0004 keeps them one each), stops running Pipelines and starts none, marked or not. On again starts the marked ones. |
| **History per Connection**                                        | 100,000           | Lowering it trims at once. History per topic stays on each Broker Profile, where it already was.                                                                                               |
| **Message Log**: kept while live, and scrollback                  | 5,000 and 100,000 | Were constants in `App.tsx`; a smaller live count trims at once.                                                                                                                               |
| **QoS for new Subscriptions**                                     | 0                 | What the Subscribe field and a Profile's new Subscriptions start with.                                                                                                                         |
| **Theme**: system, light or dark                                  | System            | Today only the system decides.                                                                                                                                                                 |
| **Topic Tree opens**: by hand, groups, everything                 | By hand           | Moves the tree's preference out of browser storage, into the file.                                                                                                                             |

Values out of range are refused with what's allowed (history 1,000 to 10,000,000; live 100 to 100,000; scrollback from the live count to 1,000,000), and nothing is saved.

**Connect at startup** isn't a setting here: it's a tick on each Broker Profile, "Connect when måqtt starts", beside the Broker it's about (as Start with måqtt is on each Pipeline). `MAQTT_AUTOCONNECT` keeps working for its session.

## Where it's changed

A **Settings** view, opened from a gear button at the right of the title bar, next to the message rate. One column of sections (General, Pipelines, History), each setting a label, a control and a line saying what it does. A **Reset to defaults** per section. The file's location is shown at the bottom, as the Pipelines tab shows its folder.

## Milestones

### S1: The file and the view

`settings.toml` loaded at startup with today's defaults, a `get_settings` / `set_settings` pair of commands, and the Settings view with the **Pipelines** switch as its first setting.

**Done when:** turning Pipelines off hides the tab and the Topic Tree branch at once, survives a restart, and turning it back on restores both.

Status: done on the `settings-s1` branch. A **Settings** button at the right of the title bar opens the view; changes apply and save at once. Checked over WebDriver: turning Pipelines off hid the tab and the Pipelines root in the Topic Tree, wrote `enabled = false`, and after a restart the marked Pipeline didn't start; turning them on again brought both back and started it. A `settings.toml` that can't be read says so in the view, and the defaults are used.

### S2: Startup, history and defaults

"Connect when måqtt starts" on each Broker Profile, history per Connection, the Message Log's live count and scrollback, and the QoS for new Subscriptions. `MAQTT_AUTOCONNECT` keeps working for its session.

**Done when:** a Broker ticked to connect does so on the next start without the environment variable, and the history, scrollback and QoS settings take effect.

Status: done on the `settings-s2` branch. None of them needs a restart after all: a lower history limit trims every Connection at once, and the Message Log takes new limits as they're saved. A global history per topic was dropped, as each Profile already sets it. Checked over WebDriver: a Profile ticked to connect did so on the next start with no `MAQTT_AUTOCONNECT`; the Subscribe field then started at QoS 1; a scrollback below the live count was refused, and the field went back.

### S3: Appearance and preferences

Theme, and the Topic Tree's opening preference moved from browser storage into the file (read once from browser storage, so nobody's current choice is lost).

**Done when:** the theme follows the setting in both directions, and nothing måqtt remembers lives in browser storage any more except what's purely per window.

Status: done on the `settings-s3` branch. The theme sets the page's colour scheme, which every colour follows; the window's own frame is left to the system, as on Linux setting the window's theme back to "system" turns the dark preference off instead of following the desktop (found while checking: a fresh start came up light on a dark desktop). The Topic Tree's own menu and Settings change the same setting; a choice an older måqtt left in browser storage is moved into the file once. What stays in browser storage is the Pipelines Reference panel being open, which is per window. Checked over WebDriver across a restart: the old choice moved into the file and the tree opened with it, the light theme applied from the start, the tree's menu saved to the file, and system, light and dark each gave the right colours on a dark desktop.

## Decisions

1. **Connect at startup** is a tick on each Broker Profile, not a list in Settings.
2. **Hiding Pipelines stops them**: running ones stop and marked ones don't start, so nothing processes out of sight.
3. **Also in the first settings**: Message Log scrollback and the default QoS for new Subscriptions (S2).
