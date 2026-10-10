import {
  lazy,
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  clearRetained,
  clearTopicMessages,
  listProfiles,
  messageDetail,
  newProfile,
  specChecks,
  streamMessages,
  topicSnapshot,
  type ConnectionId,
  type ConnectionStatus,
  type Profile,
  type ProfileView,
} from "./bridge";
import { MessageLogController } from "./messageLog/controller";
import { MessageLog } from "./messageLog/MessageLog";
import { MessageLogStore } from "./messageLog/store";
import { ProfileEditor } from "./profiles/ProfileEditor";
import { ProfileList } from "./profiles/ProfileList";
import { emptyDraft, type PublishDraft } from "./publish/draft";
import { payloadAsText } from "./topicDetail/format";
import { copy } from "./ui/clipboard";
import { ContextMenu } from "./ui/ContextMenu";
import { Toasts } from "./ui/Toasts";
import { toast } from "./ui/toast";
import { TopicTreeStore, type TreeNode } from "./topicTree/store";
import { takeOldExpandMode } from "./topicTree/expandPreference";
import { TopicTree } from "./topicTree/TopicTree";
import { cameBack, type Watching } from "./topicTree/watching";
import { TopicDetail } from "./topicDetail/TopicDetail";
import { PIPELINES_CONNECTION } from "./processing/api";
import { usePipelineBranchLabel } from "./processing/treeBranches";
import {
  getSettings,
  setSettings as saveSettings,
  type Settings,
  type SettingsView,
} from "./settings/api";
import { SettingsDialog } from "./settings/SettingsView";
import { DEFAULT_SETTINGS, SettingsContext } from "./settings/context";
import "./App.css";

// Pipelines live in their own folder and load only when their tab opens (ADR 0004).
const PipelinesTab = lazy(() => import("./processing/PipelinesTab"));
// With its code editors, loaded when first opened.
const PublishPanel = lazy(() =>
  import("./publish/PublishPanel").then((m) => ({ default: m.PublishPanel })),
);

interface Editing {
  profile: Profile;
  hasPassword: boolean;
  isNew: boolean;
}

function App() {
  const [controller] = useState(
    () =>
      new MessageLogController(
        new MessageLogStore(
          DEFAULT_SETTINGS.messageLog.live,
          DEFAULT_SETTINGS.messageLog.scrollback,
        ),
      ),
  );
  const { store } = controller;
  const [tree] = useState(() => new TopicTreeStore());
  const [tab, setTab] = useState<"topics" | "log" | "pipelines">("topics");
  const [selected, setSelected] = useState<TreeNode | null>(null);
  // A topic the user cleared while looking at it: the panel stays on it, and picks it up again
  // once the clear has come through and the topic is back.
  const [watching, setWatching] = useState<Watching | null>(null);
  // For the message stream's handler, which outlives renders.
  const watchingRef = useRef<Watching | null>(null);
  useEffect(() => {
    watchingRef.current = watching;
  }, [watching]);
  const select = useCallback((node: TreeNode | null) => {
    setWatching(null);
    setSelected(node);
  }, []);
  const [statuses, setStatuses] = useState<Map<ConnectionId, ConnectionStatus>>(
    new Map(),
  );
  const [profiles, setProfiles] = useState<ProfileView[]>([]);
  const [editing, setEditing] = useState<Editing | null>(null);
  const [rate, setRate] = useState(0);
  const [publishing, setPublishing] = useState(false);
  const [settings, setSettings] = useState<SettingsView | null>(null);
  const [showingSettings, setShowingSettings] = useState(false);
  // Until the settings arrive, Pipelines show as they always have.
  const pipelinesOn = settings?.settings.pipelines.enabled ?? true;
  const logShown = settings?.settings.messageLog.shown ?? true;
  // A tab turned off in Settings shows Topics instead.
  const shownTab =
    (tab === "pipelines" && !pipelinesOn) || (tab === "log" && !logShown)
      ? "topics"
      : tab;

  useEffect(() => {
    void getSettings().then(
      async (view) => {
        // An older mÅqtt kept how the tree opens in browser storage: hand it over, once.
        const old = takeOldExpandMode();
        if (old && old !== view.settings.topicTree.opens) {
          view = await saveSettings({
            ...view.settings,
            topicTree: { opens: old },
          }).catch(() => view);
        }
        setSettings(view);
      },
      () => {},
    );
  }, []);

  /** Saves settings changed outside the Settings view, such as the tree's own menu. */
  const changeSettings = async (next: Settings) => {
    try {
      setSettings(await saveSettings(next));
    } catch (error) {
      toast(`Couldn't save the settings: ${String(error)}`, true);
    }
  };
  const inForce = settings?.settings ?? DEFAULT_SETTINGS;
  useEffect(() => {
    tree.setExpandMode(inForce.topicTree.opens);
  }, [tree, inForce.topicTree.opens]);
  useEffect(() => {
    const theme = inForce.appearance.theme;
    // Every colour follows `color-scheme` through light-dark(). The window's frame is left to
    // the system: on Linux, setting the window's theme back to "system" turns the dark
    // preference off instead of following the desktop.
    document.documentElement.style.colorScheme =
      theme === "system" ? "" : theme;
  }, [inForce.appearance.theme]);
  useEffect(() => {
    store.setLimits(inForce.messageLog.live, inForce.messageLog.scrollback);
  }, [store, inForce.messageLog.live, inForce.messageLog.scrollback]);
  const [draft, setDraft] = useState<PublishDraft>(() => emptyDraft());
  /** The topic whose "Clear retained" is waiting for confirmation. */
  const [clearing, setClearing] = useState<string | null>(null);
  const [menu, setMenu] = useState<{
    node: TreeNode;
    x: number;
    y: number;
  } | null>(null);

  useEffect(() => {
    listProfiles()
      .then(setProfiles)
      .catch((err) => console.error("list_profiles failed", err));
  }, []);

  useEffect(() => {
    streamMessages((batch) => {
      store.applyBatch(batch);
      for (const { connectionId, prefix } of batch.cleared) {
        tree.clear(connectionId, prefix);
      }
      if (batch.cleared.length > 0) {
        // The selected topic may be among those cleared.
        setSelected((node) => (node && !tree.contains(node) ? null : node));
      }
      tree.apply(batch.topics);
      tree.applyChecks(batch.checks);
      const watched = watchingRef.current;
      const back = watched && cameBack(watched, (c, p) => tree.find(c, p));
      if (back) {
        watchingRef.current = null;
        setWatching(null);
        setSelected(back);
      }
      if (batch.connectionStates.length > 0) {
        setStatuses((previous) => {
          const next = new Map(previous);
          for (const status of batch.connectionStates)
            next.set(status.connectionId, status);
          return next;
        });
      }
    })
      // Backfill from the History Store, e.g. after a reload of the frontend.
      .then(async (generation) => {
        tree.apply(await topicSnapshot(), { live: false });
        tree.applyChecks(await specChecks());
        await controller.resetForStream(generation);
      })
      .catch((err) => console.error("stream_messages failed", err));
  }, [controller, store, tree]);

  useEffect(() => {
    let last = store.totalReceived;
    const timer = setInterval(() => {
      setRate(store.totalReceived - last);
      last = store.totalReceived;
    }, 1000);
    return () => clearInterval(timer);
  }, [store]);

  const statusesByProfile = useMemo(
    () => new Map([...statuses.values()].map((s) => [s.profileId, s])),
    [statuses],
  );
  const connectionStates = useMemo(
    () => new Map([...statuses].map(([id, status]) => [id, status.state])),
    [statuses],
  );
  const connectionNames = useMemo(
    () => new Map([...statuses].map(([id, status]) => [id, status.name])),
    [statuses],
  );

  const pipelineBranchLabel = usePipelineBranchLabel(
    pipelinesOn && statuses.has(PIPELINES_CONNECTION),
  );

  // Whether a topic's Connection can publish: connected, and a real Broker rather than the
  // Pipelines pseudo-Connection that Derived Topics come from.
  const isOnline = (node: TreeNode) =>
    node.connectionId !== PIPELINES_CONNECTION &&
    statuses.get(node.connectionId)?.state.kind === "connected";

  const publishTo = (node: TreeNode) => {
    const profileId = statuses.get(node.connectionId)?.profileId ?? null;
    setDraft((d) => ({ ...d, profileId, topic: node.path }));
    setPublishing(true);
  };

  const clearRetainedOn = async (node: TreeNode) => {
    setClearing(null);
    const status = statuses.get(node.connectionId);
    if (!status) return;
    try {
      await clearRetained(status.profileId, node.path);
    } catch (err) {
      console.error("clear_retained failed", err);
    }
  };

  const clearMessagesOn = async (node: TreeNode) => {
    if (selected?.key === node.key) {
      setWatching({
        connectionId: node.connectionId,
        path: node.path,
        was: node,
      });
    }
    try {
      await clearTopicMessages(node.connectionId, node.path);
    } catch (err) {
      setWatching(null);
      toast(`Couldn't clear ${node.path}: ${String(err)}`, true);
    }
  };

  const copyLatestPayload = async (node: TreeNode) => {
    if (!node.topic) return;
    const detail = await messageDetail(
      node.connectionId,
      node.topic.latest.seq,
    );
    if (detail) await copy(payloadAsText(detail.payload), "payload");
  };

  const menuItems = menu
    ? [
        ...(menu.node.depth > 0
          ? [
              {
                label: "Copy topic",
                onSelect: () => void copy(menu.node.path, "topic"),
              },
            ]
          : []),
        ...(menu.node.topic
          ? [
              {
                label: "Copy latest payload",
                onSelect: () => void copyLatestPayload(menu.node),
              },
            ]
          : []),
        ...(menu.node.depth > 0 && isOnline(menu.node)
          ? [
              {
                label: "Publish to this topic…",
                onSelect: () => publishTo(menu.node),
              },
            ]
          : []),
        ...(menu.node.depth > 0
          ? [
              {
                label:
                  menu.node.children.size > 0
                    ? "Clear messages here and below"
                    : "Clear messages",
                onSelect: () => void clearMessagesOn(menu.node),
              },
            ]
          : []),
        ...(menu.node.topic?.retained && isOnline(menu.node)
          ? [
              {
                label: "Clear retained message…",
                danger: true,
                // Confirmed in the detail panel, which the right-click already opened.
                onSelect: () => setClearing(menu.node.key),
              },
            ]
          : []),
      ]
    : [];

  const startNew = useCallback(async () => {
    setEditing({
      profile: await newProfile(),
      hasPassword: false,
      isNew: true,
    });
  }, []);

  return (
    <SettingsContext.Provider value={inForce}>
      <div className="app">
        <header className="app__header">
          <strong>
            m<span className="app__brand-ring">Å</span>qtt
          </strong>
          <button
            className={publishing ? "is-active" : undefined}
            onClick={() => setPublishing((open) => !open)}
          >
            Publish
          </button>
          <span className="app__stats">
            {rate.toLocaleString()} msg/s ·{" "}
            {store.totalReceived.toLocaleString()} received
            {store.dropped > 0 &&
              ` · ${store.dropped.toLocaleString()} not shown live`}
          </span>
          <button
            className={showingSettings ? "is-active" : undefined}
            disabled={!settings}
            onClick={() => setShowingSettings(true)}
          >
            Settings
          </button>
        </header>
        <div className="app__body">
          <ProfileList
            profiles={profiles}
            statuses={statusesByProfile}
            onNew={() => void startNew()}
            onEdit={({ hasPassword, ...profile }) =>
              setEditing({ profile, hasPassword, isNew: false })
            }
            onImported={setProfiles}
          />
          <main className="app__main">
            <nav className="tabs">
              <button
                className={
                  shownTab === "topics" ? "tabs__tab--active" : undefined
                }
                onClick={() => setTab("topics")}
              >
                Topics
              </button>
              {logShown && (
                <button
                  className={
                    shownTab === "log" ? "tabs__tab--active" : undefined
                  }
                  onClick={() => setTab("log")}
                >
                  Message Log
                </button>
              )}
              {pipelinesOn && (
                <button
                  className={
                    shownTab === "pipelines" ? "tabs__tab--active" : undefined
                  }
                  onClick={() => setTab("pipelines")}
                >
                  Pipelines
                </button>
              )}
            </nav>
            {shownTab === "topics" ? (
              <div className="app__split">
                <TopicTree
                  store={tree}
                  connectionNames={connectionNames}
                  connectionStates={connectionStates}
                  selectedKey={selected?.key ?? null}
                  onSelect={select}
                  onContextMenu={(node, x, y) => setMenu({ node, x, y })}
                  branchLabel={pipelineBranchLabel}
                  hidden={pipelinesOn ? undefined : PIPELINES_CONNECTION}
                  onExpandMode={(opens) =>
                    void changeSettings({ ...inForce, topicTree: { opens } })
                  }
                />
                {!selected && watching && (
                  <aside className="topic-detail">
                    <header className="topic-detail__header">
                      <code className="topic-detail__topic">
                        {watching.path}
                      </code>
                    </header>
                    <p className="topic-detail__hint">
                      Cleared. Its messages show here again as they arrive.
                    </p>
                  </aside>
                )}
                {selected && (
                  <TopicDetail
                    // A fresh panel per topic, so its history, pin and rate start over.
                    key={selected.key}
                    node={selected}
                    tree={tree}
                    connectionName={connectionNames.get(selected.connectionId)}
                    online={isOnline(selected)}
                    onPublish={() => publishTo(selected)}
                    confirmingClear={clearing === selected.key}
                    onClearRetained={() => setClearing(selected.key)}
                    onConfirmClear={() => void clearRetainedOn(selected)}
                    onCancelClear={() => setClearing(null)}
                    onClearMessages={() => void clearMessagesOn(selected)}
                  />
                )}
              </div>
            ) : shownTab === "log" ? (
              <MessageLog
                controller={controller}
                connectionNames={connectionNames}
              />
            ) : (
              <Suspense fallback={null}>
                <PipelinesTab />
              </Suspense>
            )}
            {publishing && (
              <Suspense fallback={null}>
                <PublishPanel
                  draft={draft}
                  onDraftChange={setDraft}
                  profiles={profiles}
                  statuses={statusesByProfile}
                  seenTopics={(profileId) => {
                    const status = statusesByProfile.get(profileId);
                    return status ? tree.topics(status.connectionId) : [];
                  }}
                  onClose={() => setPublishing(false)}
                />
              </Suspense>
            )}
          </main>
        </div>
        {menu && menuItems.length > 0 && (
          <ContextMenu
            x={menu.x}
            y={menu.y}
            onClose={() => setMenu(null)}
            items={menuItems}
          />
        )}
        <Toasts />
        {showingSettings && settings && (
          <SettingsDialog
            view={settings}
            onChanged={(view) => {
              setSettings(view);
              // Leave a tab that was just turned off.
              const hidden =
                (tab === "pipelines" && !view.settings.pipelines.enabled) ||
                (tab === "log" && !view.settings.messageLog.shown);
              if (hidden) setTab("topics");
            }}
            onClose={() => setShowingSettings(false)}
          />
        )}
        {editing && (
          <ProfileEditor
            key={editing.profile.id}
            profile={editing.profile}
            hasPassword={editing.hasPassword}
            isNew={editing.isNew}
            onSaved={setProfiles}
            onClose={() => setEditing(null)}
          />
        )}
      </div>
    </SettingsContext.Provider>
  );
}

export default App;
