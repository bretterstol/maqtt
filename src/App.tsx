import {
  lazy,
  Suspense,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";
import {
  clearRetained,
  listProfiles,
  messageDetail,
  newProfile,
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
import { PublishPanel } from "./publish/PublishPanel";
import { payloadAsText } from "./topicDetail/format";
import { copy } from "./ui/clipboard";
import { ContextMenu } from "./ui/ContextMenu";
import { Toasts } from "./ui/Toasts";
import { TopicTreeStore, type TreeNode } from "./topicTree/store";
import { savedExpandMode } from "./topicTree/expandPreference";
import { TopicTree } from "./topicTree/TopicTree";
import { TopicDetail } from "./topicDetail/TopicDetail";
import { PIPELINES_CONNECTION } from "./processing/api";
import { usePipelineBranchLabel } from "./processing/treeBranches";
import "./App.css";

// Pipelines live in their own folder and load only when their tab opens (ADR 0004).
const PipelinesTab = lazy(() => import("./processing/PipelinesTab"));

const LIVE_CAPACITY = 5_000;
const MAX_SCROLLBACK = 100_000;

interface Editing {
  profile: Profile;
  hasPassword: boolean;
  isNew: boolean;
}

function App() {
  const [controller] = useState(
    () =>
      new MessageLogController(
        new MessageLogStore(LIVE_CAPACITY, MAX_SCROLLBACK),
      ),
  );
  const { store } = controller;
  const [tree] = useState(() => {
    const store = new TopicTreeStore();
    store.setExpandMode(savedExpandMode());
    return store;
  });
  const [tab, setTab] = useState<"topics" | "log" | "pipelines">("topics");
  const [selected, setSelected] = useState<TreeNode | null>(null);
  const [statuses, setStatuses] = useState<Map<ConnectionId, ConnectionStatus>>(
    new Map(),
  );
  const [profiles, setProfiles] = useState<ProfileView[]>([]);
  const [editing, setEditing] = useState<Editing | null>(null);
  const [rate, setRate] = useState(0);
  const [publishing, setPublishing] = useState(false);
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
    statuses.has(PIPELINES_CONNECTION),
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
    <div className="app">
      <header className="app__header">
        <strong>måqtt</strong>
        <button
          className={publishing ? "is-active" : undefined}
          onClick={() => setPublishing((open) => !open)}
        >
          Publish
        </button>
        <span className="app__stats">
          {rate.toLocaleString()} msg/s · {store.totalReceived.toLocaleString()}{" "}
          received
          {store.dropped > 0 &&
            ` · ${store.dropped.toLocaleString()} not shown live`}
        </span>
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
              className={tab === "topics" ? "tabs__tab--active" : undefined}
              onClick={() => setTab("topics")}
            >
              Topics
            </button>
            <button
              className={tab === "log" ? "tabs__tab--active" : undefined}
              onClick={() => setTab("log")}
            >
              Message Log
            </button>
            <button
              className={tab === "pipelines" ? "tabs__tab--active" : undefined}
              onClick={() => setTab("pipelines")}
            >
              Pipelines
            </button>
          </nav>
          {tab === "topics" ? (
            <div className="app__split">
              <TopicTree
                store={tree}
                connectionNames={connectionNames}
                connectionStates={connectionStates}
                selectedKey={selected?.key ?? null}
                onSelect={setSelected}
                onContextMenu={(node, x, y) => setMenu({ node, x, y })}
                branchLabel={pipelineBranchLabel}
              />
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
                />
              )}
            </div>
          ) : tab === "log" ? (
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
            <PublishPanel
              draft={draft}
              onDraftChange={setDraft}
              profiles={profiles}
              statuses={statusesByProfile}
              onClose={() => setPublishing(false)}
            />
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
  );
}

export default App;
