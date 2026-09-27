import { useCallback, useEffect, useMemo, useState } from "react";
import {
  listProfiles,
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
import { TopicTreeStore, type TreeNode } from "./topicTree/store";
import { TopicTree } from "./topicTree/TopicTree";
import "./App.css";

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
  const [tree] = useState(() => new TopicTreeStore());
  const [tab, setTab] = useState<"topics" | "log">("topics");
  const [selected, setSelected] = useState<TreeNode | null>(null);
  const [statuses, setStatuses] = useState<Map<ConnectionId, ConnectionStatus>>(
    new Map(),
  );
  const [profiles, setProfiles] = useState<ProfileView[]>([]);
  const [editing, setEditing] = useState<Editing | null>(null);
  const [rate, setRate] = useState(0);

  useEffect(() => {
    listProfiles()
      .then(setProfiles)
      .catch((err) => console.error("list_profiles failed", err));
  }, []);

  useEffect(() => {
    streamMessages((batch) => {
      store.applyBatch(batch);
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
        tree.apply(await topicSnapshot());
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
          </nav>
          {tab === "topics" ? (
            <TopicTree
              store={tree}
              connectionNames={connectionNames}
              connectionStates={connectionStates}
              selectedKey={selected?.key ?? null}
              onSelect={setSelected}
            />
          ) : (
            <MessageLog
              controller={controller}
              connectionNames={connectionNames}
            />
          )}
        </main>
      </div>
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
