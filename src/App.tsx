import { useCallback, useEffect, useMemo, useState } from "react";
import {
  listProfiles,
  newProfile,
  streamMessages,
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
      .then((generation) => controller.resetForStream(generation))
      .catch((err) => console.error("stream_messages failed", err));
  }, [controller, store]);

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
          <MessageLog
            controller={controller}
            connectionNames={connectionNames}
          />
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
