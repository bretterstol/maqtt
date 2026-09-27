import { useEffect, useMemo, useState } from "react";
import {
  streamMessages,
  type ConnectionId,
  type ConnectionStatus,
} from "./bridge";
import { MessageLogController } from "./messageLog/controller";
import { MessageLog } from "./messageLog/MessageLog";
import { MessageLogStore } from "./messageLog/store";
import "./App.css";

const LIVE_CAPACITY = 5_000;
const MAX_SCROLLBACK = 100_000;

function App() {
  const [controller] = useState(
    () =>
      new MessageLogController(
        new MessageLogStore(LIVE_CAPACITY, MAX_SCROLLBACK),
      ),
  );
  const { store } = controller;
  const [connections, setConnections] = useState<
    Map<ConnectionId, ConnectionStatus>
  >(new Map());
  const [rate, setRate] = useState(0);

  useEffect(() => {
    streamMessages((batch) => {
      store.applyBatch(batch);
      if (batch.connectionStates.length > 0) {
        setConnections((previous) => {
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

  const connectionNames = useMemo(
    () => new Map([...connections].map(([id, status]) => [id, status.name])),
    [connections],
  );

  return (
    <div className="app">
      <header className="app__header">
        <strong>måqtt</strong>
        {[...connections.values()].map((status) => (
          <ConnectionBadge key={status.connectionId} status={status} />
        ))}
        <span className="app__stats">
          {rate.toLocaleString()} msg/s · {store.totalReceived.toLocaleString()}{" "}
          received
          {store.dropped > 0 &&
            ` · ${store.dropped.toLocaleString()} not shown live`}
        </span>
      </header>
      <MessageLog controller={controller} connectionNames={connectionNames} />
    </div>
  );
}

function ConnectionBadge({ status }: { status: ConnectionStatus }) {
  const { state } = status;
  const title = state.kind === "disconnected" ? state.reason : state.kind;
  return (
    <span className="connection-badge" title={title}>
      <span
        className={`connection-badge__dot connection-badge__dot--${state.kind}`}
      />
      {status.name}
    </span>
  );
}

export default App;
