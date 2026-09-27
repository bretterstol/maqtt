import { useEffect, useMemo, useState } from "react";
import {
  streamMessages,
  type ConnectionId,
  type ConnectionStatus,
} from "./bridge";
import { MessageLog } from "./messageLog/MessageLog";
import { MessageLogStore } from "./messageLog/store";
import "./App.css";

const LOG_CAPACITY = 20_000;

function App() {
  const [logStore] = useState(() => new MessageLogStore(LOG_CAPACITY));
  const [connections, setConnections] = useState<
    Map<ConnectionId, ConnectionStatus>
  >(new Map());
  const [rate, setRate] = useState(0);

  useEffect(() => {
    streamMessages((batch) => {
      logStore.append(batch.messages, batch.dropped);
      if (batch.connectionStates.length > 0) {
        setConnections((previous) => {
          const next = new Map(previous);
          for (const status of batch.connectionStates)
            next.set(status.connectionId, status);
          return next;
        });
      }
    }).catch((err) => console.error("stream_messages failed", err));
  }, [logStore]);

  useEffect(() => {
    let last = logStore.totalReceived;
    const timer = setInterval(() => {
      setRate(logStore.totalReceived - last);
      last = logStore.totalReceived;
    }, 1000);
    return () => clearInterval(timer);
  }, [logStore]);

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
          {rate} msg/s · {logStore.totalReceived.toLocaleString()} received
          {logStore.dropped > 0 &&
            ` · ${logStore.dropped.toLocaleString()} dropped`}
        </span>
      </header>
      <MessageLog store={logStore} connectionNames={connectionNames} />
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
