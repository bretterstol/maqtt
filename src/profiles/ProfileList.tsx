import { useEffect, useState } from "react";
import {
  connect,
  disconnect,
  retryNow,
  subscribe,
  unsubscribe,
  type ConnectionState,
  type ConnectionStatus,
  type ProfileView,
  type SubscriptionStatus,
} from "../bridge";
import "./ProfileList.css";

interface Props {
  profiles: ProfileView[];
  statuses: Map<string, ConnectionStatus>;
  onNew: () => void;
  onEdit: (profile: ProfileView) => void;
}

export function ProfileList({ profiles, statuses, onNew, onEdit }: Props) {
  return (
    <aside className="profiles">
      <div className="profiles__header">
        <span>Brokers</span>
        <button onClick={onNew} title="New Broker Profile">
          + New
        </button>
      </div>
      {profiles.length === 0 && (
        <p className="profiles__empty">
          No Broker Profiles yet. Add one to connect.
        </p>
      )}
      <ul className="profiles__list">
        {profiles.map((profile) => (
          <ProfileItem
            key={profile.id}
            profile={profile}
            status={statuses.get(profile.id)}
            onEdit={() => onEdit(profile)}
          />
        ))}
      </ul>
    </aside>
  );
}

function isLive(state: ConnectionState | undefined): boolean {
  return state !== undefined && state.kind !== "closed";
}

function ProfileItem({
  profile,
  status,
  onEdit,
}: {
  profile: ProfileView;
  status: ConnectionStatus | undefined;
  onEdit: () => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const live = isLive(status?.state);
  const stateKind = status?.state.kind ?? "closed";

  const toggle = async () => {
    setError(null);
    try {
      await (live ? disconnect(profile.id) : connect(profile.id));
    } catch (err) {
      setError(String(err));
    }
  };

  return (
    <li className="profile">
      <div className="profile__row">
        <span
          className={`status-dot status-dot--${stateKind}`}
          title={stateTitle(status?.state)}
        />
        <button className="profile__name" onClick={onEdit} title="Edit">
          <strong>{profile.name}</strong>
          <small>
            {profile.host}:{profile.port}
            {profile.tls ? " · TLS" : ""}
            {profile.protocol === "mqtt311" ? " · 3.1.1" : ""}
          </small>
        </button>
        <button onClick={toggle}>{live ? "Disconnect" : "Connect"}</button>
      </div>
      {status?.state.kind === "disconnected" && (
        <div className="profile__retry">
          <p className="profile__error">{status.state.reason}</p>
          <RetryCountdown retryAtMs={status.state.retryAtMs} />
          <button onClick={() => void retryNow(profile.id)}>Retry now</button>
        </div>
      )}
      {error && <p className="profile__error">{error}</p>}
      {live && status && (
        <Subscriptions profileId={profile.id} status={status} />
      )}
    </li>
  );
}

function RetryCountdown({ retryAtMs }: { retryAtMs: number }) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 500);
    return () => clearInterval(timer);
  }, []);
  const seconds = Math.max(0, Math.ceil((retryAtMs - now) / 1000));
  return (
    <span className="profile__countdown">
      {seconds > 0 ? `Retrying in ${seconds} s` : "Retrying…"}
    </span>
  );
}

function stateTitle(state: ConnectionState | undefined): string {
  if (!state || state.kind === "closed") return "Not connected";
  if (state.kind === "disconnected") return `Disconnected: ${state.reason}`;
  return state.kind === "connected" ? "Connected" : "Connecting…";
}

function Subscriptions({
  profileId,
  status,
}: {
  profileId: string;
  status: ConnectionStatus;
}) {
  const [filter, setFilter] = useState("");
  const [qos, setQos] = useState(0);
  const [error, setError] = useState<string | null>(null);

  const add = async () => {
    if (!filter.trim()) return;
    setError(null);
    try {
      await subscribe(profileId, { filter: filter.trim(), qos });
      setFilter("");
    } catch (err) {
      setError(String(err));
    }
  };

  return (
    <div className="subscriptions">
      <ul>
        {status.subscriptions.map((sub) => (
          <li key={sub.filter} className="subscription">
            <SubscriptionIcon sub={sub} />
            <code title={sub.filter}>{sub.filter}</code>
            <span className="subscription__qos">Q{sub.qos}</span>
            <button
              className="icon-button"
              title="Unsubscribe (this session only)"
              onClick={() => void unsubscribe(profileId, sub.filter)}
            >
              ×
            </button>
          </li>
        ))}
      </ul>
      <form
        className="subscriptions__add"
        onSubmit={(e) => {
          e.preventDefault();
          void add();
        }}
      >
        <input
          placeholder="Subscribe to…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          title="Topic filter; added for this session only"
        />
        <select value={qos} onChange={(e) => setQos(Number(e.target.value))}>
          <option value={0}>Q0</option>
          <option value={1}>Q1</option>
          <option value={2}>Q2</option>
        </select>
      </form>
      {error && <p className="profile__error">{error}</p>}
    </div>
  );
}

function SubscriptionIcon({ sub }: { sub: SubscriptionStatus }) {
  switch (sub.state.kind) {
    case "active":
      return (
        <span
          className="subscription__state subscription__state--active"
          title={`Active, granted QoS ${sub.state.grantedQos}`}
        >
          ●
        </span>
      );
    case "rejected":
      return (
        <span
          className="subscription__state subscription__state--rejected"
          title={`Rejected: ${sub.state.reason}`}
        >
          !
        </span>
      );
    default:
      return (
        <span className="subscription__state" title="Waiting for the Broker">
          ○
        </span>
      );
  }
}
