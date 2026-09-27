import { useSyncExternalStore } from "react";
import { currentToasts, subscribeToasts } from "./toast";
import "./Toasts.css";

export function Toasts() {
  const toasts = useSyncExternalStore(subscribeToasts, currentToasts);
  return (
    <div className="toasts" role="status" aria-live="polite">
      {toasts.map((t) => (
        <div key={t.id} className={`toast${t.error ? " toast--error" : ""}`}>
          {t.text}
        </div>
      ))}
    </div>
  );
}
