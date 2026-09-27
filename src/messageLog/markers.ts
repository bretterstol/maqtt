import type { MarkerEvent } from "../bridge";

/** How a Message Log marker reads, after the Connection's name. */
export function describeMarker(event: MarkerEvent): {
  text: string;
  tone: "ok" | "bad" | "muted";
} {
  switch (event.kind) {
    case "connected":
      return { text: "connected", tone: "ok" };
    case "reconnected":
      return {
        text: `reconnected after ${formatDuration(event.offlineMs)} offline`,
        tone: "ok",
      };
    case "lost":
      return { text: `lost its connection: ${event.reason}`, tone: "bad" };
    case "failed":
      return { text: `couldn't connect: ${event.reason}`, tone: "bad" };
    case "closed":
      return { text: "disconnected", tone: "muted" };
  }
}

export function formatDuration(ms: number): string {
  const seconds = Math.round(ms / 1000);
  if (seconds < 60) return `${seconds} s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} min ${seconds % 60} s`;
  return `${Math.floor(minutes / 60)} h ${minutes % 60} min`;
}
