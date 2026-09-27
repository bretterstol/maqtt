/** Short-lived notices ("Copied", "Exported 1,234 lines"), shown by `<Toasts />`. */
export interface Toast {
  id: number;
  text: string;
  error: boolean;
}

const TOAST_MS = 3_000;
let nextId = 1;
let toasts: Toast[] = [];
const listeners = new Set<() => void>();

function emit() {
  listeners.forEach((listener) => listener());
}

export function toast(text: string, error = false): void {
  const item = { id: nextId++, text, error };
  toasts = [...toasts, item];
  emit();
  setTimeout(() => {
    toasts = toasts.filter((t) => t !== item);
    emit();
  }, TOAST_MS);
}

export function subscribeToasts(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function currentToasts(): Toast[] {
  return toasts;
}
