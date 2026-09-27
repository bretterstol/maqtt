import {
  open,
  save,
  type OpenDialogOptions,
  type SaveDialogOptions,
} from "@tauri-apps/plugin-dialog";

declare global {
  interface Window {
    /**
     * Dev builds only: a path end-to-end tests set to stand in for the native file dialogs,
     * which WebDriver can't operate.
     */
    __maqttDialogAnswer?: string;
  }
}

function testAnswer(): string | undefined {
  return import.meta.env.DEV ? window.__maqttDialogAnswer : undefined;
}

/** Asks where to save; null when cancelled. */
export async function chooseSavePath(
  options: SaveDialogOptions,
): Promise<string | null> {
  return testAnswer() ?? (await save(options));
}

/** Asks for one file to open; null when cancelled. */
export async function chooseFile(
  options: Omit<OpenDialogOptions, "multiple" | "directory">,
): Promise<string | null> {
  const answer = testAnswer();
  if (answer !== undefined) return answer;
  const picked = await open({ ...options, multiple: false, directory: false });
  return typeof picked === "string" ? picked : null;
}
