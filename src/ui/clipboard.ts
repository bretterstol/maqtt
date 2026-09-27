import { writeText } from "@tauri-apps/plugin-clipboard-manager";
import { toast } from "./toast";

/** Copies to the system clipboard and says so. */
export async function copy(text: string, what: string): Promise<void> {
  try {
    await writeText(text);
    toast(`Copied ${what}`);
  } catch (err) {
    toast(`Couldn't copy ${what}: ${err}`, true);
  }
}
