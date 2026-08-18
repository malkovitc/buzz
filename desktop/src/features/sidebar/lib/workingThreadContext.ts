import { getThreadReference } from "@/features/messages/lib/threading";
import { getEventById } from "@/shared/api/tauri";
import type { RelayEvent } from "@/shared/api/types";
import { KIND_FORUM_POST } from "@/shared/constants/kinds";

export type WorkingThreadContext = {
  rootId: string;
  label: string;
};

/** Resolve an observer turn's triggering reply to its human-readable thread root. */
export async function resolveWorkingThreadContext(
  triggeringEventId: string,
  channelId: string,
  fetchEvent: (
    eventId: string,
    channelId?: string,
  ) => Promise<RelayEvent> = getEventById,
): Promise<WorkingThreadContext | null> {
  const trigger = await fetchEvent(triggeringEventId, channelId);
  const rootId =
    trigger.kind === KIND_FORUM_POST
      ? trigger.id
      : getThreadReference(trigger.tags).rootId;
  if (!rootId) return null;

  const root =
    rootId === trigger.id ? trigger : await fetchEvent(rootId, channelId);
  const firstLine = root.content
    .split("\n")
    .map((line) => line.trim())
    .find(Boolean);
  return {
    rootId,
    label: (firstLine ?? `Thread ${rootId.slice(0, 8)}…`)
      .replace(/^#{1,6}\s*/, "")
      .slice(0, 100),
  };
}
