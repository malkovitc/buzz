import type { TimelineMessage } from "@/features/messages/types";

export const CALIPER_CLOUD_CONTROL_CHANNEL_ID =
  "61b56145-8e1a-41da-9038-043d24f621ec";
export const CALIPER_CLOUD_CONTROL_PUBKEY =
  "fdfb8e2d64fbef9b2b44344a00da43dfa8cf566c8098c0966005558faf220158";
export const CALIPER_CLOUD_CONTROL_OWNER_PUBKEY =
  "7c046a4571dea5832897693790db1209a3b497c0fe47f79f52fbce9f7c5ff8ad";

export function isCaliperCloudControlContext(
  channelId: string | null,
  threadRootId: string | null,
  currentPubkey: string | null,
): boolean {
  return (
    channelId === CALIPER_CLOUD_CONTROL_CHANNEL_ID &&
    /^[0-9a-f]{64}$/.test(threadRootId ?? "") &&
    currentPubkey?.toLowerCase() === CALIPER_CLOUD_CONTROL_OWNER_PUBKEY
  );
}

export type CloudControlCommand = {
  id: "cloud" | "local" | "status";
  slash: "/cloud" | "/local" | "/status";
  wire: "-cloud" | "-local" | "-status";
  label: string;
  description: string;
};

export const EMPTY_CLOUD_CONTROL_COMMANDS: readonly CloudControlCommand[] = [];

export const CLOUD_CONTROL_COMMANDS: readonly CloudControlCommand[] = [
  {
    id: "cloud",
    slash: "/cloud",
    wire: "-cloud",
    label: "Continue in cloud",
    description: "Transfer this task to the cloud worker",
  },
  {
    id: "local",
    slash: "/local",
    wire: "-local",
    label: "Return local",
    description: "Return this task to the local worker",
  },
  {
    id: "status",
    slash: "/status",
    wire: "-status",
    label: "Check location",
    description: "Read the authoritative execution owner",
  },
] as const;

export type CloudControlUiState =
  | "unknown"
  | "local"
  | "cloud"
  | "switching-local"
  | "switching-cloud"
  | "blocked";

export function cloudControlSuggestions(
  text: string,
  cursor: number,
): readonly CloudControlCommand[] {
  const beforeCursor = text.slice(0, cursor);
  if (!/^\/[a-z]*$/i.test(beforeCursor) || text.slice(cursor).trim() !== "") {
    return EMPTY_CLOUD_CONTROL_COMMANDS;
  }
  const query = beforeCursor.slice(1).toLowerCase();
  return CLOUD_CONTROL_COMMANDS.filter(
    (command) =>
      command.id.startsWith(query) ||
      command.label.toLowerCase().includes(query),
  );
}

export async function publishCloudControlCommand({
  command,
  agentPubkey,
  channelId,
  threadContext,
  send,
}: {
  command: CloudControlCommand;
  agentPubkey: string;
  channelId: string | null;
  threadContext: {
    parentEventId: string | null;
    threadHeadId: string | null;
  };
  send: (
    content: string,
    mentionPubkeys: string[],
    mediaTags: undefined,
    channelId: string | null,
    threadContext: {
      parentEventId: string | null;
      threadHeadId: string | null;
    },
    forceRest: boolean,
  ) => Promise<void>;
}): Promise<void> {
  await send(
    command.wire,
    [agentPubkey.toLowerCase()],
    undefined,
    channelId,
    threadContext,
    true,
  );
}

export function cloudControlWireCommand(
  text: string,
): CloudControlCommand | null {
  const normalized = text.trim().toLowerCase();
  return (
    CLOUD_CONTROL_COMMANDS.find((command) => command.slash === normalized) ??
    null
  );
}

function parseFields(body: string): Map<string, string> | null {
  if (!body.startsWith("[PI CLOUD CONTROL]\n")) return null;
  const fields = new Map<string, string>();
  for (const line of body.split("\n").slice(1)) {
    const separator = line.indexOf("=");
    if (separator <= 0) continue;
    fields.set(line.slice(0, separator), line.slice(separator + 1));
  }
  return fields;
}

export function reconcileCloudControlState(
  current: CloudControlUiState,
  next: CloudControlUiState,
  preservePending = false,
): CloudControlUiState {
  if (current === "switching-cloud" && next === "local") return current;
  if (current === "switching-local" && next === "cloud") return current;
  if (preservePending && current.startsWith("switching")) return current;
  return next;
}

export type CloudControlSnapshot = {
  state: CloudControlUiState;
  messageId: string;
  preservePending: boolean;
};

function controlStatusRank(message: TimelineMessage): number {
  const status = parseFields(message.body)?.get("status") ?? "";
  return status === "PREPARED" ? 0 : 1;
}

export function cloudControlSnapshot(
  messages: readonly TimelineMessage[],
): CloudControlSnapshot | null {
  const indexed = messages.map((message, index) => ({ message, index }));
  const commands = indexed.filter(
    ({ message }) =>
      (message.signerPubkey ?? message.pubkey)?.toLowerCase() ===
        CALIPER_CLOUD_CONTROL_OWNER_PUBKEY &&
      CLOUD_CONTROL_COMMANDS.some((command) => command.wire === message.body),
  );
  const newestCommandSecond = Math.max(
    ...commands.map(({ message }) => message.createdAt),
    -1,
  );
  const newestCommands = commands.filter(
    ({ message }) => message.createdAt === newestCommandSecond,
  );
  // Nostr timestamps have one-second precision and event IDs are not ordered.
  // Multiple controls in one second therefore have no safe total order.
  if (newestCommands.length > 1) {
    return {
      state: "unknown",
      messageId: `ambiguous:${newestCommands
        .map(({ message }) => message.id)
        .sort()
        .join(":")}`,
      preservePending: false,
    };
  }
  const latestCommand = newestCommands.sort(
    (left, right) =>
      right.message.createdAt - left.message.createdAt ||
      right.index - left.index,
  )[0]?.message;
  const replies = indexed.filter(
    ({ message }) =>
      (message.signerPubkey ?? message.pubkey)?.toLowerCase() ===
        CALIPER_CLOUD_CONTROL_PUBKEY &&
      message.body.startsWith("[PI CLOUD CONTROL]\n"),
  );
  const correlatedReplies = latestCommand
    ? replies.filter(
        ({ message }) =>
          parseFields(message.body)?.get("command_event") === latestCommand.id,
      )
    : replies;
  const latest = correlatedReplies.sort(
    (left, right) =>
      right.message.createdAt - left.message.createdAt ||
      controlStatusRank(right.message) - controlStatusRank(left.message) ||
      right.message.id.localeCompare(left.message.id) ||
      right.index - left.index,
  )[0]?.message;
  if (!latest) return null;
  const fields = parseFields(latest.body);
  if (!fields) return null;
  const status = fields.get("status") ?? "";
  let state: CloudControlUiState = "unknown";
  if (status.startsWith("BLOCKED") || fields.get("state") === "blocked") {
    state = "blocked";
  } else if (status === "PREPARED") {
    state =
      fields.get("command") === "-cloud"
        ? "switching-cloud"
        : "switching-local";
  } else if (
    fields.get("state") === "cloud-owned" ||
    status === "CLOUD_ACTIVE"
  ) {
    state = "cloud";
  } else if (
    fields.get("state") === "local-owned" ||
    status === "LOCAL_ACTIVE"
  ) {
    state = "local";
  }
  return {
    state,
    messageId: latest.id,
    preservePending: status === "STATUS" && fields.get("state") === "prepared",
  };
}

export function cloudControlUiState(
  messages: readonly TimelineMessage[],
): CloudControlUiState {
  return cloudControlSnapshot(messages)?.state ?? "unknown";
}
