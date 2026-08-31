import * as React from "react";

import {
  CALIPER_CLOUD_CONTROL_PUBKEY,
  cloudControlSnapshot,
  type CloudControlUiState,
  isCaliperCloudControlContext,
  reconcileCloudControlState,
} from "@/features/messages/lib/cloudControlCommands";
import type { TimelineMessage } from "@/features/messages/types";
import type { MessageComposerCloudControls } from "@/features/messages/ui/MessageComposer.types";

export function useCloudControlState({
  channelId,
  currentPubkey,
  historyPending,
  threadHead,
  threadMessages,
}: {
  channelId: string | null;
  currentPubkey: string | null;
  historyPending: boolean;
  threadHead: TimelineMessage | null;
  threadMessages: readonly TimelineMessage[];
}): MessageComposerCloudControls | undefined {
  const threadId = threadHead?.id ?? null;
  const eligible = isCaliperCloudControlContext(
    channelId,
    threadId,
    currentPubkey,
  );
  const snapshot = React.useMemo(
    () =>
      eligible
        ? cloudControlSnapshot([
            ...(threadHead ? [threadHead] : []),
            ...threadMessages,
          ])
        : null,
    [eligible, threadHead, threadMessages],
  );
  const baselineRef = React.useRef<{
    threadId: string | null;
    messageId: string | null;
  } | null>(null);
  const [state, setState] = React.useState<CloudControlUiState>("unknown");

  // biome-ignore lint/correctness/useExhaustiveDependencies: snapshot changes are handled by the live-reply effect
  React.useEffect(() => {
    if (!eligible || historyPending) return;
    baselineRef.current = {
      threadId,
      messageId: snapshot?.messageId ?? null,
    };
    setState("unknown");
    // Capture history once it has settled; later IDs are live replies.
  }, [eligible, historyPending, threadId]);

  React.useEffect(() => {
    const baseline = baselineRef.current;
    if (
      eligible &&
      !historyPending &&
      baseline?.threadId === threadId &&
      snapshot &&
      snapshot.messageId !== baseline.messageId
    ) {
      setState((current) =>
        reconcileCloudControlState(
          current,
          snapshot.state,
          snapshot.preservePending,
        ),
      );
    }
  }, [eligible, historyPending, snapshot, threadId]);

  const displayedState =
    baselineRef.current?.threadId === threadId ? state : "unknown";
  return React.useMemo(
    () =>
      !eligible || historyPending
        ? undefined
        : {
            agentPubkey: CALIPER_CLOUD_CONTROL_PUBKEY,
            state: displayedState,
          },
    [displayedState, eligible, historyPending],
  );
}
