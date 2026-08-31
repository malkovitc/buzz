import type { MutableRefObject } from "react";

import {
  publishCloudControlCommand,
  type CloudControlCommand,
} from "@/features/messages/lib/cloudControlCommands";
import type { MessageComposerProps } from "./MessageComposer.types";

export async function submitComposerCloudControl({
  agentPubkey,
  channelId,
  clearCurrentComposer,
  getCurrentContent,
  markDraftSent,
  command,
  currentDraftKeyRef,
  onError,
  onPreparingChange,
  send,
  sentDraftKey,
  submitLockedRef,
  submittedContent,
  submittedDraftKey,
  threadContext,
}: {
  agentPubkey: string;
  channelId: string | null;
  clearCurrentComposer: () => void;
  getCurrentContent: () => string;
  markDraftSent: (
    draftKey: string,
    content: string,
    channelId: string,
    pendingImeta: [],
    spoileredAttachmentUrls: [],
  ) => void;
  command: CloudControlCommand;
  currentDraftKeyRef: MutableRefObject<string | null | undefined>;
  onError: (error: unknown) => void;
  onPreparingChange?: (pending: boolean) => void;
  send: MessageComposerProps["onSend"];
  sentDraftKey: string | null;
  submitLockedRef: MutableRefObject<boolean>;
  submittedContent: string;
  submittedDraftKey: string | null | undefined;
  threadContext: {
    parentEventId: string | null;
    threadHeadId: string | null;
  };
}) {
  submitLockedRef.current = true;
  onPreparingChange?.(true);
  try {
    await publishCloudControlCommand({
      command,
      agentPubkey,
      channelId,
      threadContext,
      send,
    });
    if (sentDraftKey && channelId) {
      markDraftSent(sentDraftKey, submittedContent, channelId, [], []);
    }
    if (
      currentDraftKeyRef.current === submittedDraftKey &&
      getCurrentContent().trim() === submittedContent
    ) {
      clearCurrentComposer();
    }
  } catch (error) {
    onError(error);
  } finally {
    submitLockedRef.current = false;
    onPreparingChange?.(false);
  }
}
