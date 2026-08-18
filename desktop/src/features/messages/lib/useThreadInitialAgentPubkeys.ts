import * as React from "react";

import { useKnownAgentPubkeys } from "@/features/agents/useKnownAgentPubkeys";
import { useChannelMembersQuery } from "@/features/channels/hooks";
import type { TimelineMessage } from "@/features/messages/types";
import type { UserProfileLookup } from "@/features/profile/lib/identity";
import { normalizePubkey } from "@/shared/lib/pubkey";
import { resolveMentionProps } from "@/shared/lib/resolveMentionNames";
import { orderMentionPubkeysByText } from "./orderMentionPubkeys";

/** Preserve agents intentionally addressed by a user-authored thread root. */
export function useThreadInitialAgentPubkeys({
  channelId,
  currentPubkey,
  profiles,
  threadHead,
}: {
  channelId: string | null;
  currentPubkey?: string;
  profiles?: UserProfileLookup;
  threadHead: TimelineMessage | null;
}) {
  const knownAgentPubkeys = useKnownAgentPubkeys();
  const channelMembersQuery = useChannelMembersQuery(channelId);
  const botMemberPubkeys = React.useMemo(
    () =>
      new Set(
        (channelMembersQuery.data ?? [])
          .filter((member) => member.role === "bot" || member.isAgent === true)
          .map((member) => normalizePubkey(member.pubkey)),
      ),
    [channelMembersQuery.data],
  );

  return React.useMemo(() => {
    if (
      !threadHead ||
      !currentPubkey ||
      normalizePubkey(threadHead.signerPubkey ?? threadHead.pubkey ?? "") !==
        normalizePubkey(currentPubkey)
    ) {
      return [];
    }
    const { mentionPubkeysByName } = resolveMentionProps(
      threadHead.tags,
      profiles,
    );
    if (!mentionPubkeysByName) return [];

    return orderMentionPubkeysByText(
      threadHead.body,
      mentionPubkeysByName,
      (pubkey) =>
        knownAgentPubkeys.has(pubkey) ||
        botMemberPubkeys.has(pubkey) ||
        profiles?.[pubkey]?.isAgent === true,
    );
  }, [
    botMemberPubkeys,
    currentPubkey,
    knownAgentPubkeys,
    profiles,
    threadHead,
  ]);
}
