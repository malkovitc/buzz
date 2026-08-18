import { useChannelMembersQuery } from "@/features/channels/hooks";
import type { UserProfileLookup } from "@/features/profile/lib/identity";
import type { ChannelType } from "@/shared/api/types";
import { useMentions } from "./useMentions";

/** Channel-aware mention candidates for stream/thread composers. */
export function useComposerMentions(
  channelId: string | null,
  profiles: UserProfileLookup | undefined,
  channelType: ChannelType | null,
) {
  const membersQuery = useChannelMembersQuery(channelId);
  return useMentions(channelId, membersQuery.data, profiles, { channelType });
}
