import * as React from "react";

import {
  getActiveTurnsByChannel,
  subscribeActiveAgentTurns,
  type ActiveChannelTurnSummary,
} from "@/features/agents/activeAgentTurnsStore";
import type { BotActivityAgent } from "@/features/channels/ui/BotActivityBar";
import { normalizePubkey } from "@/shared/lib/pubkey";

type ForumThreadAgentActivity = {
  workingAgentPubkeys: string[];
  workingTurnIds: string[];
};

export function resolveForumThreadAgentActivity(
  agents: readonly BotActivityAgent[],
  channelSummaries: readonly ActiveChannelTurnSummary[],
  channelId: string,
  threadEventIds: ReadonlySet<string>,
): ForumThreadAgentActivity {
  const summary = channelSummaries.find((turn) => turn.channelId === channelId);
  if (!summary?.activeTurnScopesByAgent) {
    return { workingAgentPubkeys: [], workingTurnIds: [] };
  }
  const workingAgentPubkeys: string[] = [];
  const workingTurnIds: string[] = [];
  for (const agent of agents) {
    const scopes =
      summary.activeTurnScopesByAgent[normalizePubkey(agent.pubkey)] ?? [];
    const matching = scopes.filter((scope) =>
      scope.triggeringEventIds.some((id) => threadEventIds.has(id)),
    );
    if (matching.length > 0) {
      workingAgentPubkeys.push(agent.pubkey);
      workingTurnIds.push(...matching.map((scope) => scope.turnId));
    }
  }
  return { workingAgentPubkeys, workingTurnIds };
}

export function mergeForumThreadTypingPubkeys(
  observerPubkeys: readonly string[],
  typingPubkeys: readonly string[],
): string[] {
  const pubkeys = new Map(
    observerPubkeys.map((pubkey) => [normalizePubkey(pubkey), pubkey]),
  );
  for (const pubkey of typingPubkeys) {
    pubkeys.set(normalizePubkey(pubkey), pubkey);
  }
  return [...pubkeys.values()];
}

export function useForumThreadAgentActivity(
  agents: readonly BotActivityAgent[],
  channelId: string,
  threadEventIds: ReadonlySet<string>,
): ForumThreadAgentActivity {
  const summaries = React.useSyncExternalStore(
    subscribeActiveAgentTurns,
    getActiveTurnsByChannel,
  );
  return React.useMemo(
    () =>
      resolveForumThreadAgentActivity(
        agents,
        summaries,
        channelId,
        threadEventIds,
      ),
    [agents, channelId, summaries, threadEventIds],
  );
}
