import { classifyRelayClosed } from "@/shared/api/relayClosedPolicy";
import {
  activateRateLimit,
  isRateLimited,
  parseRateLimitHint,
  rateLimitRemainingMs,
  waitForRateLimit,
} from "@/shared/api/relayRateLimitGate";
import {
  sortEvents,
  type RelaySubscription,
  type RelaySubscriptionFilter,
  type SubscriptionEventBufferItem,
} from "@/shared/api/relayClientShared";
import type { ChannelReconnectRepairRequest } from "@/shared/api/channelReconnectRepair";
import {
  PAGE_REPLAY_MAX_ATTEMPTS,
  replayReconnectHistoryPages,
  resolveReconnectReplaySince,
  shouldPageReconnectReplay,
} from "@/shared/api/relayReconnectReplay";
import type { RelayEvent } from "@/shared/api/types";

const RETRY_BASE_DELAY_MS = 1_000;
const RETRY_MAX_DELAY_MS = 30_000;

type LiveSubscription = Extract<RelaySubscription, { mode: "live" }>;

export function clearClosedRetry(subscription: LiveSubscription) {
  if (subscription.closedRetryTimeout === undefined) return;
  window.clearTimeout(subscription.closedRetryTimeout);
  subscription.closedRetryTimeout = undefined;
}

function notifyClosedRepairSettled(subscription: LiveSubscription) {
  try {
    subscription.onClosedRepairSettled?.();
  } catch (error) {
    console.error("Failed to refresh after CLOSED repair", error);
  }
}

export function handleRelayClosed({
  subscriptions,
  subId,
  message,
  sendReq,
  requestRepair,
  replaySubscriptionEvent,
  flushReplayEvents,
  generation = 0,
  isActive = () => true,
}: {
  subscriptions: Map<string, RelaySubscription>;
  subId: string;
  message: string;
  sendReq: (subId: string, filter: RelaySubscriptionFilter) => Promise<void>;
  requestRepair?: (
    request: ChannelReconnectRepairRequest,
  ) => Promise<RelayEvent[]>;
  replaySubscriptionEvent?: (subId: string, event: RelayEvent) => void;
  flushReplayEvents?: () => void;
  generation?: number;
  isActive?: () => boolean;
}) {
  const subscription = subscriptions.get(subId);
  if (!subscription) return;
  if (subscription.mode !== "live") {
    // Classify before rejecting so a `rate-limited:` history CLOSED arms the
    // gate for concurrent ops. A history sub can't be retried (the caller holds
    // the promise), so we still reject immediately after arming.
    const closedClass = classifyRelayClosed(message);
    if (closedClass === "rate-limited") {
      const hintSeconds = parseRateLimitHint(message);
      activateRateLimit(hintSeconds);
    }
    window.clearTimeout(subscription.timeout);
    subscriptions.delete(subId);
    subscription.reject(
      new Error(message || "Relay closed the history subscription."),
    );
    return;
  }
  recoverLiveSubscriptionFromClosed({
    subscriptions,
    subId,
    subscription,
    message,
    sendReq,
    requestRepair,
    replaySubscriptionEvent,
    flushReplayEvents,
    generation,
    isActive,
  });
}

function recoverLiveSubscriptionFromClosed({
  subscriptions,
  subId,
  subscription,
  message,
  sendReq,
  requestRepair,
  replaySubscriptionEvent,
  flushReplayEvents,
  generation,
  isActive,
}: {
  subscriptions: Map<string, RelaySubscription>;
  subId: string;
  subscription: LiveSubscription;
  message: string;
  sendReq: (subId: string, filter: RelaySubscriptionFilter) => Promise<void>;
  requestRepair?: (
    request: ChannelReconnectRepairRequest,
  ) => Promise<RelayEvent[]>;
  replaySubscriptionEvent?: (subId: string, event: RelayEvent) => void;
  flushReplayEvents?: () => void;
  generation: number;
  isActive: () => boolean;
}) {
  subscription.resolveReady?.("closed");
  subscription.resolveReady = undefined;

  const closedClass = classifyRelayClosed(message);

  if (closedClass === "terminal") {
    // Auth/access/filter failure — permanently remove the subscription so it
    // doesn't silently loop.
    subscriptions.delete(subId);
    return;
  }

  if (subscription.closedRetryTimeout !== undefined) return;

  const attempt = subscription.closedRetryAttempt ?? 0;
  const backoffMs = Math.min(
    RETRY_BASE_DELAY_MS * 2 ** attempt,
    RETRY_MAX_DELAY_MS,
  );

  let delayMs = backoffMs;

  if (closedClass === "rate-limited") {
    // Activate the gate so concurrent operations back off too.
    const hintSeconds = parseRateLimitHint(message);
    activateRateLimit(hintSeconds);
    // Use the gate's actual remaining time so a shorter hint arriving under a
    // longer active gate does not schedule a premature retry that just gets
    // another CLOSED. The fallback covers the gate-inactive edge case
    // (hint * 1000, or 10s default when no hint).
    const fallbackMs = (hintSeconds ?? 10) * 1_000;
    delayMs = Math.max(backoffMs, rateLimitRemainingMs() || fallbackMs);
  }

  subscription.closedRetryAttempt = attempt + 1;
  subscription.closedRetryTimeout = window.setTimeout(() => {
    subscription.closedRetryTimeout = undefined;
    if (!isActive() || subscriptions.get(subId) !== subscription) return;

    const channelId = subscription.filter["#h"]?.[0];
    const shouldPageReplay = shouldPageReconnectReplay(subscription.filter);
    const replaySince = resolveReconnectReplaySince(
      subscription,
      shouldPageReplay,
    );
    const shouldRepair =
      channelId !== undefined &&
      replaySince !== undefined &&
      requestRepair !== undefined &&
      shouldPageReplay;

    if (shouldRepair) {
      subscription.pendingReplaySince = replaySince;
      subscription.reconnectReplay = {
        generation,
        seenEventIds: new Set(),
        liveEose: false,
        repairDone: false,
      };
    }

    void (async () => {
      try {
        await sendReq(subId, subscription.filter);
      } catch (error) {
        if (subscriptions.get(subId) !== subscription) return;
        console.error("Failed to restore closed relay subscription", error);
        recoverLiveSubscriptionFromClosed({
          subscriptions,
          subId,
          subscription,
          message,
          sendReq,
          requestRepair,
          replaySubscriptionEvent,
          flushReplayEvents,
          generation,
          isActive,
        });
        return;
      }
      if (
        !shouldRepair ||
        channelId === undefined ||
        replaySince === undefined ||
        !requestRepair
      ) {
        return;
      }

      for (let attempt = 1; attempt <= PAGE_REPLAY_MAX_ATTEMPTS; attempt++) {
        try {
          const completed = await replayReconnectHistoryPages({
            subscription,
            channelId,
            since: subscription.pendingReplaySince ?? replaySince,
            until: undefined,
            isActive: () =>
              isActive() && subscriptions.get(subId) === subscription,
            requestRepair,
            replaySubscriptionEvent: replaySubscriptionEvent
              ? (event) => replaySubscriptionEvent(subId, event)
              : undefined,
          });
          if (completed) {
            flushReplayEvents?.();
            subscription.pendingReplaySince = undefined;
            markReconnectRepairDone(subscription, generation);
            notifyClosedRepairSettled(subscription);
          }
          return;
        } catch (error) {
          console.warn(
            `[CLOSED recovery] history backfill attempt ${attempt}/${PAGE_REPLAY_MAX_ATTEMPTS} failed for ${subId}:`,
            error,
          );
          if (attempt === PAGE_REPLAY_MAX_ATTEMPTS) {
            // Partial attempts may have queued rows. Dispatch them while the
            // cross-attempt/live dedupe set still exists, then release this
            // generation without clearing the unresolved floor.
            flushReplayEvents?.();
            markReconnectRepairDone(subscription, generation);
            notifyClosedRepairSettled(subscription);
            return;
          }
          if (isRateLimited()) await waitForRateLimit();
          if (!isActive() || subscriptions.get(subId) !== subscription) return;
        }
      }
    })();
  }, delayMs);
}

export function prepareBufferedSubscriptionEvent(
  subscription: RelaySubscription,
  event: RelayEvent,
  isRepair: boolean,
) {
  return isRepair
    ? subscription.mode === "live"
    : prepareSubscriptionEvent(subscription, event);
}

export function prepareSubscriptionEvent(
  subscription: RelaySubscription,
  event: RelayEvent,
) {
  if (subscription.mode === "history") {
    subscription.events.push(event);
    return false;
  }
  if (subscription.mode === "first") {
    return false;
  }
  subscription.closedRetryAttempt = 0;
  clearClosedRetry(subscription);
  subscription.lastSeenCreatedAt = Math.max(
    subscription.lastSeenCreatedAt ?? 0,
    event.created_at,
  );
  return true;
}

export function shouldDispatchSubscriptionEvent(
  subscription: Extract<RelaySubscription, { mode: "live" }>,
  event: RelayEvent,
) {
  const replay = subscription.reconnectReplay;
  if (replay?.seenEventIds.has(event.id)) return false;
  replay?.seenEventIds.add(event.id);
  return true;
}

export function flushEvents(
  buffer: SubscriptionEventBufferItem[],
  subscriptions: Map<string, RelaySubscription>,
  generation: number,
) {
  const flushCallbacks = new Set<() => void>();
  for (const item of buffer) {
    const subscription = subscriptions.get(item.subId);
    if (
      subscription?.mode === "live" &&
      item.generation === generation &&
      shouldDispatchSubscriptionEvent(subscription, item.event)
    ) {
      subscription.onEvent(item.event);
      if (subscription.onFlush) flushCallbacks.add(subscription.onFlush);
    }
  }
  for (const callback of flushCallbacks) callback();
}

export function markReconnectLiveEose(
  subscription: Extract<RelaySubscription, { mode: "live" }>,
  generation: number,
) {
  const replay = subscription.reconnectReplay;
  if (!replay || replay.generation !== generation) return;
  replay.liveEose = true;
  if (replay.repairDone) subscription.reconnectReplay = undefined;
}

export function markReconnectRepairDone(
  subscription: Extract<RelaySubscription, { mode: "live" }>,
  generation: number,
) {
  const replay = subscription.reconnectReplay;
  if (!replay || replay.generation !== generation) return;
  replay.repairDone = true;
  if (replay.liveEose) subscription.reconnectReplay = undefined;
}

export function handleSubscriptionEose({
  subscriptions,
  subId,
  closeSubscription,
  generation,
}: {
  subscriptions: Map<string, RelaySubscription>;
  subId: string;
  closeSubscription: (subId: string) => Promise<void>;
  generation?: number;
}) {
  const subscription = subscriptions.get(subId);
  if (!subscription) return;
  if (subscription.mode === "live") {
    if (generation !== undefined)
      markReconnectLiveEose(subscription, generation);
    subscription.resolveReady?.("eose");
    subscription.resolveReady = undefined;
    subscription.closedRetryAttempt = 0;
    clearClosedRetry(subscription);
    return;
  }
  window.clearTimeout(subscription.timeout);
  subscriptions.delete(subId);
  void closeSubscription(subId);
  if (subscription.mode === "first") {
    subscription.resolve(null);
  } else {
    subscription.resolve(sortEvents(subscription.events));
  }
}
