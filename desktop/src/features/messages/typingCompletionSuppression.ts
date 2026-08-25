type TypingCompletion = {
  channelId: string;
  pubkey: string;
  threadHeadId: string;
  createdAt: number;
};

type Suppression = TypingCompletion & {
  suppressUntil: number;
  retainUntil: number;
};
const suppressions = new Map<string, Suppression>();
const listeners = new Set<(completion: TypingCompletion) => void>();
const POST_COMPLETION_GRACE_MS = 2_000;
const RETENTION_MS = 8_000;

function key(channelId: string, pubkey: string, threadHeadId: string) {
  return `${channelId}\u0000${pubkey.toLowerCase()}\u0000${threadHeadId}`;
}

export function recordTypingCompletion(completion: TypingCompletion) {
  const normalized = { ...completion, pubkey: completion.pubkey.toLowerCase() };
  const suppressionKey = key(
    normalized.channelId,
    normalized.pubkey,
    normalized.threadHeadId,
  );
  suppressions.set(suppressionKey, {
    ...normalized,
    suppressUntil: Date.now() + POST_COMPLETION_GRACE_MS,
    retainUntil: Date.now() + RETENTION_MS,
  });
  if (suppressions.size > 1_000) {
    const oldest = suppressions.keys().next().value;
    if (oldest) suppressions.delete(oldest);
  }
  for (const listener of listeners) listener(normalized);
}

export function shouldSuppressTyping(
  channelId: string,
  pubkey: string,
  threadHeadId: string | null,
  eventCreatedAt: number,
): boolean {
  if (!threadHeadId) return false;
  const suppressionKey = key(channelId, pubkey, threadHeadId);
  const suppression = suppressions.get(suppressionKey);
  if (suppression && Date.now() >= suppression.retainUntil) {
    suppressions.delete(suppressionKey);
    return false;
  }
  return Boolean(
    suppression &&
      (eventCreatedAt <= suppression.createdAt ||
        Date.now() < suppression.suppressUntil),
  );
}

export function subscribeTypingCompletions(
  listener: (completion: TypingCompletion) => void,
) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function resetTypingCompletionSuppression() {
  suppressions.clear();
  listeners.clear();
}

export function resetTypingCompletionSuppressionForTests() {
  resetTypingCompletionSuppression();
  suppressions.clear();
  listeners.clear();
}
