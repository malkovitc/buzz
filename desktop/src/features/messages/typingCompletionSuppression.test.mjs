import assert from "node:assert/strict";
import { beforeEach, it } from "node:test";

import {
  recordTypingCompletion,
  resetTypingCompletionSuppressionForTests,
  shouldSuppressTyping,
  subscribeTypingCompletions,
} from "./typingCompletionSuppression.ts";

beforeEach(resetTypingCompletionSuppressionForTests);

it("clears local consumers and rejects queued frames for the completed thread", () => {
  const seen = [];
  const unsubscribe = subscribeTypingCompletions((completion) =>
    seen.push(completion),
  );
  recordTypingCompletion({
    channelId: "forum",
    pubkey: "AGENT",
    threadHeadId: "post-a",
    createdAt: 100,
  });
  unsubscribe();

  assert.equal(seen.length, 1);
  assert.equal(shouldSuppressTyping("forum", "agent", "post-a", 100), true);
  assert.equal(shouldSuppressTyping("forum", "agent", "post-b", 100), false);
  assert.equal(shouldSuppressTyping("other", "agent", "post-a", 100), false);
});
