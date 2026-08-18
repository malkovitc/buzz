import { expect, test } from "@playwright/test";

import { TEST_IDENTITIES, installMockBridge } from "../helpers/bridge";

const FORUM_CHANNEL_ID = "a27e1ee9-76a6-5bdf-a5d5-1d85610dad11";
const FORUM_POST_ID = "mock-forum-release-thread";

test.beforeEach(async ({ page }) => {
  await installMockBridge(page);
});

type ActiveTurnWindow = Window & {
  __BUZZ_E2E_SEED_ACTIVE_TURNS__?: (input: {
    agentPubkey: string;
    channelId: string;
    turnId: string;
    kind?: "turn_started" | "turn_completed";
  }) => void;
};

test("forum thread shows the shared live agent activity status", async ({
  page,
}) => {
  await page.goto(`/#/channels/${FORUM_CHANNEL_ID}/posts/${FORUM_POST_ID}`);
  await expect(page.getByTestId("chat-title")).toHaveText("watercooler");

  await page.waitForFunction(
    () =>
      typeof (window as ActiveTurnWindow).__BUZZ_E2E_SEED_ACTIVE_TURNS__ ===
      "function",
  );
  await page.evaluate(
    ({ agentPubkey, channelId }) => {
      const seedActiveTurns = (window as ActiveTurnWindow)
        .__BUZZ_E2E_SEED_ACTIVE_TURNS__;
      if (!seedActiveTurns)
        throw new Error("Active-turn helper is unavailable.");
      seedActiveTurns({
        agentPubkey,
        channelId,
        turnId: "forum-agent-turn",
      });
    },
    { agentPubkey: TEST_IDENTITIES.alice.pubkey, channelId: FORUM_CHANNEL_ID },
  );

  const activityRow = page.getByTestId("forum-agent-activity-row");
  await expect(activityRow).toBeVisible();
  await expect(
    activityRow.getByTestId("bot-activity-composer-trigger"),
  ).toContainText("Working");

  await activityRow.getByTestId("bot-activity-composer-trigger").click();
  const item = page.getByTestId(
    `bot-activity-composer-item-${TEST_IDENTITIES.alice.pubkey}`,
  );
  await expect(item).toContainText("Working");
  await expect(item).not.toContainText("View activity");

  await page.evaluate(
    ({ agentPubkey, channelId }) => {
      (window as ActiveTurnWindow).__BUZZ_E2E_SEED_ACTIVE_TURNS__?.({
        agentPubkey,
        channelId,
        turnId: "forum-agent-turn",
        kind: "turn_completed",
      });
    },
    { agentPubkey: TEST_IDENTITIES.alice.pubkey, channelId: FORUM_CHANNEL_ID },
  );
  await expect(activityRow).toHaveCount(0);
});
