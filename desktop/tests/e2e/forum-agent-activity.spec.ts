import { expect, test } from "@playwright/test";

import { TEST_IDENTITIES, installMockBridge } from "../helpers/bridge";

const FORUM_CHANNEL_ID = "a27e1ee9-76a6-5bdf-a5d5-1d85610dad11";
const FORUM_POST_ID = "mock-forum-release-thread";
const AGENT = TEST_IDENTITIES.charlie;
const SECOND_AGENT = TEST_IDENTITIES.bob;

test.beforeEach(async ({ page }) => {
  await installMockBridge(page, {
    managedAgents: [
      {
        pubkey: AGENT.pubkey,
        name: AGENT.username,
        status: "running",
        channelIds: [FORUM_CHANNEL_ID],
      },
      {
        pubkey: SECOND_AGENT.pubkey,
        name: SECOND_AGENT.username,
        status: "running",
        channelIds: [FORUM_CHANNEL_ID],
      },
    ],
  });
});

type ActiveTurnWindow = Window & {
  __BUZZ_E2E_SEED_ACTIVE_TURNS__?: (input: {
    agentPubkey: string;
    channelId: string;
    turnId: string;
    kind?: "turn_started" | "turn_completed";
  }) => void;
  __BUZZ_E2E_SEED_OBSERVER_EVENTS__?: (input: {
    agentPubkey: string;
    events: Array<Record<string, unknown>>;
  }) => void;
};

test("forum thread shows the shared live agent activity status", async ({
  page,
}) => {
  await page.goto(`/#/channels/${FORUM_CHANNEL_ID}/posts/${FORUM_POST_ID}`);
  await expect(page.getByTestId("chat-title")).toHaveText("watercooler", {
    timeout: 15_000,
  });

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
    { agentPubkey: AGENT.pubkey, channelId: FORUM_CHANNEL_ID },
  );

  await page.evaluate(
    ({ agentPubkey, channelId }) => {
      (window as ActiveTurnWindow).__BUZZ_E2E_SEED_OBSERVER_EVENTS__?.({
        agentPubkey,
        events: [
          {
            seq: 1,
            timestamp: new Date().toISOString(),
            kind: "acp_read",
            agentIndex: 0,
            channelId,
            sessionId: "forum-session",
            turnId: "forum-agent-turn",
            payload: {
              method: "session/update",
              params: {
                sessionId: "forum-session",
                update: {
                  sessionUpdate: "current_mode_update",
                  currentModeId: "plan",
                },
              },
            },
          },
          {
            seq: 2,
            timestamp: new Date().toISOString(),
            kind: "acp_read",
            agentIndex: 0,
            channelId,
            sessionId: "forum-session",
            turnId: "forum-agent-turn",
            payload: {
              method: "session/update",
              params: {
                sessionId: "forum-session",
                update: {
                  sessionUpdate: "tool_call",
                  toolCallId: "forum-shell",
                  title: "pnpm test",
                  kind: "shell",
                  status: "completed",
                  rawInput: { command: "pnpm test" },
                  content: [{ type: "text", text: "47 tests passed" }],
                },
              },
            },
          },
        ],
      });
    },
    { agentPubkey: AGENT.pubkey, channelId: FORUM_CHANNEL_ID },
  );

  const activityRow = page.getByTestId("forum-agent-activity-row");
  await expect(activityRow).toBeVisible();
  await expect(
    activityRow.getByTestId("bot-activity-composer-trigger"),
  ).toContainText("pnpm test");

  await activityRow.getByTestId("bot-activity-composer-trigger").click();
  const item = page.getByTestId(`bot-activity-composer-item-${AGENT.pubkey}`);
  await expect(item).toContainText("pnpm test");
  await expect(item).toContainText("Open");
  await item.evaluate((element) => (element as HTMLButtonElement).click());
  const activityPanel = page.getByTestId("agent-session-thread-panel");
  await expect(activityPanel).toBeVisible();
  await expect(activityPanel.getByText("Mode")).toBeVisible();

  const shellItem = activityPanel
    .getByTestId("transcript-tool-item")
    .filter({ hasText: "pnpm test" });
  await shellItem.locator("summary").click();
  await expect(shellItem.getByText("47 tests passed")).toBeVisible();

  await page.evaluate(
    ({ agentPubkey, channelId }) => {
      (window as ActiveTurnWindow).__BUZZ_E2E_SEED_ACTIVE_TURNS__?.({
        agentPubkey,
        channelId,
        turnId: "forum-agent-turn",
        kind: "turn_completed",
      });
    },
    { agentPubkey: AGENT.pubkey, channelId: FORUM_CHANNEL_ID },
  );
  await expect(activityRow).toHaveCount(0);
});

test("forum activity popover shows each working agent's current command", async ({
  page,
}) => {
  await page.goto(`/#/channels/${FORUM_CHANNEL_ID}/posts/${FORUM_POST_ID}`);
  await expect(page.getByTestId("chat-title")).toHaveText("watercooler", {
    timeout: 15_000,
  });
  await page.waitForFunction(
    () =>
      typeof (window as ActiveTurnWindow).__BUZZ_E2E_SEED_ACTIVE_TURNS__ ===
      "function",
  );

  await page.evaluate(
    ({ agents, channelId }) => {
      for (const [index, agent] of agents.entries()) {
        (window as ActiveTurnWindow).__BUZZ_E2E_SEED_ACTIVE_TURNS__?.({
          agentPubkey: agent.pubkey,
          channelId,
          turnId: `multi-turn-${index}`,
        });
        (window as ActiveTurnWindow).__BUZZ_E2E_SEED_OBSERVER_EVENTS__?.({
          agentPubkey: agent.pubkey,
          events: [
            {
              seq: 1,
              timestamp: new Date().toISOString(),
              kind: "acp_read",
              agentIndex: index,
              channelId,
              sessionId: `multi-session-${index}`,
              turnId: `multi-turn-${index}`,
              payload: {
                method: "session/update",
                params: {
                  update: {
                    sessionUpdate: "tool_call",
                    toolCallId: `multi-tool-${index}`,
                    title: agent.command,
                    kind: "shell",
                    status: "executing",
                    rawInput: { command: agent.command },
                  },
                },
              },
            },
          ],
        });
      }
    },
    {
      agents: [
        { pubkey: AGENT.pubkey, command: "pnpm test" },
        { pubkey: SECOND_AGENT.pubkey, command: "git diff --stat" },
      ],
      channelId: FORUM_CHANNEL_ID,
    },
  );

  const trigger = page
    .getByTestId("forum-agent-activity-row")
    .getByTestId("bot-activity-composer-trigger");
  await expect(trigger).toContainText("+1");
  await trigger.click();
  await expect(
    page.getByTestId(`bot-activity-composer-item-${AGENT.pubkey}`),
  ).toContainText("pnpm test");
  await expect(
    page.getByTestId(`bot-activity-composer-item-${SECOND_AGENT.pubkey}`),
  ).toContainText("git diff --stat");
});
