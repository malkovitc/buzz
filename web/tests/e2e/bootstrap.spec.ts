import { expect, test } from "../helpers/test";
import { E2E_APP_ORIGIN, bootstrapE2ePage } from "../helpers/bootstrap";

test("automatically starts each test at the app origin", async ({ page }) => {
  expect(new URL(page.url()).origin).toBe(E2E_APP_ORIGIN);
});

test.describe("pre-navigation setup", () => {
  test.use({ e2eBootstrap: false });

  test("reports a failed initial navigation at its source", async ({
    page,
  }) => {
    await expect(
      bootstrapE2ePage(page, { expectedOrigin: "http://127.0.0.1:1" }),
    ).rejects.toThrow(
      /E2E app navigation did not commit to http:\/\/127\.0\.0\.1:1 \(current URL: about:blank\).*Run pnpm test:e2e:smoke or run pnpm build and check the preview server/,
    );
  });
});
