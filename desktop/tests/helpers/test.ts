import { expect, test as base } from "@playwright/test";

import { bootstrapE2ePage } from "./bootstrap";

export { expect };
export type * from "@playwright/test";

type E2eFixtures = {
  /**
   * Set false only when a test must inspect or configure the page before its
   * first app navigation.
   */
  e2eBootstrap: boolean;
};

type TestBody = (
  fixtures: Record<string, unknown>,
  testInfo: unknown,
) => unknown;

const baseTest = base.extend<E2eFixtures>({
  e2eBootstrap: [true, { option: true }],
});

function withAutomaticBootstrap(register: (...args: unknown[]) => unknown) {
  return new Proxy(register, {
    apply(target, thisArg, args: unknown[]) {
      const bodyIndex = args.length - 1;
      const body = args[bodyIndex];
      if (typeof body !== "function")
        return Reflect.apply(target, thisArg, args);

      args[bodyIndex] = async (
        {
          page,
          baseURL,
          e2eBootstrap,
          browser,
          context,
          request,
        }: {
          page: import("@playwright/test").Page;
          baseURL?: string;
          e2eBootstrap: boolean;
          browser: unknown;
          context: unknown;
          request: unknown;
        },
        testInfo: unknown,
      ) => {
        if (e2eBootstrap) {
          if (!baseURL) {
            throw new Error(
              "E2E app navigation requires Playwright use.baseURL.",
            );
          }
          await bootstrapE2ePage(page, {
            expectedOrigin: new URL(baseURL).origin,
          });
        }
        return (body as TestBody)(
          { page, baseURL, e2eBootstrap, browser, context, request },
          testInfo,
        );
      };
      return Reflect.apply(target, thisArg, args);
    },
    get(target, property, receiver) {
      const value = Reflect.get(target, property, receiver);
      return typeof property === "string" &&
        ["only", "skip", "fixme", "fail"].includes(property) &&
        typeof value === "function"
        ? withAutomaticBootstrap(value)
        : value;
    },
  });
}

/**
 * Playwright test API that bootstraps the app after suite and test hooks have
 * registered their init scripts, but before each test body can use the page.
 */
export const test = withAutomaticBootstrap(baseTest) as typeof baseTest;
