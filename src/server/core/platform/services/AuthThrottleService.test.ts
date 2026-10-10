import { Effect, Layer, TestClock, TestContext } from "effect";
import { describe, expect, test } from "vitest";
import { AuthThrottleService } from "./AuthThrottleService.ts";

const testLayer = Layer.provideMerge(AuthThrottleService.Live, TestContext.TestContext);

const run = <A, E>(effect: Effect.Effect<A, E, AuthThrottleService>) =>
  Effect.runPromise(effect.pipe(Effect.provide(testLayer)));

// Simulates an attacker: an attempt that is admitted and never released counts as a failure;
// when refused, wait out the delay and try again.
const recordFailures = (count: number) =>
  Effect.gen(function* () {
    const service = yield* AuthThrottleService;
    for (let i = 0; i < count; i++) {
      let result = yield* service.beginAttempt;
      while (!result.allowed) {
        yield* TestClock.adjust(result.retryAfterMs);
        result = yield* service.beginAttempt;
      }
    }
  });

describe("AuthThrottleService", () => {
  test("allows attempts before the failure threshold is reached", async () => {
    const result = await run(
      Effect.gen(function* () {
        const service = yield* AuthThrottleService;
        yield* recordFailures(4);
        return yield* service.beginAttempt;
      }),
    );

    expect(result).toEqual({ allowed: true });
  });

  test("blocks for 1 second after the 5th failure", async () => {
    const result = await run(
      Effect.gen(function* () {
        const service = yield* AuthThrottleService;
        yield* recordFailures(5);
        return yield* service.beginAttempt;
      }),
    );

    expect(result).toEqual({ allowed: false, retryAfterMs: 1000 });
  });

  test("doubles the delay on each subsequent failure", async () => {
    const result = await run(
      Effect.gen(function* () {
        const service = yield* AuthThrottleService;
        yield* recordFailures(7);
        return yield* service.beginAttempt;
      }),
    );

    expect(result).toEqual({ allowed: false, retryAfterMs: 4000 });
  });

  test("caps the delay at 5 minutes", async () => {
    const result = await run(
      Effect.gen(function* () {
        const service = yield* AuthThrottleService;
        yield* recordFailures(30);
        return yield* service.beginAttempt;
      }),
    );

    expect(result).toEqual({ allowed: false, retryAfterMs: 5 * 60 * 1000 });
  });

  test("allows again once the delay has elapsed", async () => {
    const result = await run(
      Effect.gen(function* () {
        const service = yield* AuthThrottleService;
        yield* recordFailures(5);
        yield* TestClock.adjust("1 second");
        return yield* service.beginAttempt;
      }),
    );

    expect(result).toEqual({ allowed: true });
  });

  test("reports the remaining delay when checked mid-lockout", async () => {
    const result = await run(
      Effect.gen(function* () {
        const service = yield* AuthThrottleService;
        yield* recordFailures(6);
        yield* TestClock.adjust("500 millis");
        return yield* service.beginAttempt;
      }),
    );

    expect(result).toEqual({ allowed: false, retryAfterMs: 1500 });
  });

  test("refused attempts do not extend the delay", async () => {
    const result = await run(
      Effect.gen(function* () {
        const service = yield* AuthThrottleService;
        yield* recordFailures(5);
        yield* service.beginAttempt;
        yield* service.beginAttempt;
        yield* TestClock.adjust("1 second");
        return yield* service.beginAttempt;
      }),
    );

    expect(result).toEqual({ allowed: true });
  });

  test("admits at most the free attempts when they arrive concurrently", async () => {
    const results = await run(
      Effect.gen(function* () {
        const service = yield* AuthThrottleService;
        return yield* Effect.all(
          Array.from({ length: 20 }, () => service.beginAttempt),
          { concurrency: "unbounded" },
        );
      }),
    );

    expect(results.filter((result) => result.allowed)).toHaveLength(5);
  });

  test("resets the counter on success", async () => {
    const result = await run(
      Effect.gen(function* () {
        const service = yield* AuthThrottleService;
        yield* recordFailures(4);
        yield* service.recordSuccess;
        yield* recordFailures(4);
        return yield* service.beginAttempt;
      }),
    );

    expect(result).toEqual({ allowed: true });
  });

  test("forgets failures older than 15 minutes", async () => {
    const result = await run(
      Effect.gen(function* () {
        const service = yield* AuthThrottleService;
        yield* recordFailures(4);
        yield* TestClock.adjust("15 minutes");
        yield* recordFailures(4);
        return yield* service.beginAttempt;
      }),
    );

    expect(result).toEqual({ allowed: true });
  });
});
