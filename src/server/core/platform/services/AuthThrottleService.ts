import { Clock, Context, Effect, Layer, Ref } from "effect";
import type { InferEffect } from "../../../lib/effect/types.ts";

/** Failures allowed before the lockout kicks in. */
const FREE_FAILURES = 5;
/** Delay applied on the first locked-out failure; doubles on each following failure. */
const BASE_DELAY_MS = 1000;
/** Upper bound for the delay. */
const MAX_DELAY_MS = 5 * 60 * 1000;
/** Failures older than this are forgotten. */
const FAILURE_WINDOW_MS = 15 * 60 * 1000;

type ThrottleState = {
  failures: number;
  lastFailureAt: number;
};

const initialState: ThrottleState = { failures: 0, lastFailureAt: 0 };

export type ThrottleCheck = { allowed: true } | { allowed: false; retryAfterMs: number };

const delayFor = (failures: number): number => {
  if (failures < FREE_FAILURES) return 0;
  return Math.min(BASE_DELAY_MS * 2 ** (failures - FREE_FAILURES), MAX_DELAY_MS);
};

const isExpired = (state: ThrottleState, now: number): boolean =>
  state.failures > 0 && now - state.lastFailureAt >= FAILURE_WINDOW_MS;

const LayerImpl = Effect.gen(function* () {
  const stateRef = yield* Ref.make<ThrottleState>(initialState);

  const check: Effect.Effect<ThrottleCheck> = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const state = yield* Ref.get(stateRef);
    if (isExpired(state, now)) return { allowed: true };

    const retryAfterMs = state.lastFailureAt + delayFor(state.failures) - now;
    return retryAfterMs > 0 ? { allowed: false, retryAfterMs } : { allowed: true };
  });

  const recordFailure = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const state = yield* Ref.updateAndGet(stateRef, (current) => ({
      failures: (isExpired(current, now) ? 0 : current.failures) + 1,
      lastFailureAt: now,
    }));

    if (state.failures === FREE_FAILURES) {
      yield* Effect.logWarning(
        `Authentication lockout engaged after ${state.failures} consecutive failed password attempts`,
      );
    }
  });

  const recordSuccess = Ref.set(stateRef, initialState);

  return {
    check,
    recordFailure,
    recordSuccess,
  };
});

export type IAuthThrottleService = InferEffect<typeof LayerImpl>;

export class AuthThrottleService extends Context.Tag("AuthThrottleService")<
  AuthThrottleService,
  IAuthThrottleService
>() {
  static Live = Layer.effect(this, LayerImpl);
}
