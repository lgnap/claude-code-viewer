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

  /**
   * Admit a password attempt. Admission and counting happen in one atomic update so that
   * concurrent attempts cannot all slip through before any failure is recorded: an admitted
   * attempt is provisionally counted as a failure until `recordSuccess` releases it.
   * Refused attempts are not counted, so hammering a locked service does not extend the delay.
   */
  const beginAttempt: Effect.Effect<ThrottleCheck> = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const [result, engagedLockout] = yield* Ref.modify(
      stateRef,
      (current): [[ThrottleCheck, boolean], ThrottleState] => {
        const state = isExpired(current, now) ? initialState : current;
        const retryAfterMs = state.lastFailureAt + delayFor(state.failures) - now;
        if (retryAfterMs > 0) {
          return [[{ allowed: false, retryAfterMs }, false], state];
        }
        const failures = state.failures + 1;
        return [[{ allowed: true }, failures === FREE_FAILURES], { failures, lastFailureAt: now }];
      },
    );

    if (engagedLockout) {
      yield* Effect.logWarning(
        `Authentication lockout engaged after ${FREE_FAILURES} consecutive failed password attempts`,
      );
    }
    return result;
  });

  const recordSuccess = Ref.set(stateRef, initialState);

  return {
    beginAttempt,
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
