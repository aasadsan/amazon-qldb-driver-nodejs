import { RetryPolicy } from "./RetryPolicy";
import { BackoffFunction } from "./BackoffFunction";

const SLEEP_CAP_MS: number = 5000;
const SLEEP_BASE_MS: number = 10;

export const defaultBackoffFunction: BackoffFunction = (retryAttempt: number, error: Error, transactionId: string) => {
    const exponentialBackoff: number = Math.min(SLEEP_CAP_MS, Math.pow(2,  retryAttempt) * SLEEP_BASE_MS);
    const min: number = 0;
    const max: number = exponentialBackoff/2 + 1;
    const jitterRand: number = Math.random() * (max - min) + min;
    const delayTime: number = (exponentialBackoff/2) + jitterRand;
    return delayTime;
}

export const defaultRetryPolicy: RetryPolicy = new RetryPolicy(4, defaultBackoffFunction);