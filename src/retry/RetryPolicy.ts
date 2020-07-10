import { BackoffFunction } from "./BackoffFunction";
import { defaultBackoffFunction } from "./DefaultRetryPolicy";

export class RetryPolicy {
    private _retryLimit: number;
    private _backoffFunction: BackoffFunction; 

    constructor(retryLimit: number = 4, backoffFunction: BackoffFunction = defaultBackoffFunction) {
        if (retryLimit < 0) {
            throw new RangeError("Value for retryLimit cannot be negative.");
        }
        this._retryLimit = retryLimit;
        this._backoffFunction = backoffFunction;
    }

    getRetryLimit(): number {
        return this._retryLimit;
    }

    getBackOffFunction(): BackoffFunction {
        return this._backoffFunction;
    } 

}