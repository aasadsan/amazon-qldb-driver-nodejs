export class TransactionExecutionContext {
    private _transactionExecutionAttempt: number;
    private _lastException: Error;

    constructor() {
        this._transactionExecutionAttempt = 0;
    }

    incrementExecutionAttempt(): void {
        this._transactionExecutionAttempt += 1;
    }

    getExecutionAttempt(): number {
        return this._transactionExecutionAttempt;
    }

    setLastException(ex: Error ): void {
        this._lastException = ex;
    }

    getLastException(): Error {
        return this._lastException;
    }
}