/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

import { StartTransactionResult } from "aws-sdk/clients/qldbsession";
import { Communicator } from "./Communicator";
import {
    isInvalidSessionException,
    isOccConflictException,
    isRetriableException,
    LambdaAbortedError,
    SessionClosedError,
    StartTransactionError
} from "./errors/Errors";
import { warn } from "./LogUtil";
import { Result } from "./Result";
import { ResultStream } from "./ResultStream";
import { Transaction } from "./Transaction";
import { TransactionExecutor } from "./TransactionExecutor";

const SLEEP_CAP_MS: number = 5000;
const SLEEP_BASE_MS: number = 10;

export class QldbSession {
    private _communicator: Communicator;
    private _retryLimit: number;
    private _isClosed: boolean;

    constructor(communicator: Communicator, retryLimit: number) {
        this._communicator = communicator;
        this._retryLimit = retryLimit;
        this._isClosed = false;
    }

    endSession(): void {
        if (this._isClosed) {
            return;
        }
        this._isClosed = true;
        this._communicator.endSession();
    }

    closeSession(): void {
        this._isClosed = true;
    }

    async executeLambda(
        transactionLambda: (transactionExecutor: TransactionExecutor) => any,
        retryIndicator?: (retryAttempt: number) => void
    ): Promise<any> {
        let transaction: Transaction;
        let retryAttempt: number = 0;
        while (true) {
            try {
                transaction = null;
                transaction = await this.startTransaction();
                const transactionExecutor = new TransactionExecutor(transaction);
                let returnedValue: any = await transactionLambda(transactionExecutor);
                if (returnedValue instanceof ResultStream) {
                    returnedValue = await Result.bufferResultStream(returnedValue);
                }
                await transaction.commit();
                return returnedValue;
            } catch (e) {
                if (e instanceof StartTransactionError || e instanceof LambdaAbortedError) {
                    throw e;
                }

                if (isInvalidSessionException(e)) {
                    this.closeSession();
                    throw e;
                }

                await this._noThrowAbort(transaction);
                if (retryAttempt >= this._retryLimit) {
                    throw e;
                }
                if (isOccConflictException(e) || isRetriableException(e)) {
                    warn(`OCC conflict or retriable exception occurred: ${e}.`);
                    retryAttempt++;
                    if (retryIndicator !== undefined) {
                        retryIndicator(retryAttempt);
                    }
                    await this._retrySleep(retryAttempt);
                } else {
                    throw e;
                }
            }
        }
    }


    getSessionToken(): string {
        return this._communicator.getSessionToken();
    }

    isSessionOpen(): Boolean {
        return !this._isClosed;
    }

    async startTransaction(): Promise<Transaction> {
        try {
            const startTransactionResult: StartTransactionResult = await this._communicator.startTransaction();
            const transaction: Transaction = new Transaction(
                this._communicator,
                startTransactionResult.TransactionId
            );
            return transaction;
        } catch (e) {
            throw new StartTransactionError();
        }
    }

    private async _noThrowAbort(transaction: Transaction): Promise<void> {
        try {
            if (transaction) {
                await transaction.abort();
            }
        } catch (e) {
            warn(`Ignored error while aborting transaction during execution: ${e}.`);
        }
    }

    private _retrySleep(attemptNumber: number) {
        const delayTime = this._calculateDelayTime(attemptNumber);
        return this._sleep(delayTime);
    }

    private _calculateDelayTime(attemptNumber: number) {
        const exponentialBackoff: number = Math.min(SLEEP_CAP_MS, Math.pow(2,  attemptNumber) * SLEEP_BASE_MS);
        const jitterRand: number = this._getRandomArbitrary(0, (exponentialBackoff/2 + 1));
        const delayTime: number = (exponentialBackoff/2) + jitterRand;
        return delayTime;
    }

    private _sleep(sleepTime:number) {
        return new Promise(resolve => setTimeout(resolve, sleepTime));
    }

    private _getRandomArbitrary(min:number, max:number) {
        return Math.random() * (max - min) + min;
    }

    private _throwIfClosed(): void {
        if (this._isClosed) {
            throw new SessionClosedError();
        }
    }
}
