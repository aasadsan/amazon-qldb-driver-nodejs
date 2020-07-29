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
import { RetryPolicy } from "./retry/RetryPolicy";
import { Transaction } from "./Transaction";
import { TransactionExecutor } from "./TransactionExecutor";
import { TransactionExecutionContext } from "./TransactionExecutionContext";
import { BackoffFunction } from "./retry/BackoffFunction";

export class QldbSession {
    private _communicator: Communicator;
    private _isClosed: boolean;

    constructor(communicator: Communicator) {
        this._communicator = communicator;
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
        retryPolicy: RetryPolicy,
        executionContext: TransactionExecutionContext,
    ): Promise<any> {
        let transaction: Transaction;
        while (true) {
            transaction = null;
            try {
                transaction = await this.startTransaction();
                const transactionExecutor = new TransactionExecutor(transaction);
                let returnedValue: any = await transactionLambda(transactionExecutor);
                if (returnedValue instanceof ResultStream) {
                    returnedValue = await Result.bufferResultStream(returnedValue);
                }
                await transaction.commit();
                return returnedValue;
            } catch (e) {
                if (isInvalidSessionException(e)) {
                    this.closeSession();
                    throw e;
                }

                executionContext.setLastException(e);
                this._noThrowAbort(transaction);

                if (e instanceof LambdaAbortedError) {
                    throw e;
                }

                if (e instanceof StartTransactionError) {
                    if (executionContext.getExecutionAttempt() >= retryPolicy.getRetryLimit()) {
                        throw e.cause;
                    }
                }

                if (executionContext.getExecutionAttempt() >= retryPolicy.getRetryLimit()) {
                    throw e;
                }
                if (isOccConflictException(e) || isRetriableException(e)) {
                    warn(`OCC conflict or retriable exception occurred: ${e}.`);
                } else {
                    throw e;
                }
            }
            executionContext.incrementExecutionAttempt();
            await this._retrySleep(executionContext, retryPolicy, transaction);
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
            throw new StartTransactionError(e);
        }
    }

    private async _noThrowAbort(transaction: Transaction): Promise<void> {
        try {
            if (null == transaction) {
                this._communicator.abortTransaction();
            } else {
                await transaction.abort();
            }
        } catch (e) {
            warn(`Ignored error while aborting transaction during execution: ${e}.`);
        }
    }

    private _retrySleep(executionContext: TransactionExecutionContext, retryPolicy: RetryPolicy, transaction: Transaction) {
        let transactionId: string = (transaction != null) ? transaction.getTransactionId() : null;
        const backoffFunction: BackoffFunction = retryPolicy.getBackOffFunction();
        let backoffDelay: number = backoffFunction(executionContext.getExecutionAttempt(), executionContext.getLastException(), transactionId);
        if (backoffDelay == null || backoffDelay < 0) {
            backoffDelay = 0;
        }
        return this._sleep(backoffDelay);
    }

    private _sleep(sleepTime:number) {
        return new Promise(resolve => setTimeout(resolve, sleepTime));
    }


    private _throwIfClosed(): void {
        if (this._isClosed) {
            throw new SessionClosedError();
        }
    }
}
