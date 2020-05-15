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

/**
 * Represents a session to a specific ledger within QLDB, allowing for execution of PartiQL statements and
 * retrieval of the associated results, along with control over transactions for bundling multiple executions.
 *
 * The execute methods provided will automatically retry themselves in the case that an unexpected recoverable error
 * occurs, including OCC conflicts, by starting a brand new transaction and re-executing the statement within the new
 * transaction.
 *
 * There are three methods of execution, ranging from simple to complex; the first two are recommended for inbuilt
 * error handling:
 *  - {@linkcode QldbSession.executeStatement} allows for a single statement to be executed within a transaction
 *    where the transaction is implicitly created and committed, and any recoverable errors are transparently handled.
 *  - {@linkcode QldbSession.executeLambda} allow for more complex execution sequences where more than one
 *    execution can occur, as well as other method calls. The transaction is implicitly created and committed, and any
 *    recoverable errors are transparently handled.
 *  - {@linkcode QldbSession.startTransaction} allows for full control over when the transaction is committed and
 *    leaves the responsibility of OCC conflict handling up to the user. Transactions' methods cannot be automatically
 *    retried, as the state of the transaction is ambiguous in the case of an unexpected error.
 */
export class QldbSession {
    private _communicator: Communicator;
    private _retryLimit: number;
    public _isClosed: boolean;

    /**
     * Creates a QldbSession.
     * @param communicator The Communicator object representing a communication channel with QLDB.
     * @param retryLimit The limit for retries on execute methods when an OCC conflict or retriable exception occurs.
     */
    constructor(communicator: Communicator, retryLimit: number) {
        this._communicator = communicator;
        this._retryLimit = retryLimit;
        this._isClosed = false;
    }

    /**
     * Close this session. No-op if already closed.
     */
    close(): void {
        if (this._isClosed) {
            return;
        }
        this._isClosed = true;
        this._communicator.endSession();
    }

    /**
     * Implicitly start a transaction, execute the lambda, and commit the transaction, retrying up to the
     * retry limit if an OCC conflict or retriable exception occurs.
     *
     * @param transactionLambda lambda representing the block of code to be executed within the transaction. This cannot
     *                    have any side effects as it may be invoked multiple times, and the result cannot be trusted
     *                    until the transaction is committed.
     * @param retryIndicator An optional lambda that is invoked when the `querylambda` is about to be retried due to an
     *                       OCC conflict or retriable exception.
     * @returns Promise which fulfills with the return value of the `queryLambda` which could be a {@linkcode Result}
     *          on the result set of a statement within the lambda.
     * @throws {@linkcode SessionClosedError} when this session is closed.
     */
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
                if (e instanceof StartTransactionError || isInvalidSessionException(e)) {
                    this._isClosed = true;
                    throw e;
                }

                await this._noThrowAbort(transaction);
                if (retryAttempt >= this._retryLimit || e instanceof LambdaAbortedError) {
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

    /**
     * Return the name of the ledger for the session.
     * @returns Returns the name of the ledger as a string.
     */
    getLedgerName(): string {
        return this._communicator.getLedgerName();
    }

    /**
     * Returns the token for this session.
     * @returns Returns the session token as a string.
     */
    getSessionToken(): string {
        return this._communicator.getSessionToken();
    }


    /**
     * Start a transaction using an available database session.
     * @returns Promise which fulfills with a transaction object.
     * @throws {@linkcode SessionClosedError} when this session is closed.
     */
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

    /**
     * Send an abort request which will not throw on failure.
     * @param transaction The transaction to abort.
     * @returns Promise which fulfills with void.
     */
    private async _noThrowAbort(transaction: Transaction): Promise<void> {
        try {
            if (transaction) {
                await transaction.abort();
            }
        } catch (e) {
            warn(`Ignored error while aborting transaction during execution: ${e}.`);
        }
    }

    /**
     * Sleeps an exponentially increasing amount relative to `attemptNumber`.
     * @param attemptNumber The attempt number for the retry, used for the exponential portion of the sleep.
     * @returns Promise which fulfills with void.
     */
    private async _retrySleep(attemptNumber: number): Promise<void> {
        const jitterRand: number = Math.random();
        const exponentialBackoff: number = Math.min(SLEEP_CAP_MS, Math.pow(SLEEP_BASE_MS, attemptNumber));
        const sleep = (milliseconds: number) => {
            return new Promise(resolve => setTimeout(resolve, milliseconds));
        };
        (async() => {
            await sleep(jitterRand * (exponentialBackoff + 1));
        })();

    }


    /**
     * Check and throw if this session is closed.
     * @throws {@linkcode SessionClosedError} when this session is closed.
     */
    private _throwIfClosed(): void {
        if (this._isClosed) {
            throw new SessionClosedError();
        }
    }
}
