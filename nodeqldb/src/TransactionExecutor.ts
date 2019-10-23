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

import { Writer } from "ion-js";

import { LambdaAbortedError } from "./errors/Errors";
import { Result } from "./Result";
import { ResultStream } from "./ResultStream";
import { Transaction } from "./Transaction";

/**
 * A class to handle lambda execution.
 */
export class TransactionExecutor {
    _transaction: Transaction;

    /**
     * Creates a TransactionExecutor.
     * @param transaction The transaction that this executor is running within.
     */
    constructor(transaction: Transaction) {
        this._transaction = transaction;
    }

    /**
     * Abort the transaction and roll back any changes.
     * @throws {@linkcode LambdaAbortedError} when called.
     */
    abort(): void {
        throw new LambdaAbortedError();
    }

    /**
     * Clear the registered parameters for this transaction.
     */
    clearParameters(): void {
        this._transaction.clearParameters();
    }

    /**
     * Execute the specified statement in the current transaction.
     * @param statement The statement to execute.
     * @returns Promise which fulfills with a Result.
     * @throws {@linkcode TransactionClosedError} when the transaction is closed.
     */
    async executeInline(statement: string): Promise<Result> {
        return await this._transaction.executeInline(statement);
    }

    /**
     * Execute the specified statement in the current transaction.
     * @param statement The statement to execute.
     * @returns Promise which fulfills with a ResultStream.
     * @throws {@linkcode TransactionClosedError} when the transaction is closed.
     */
    async executeStream(statement: string): Promise<ResultStream> {
        return await this._transaction.executeStream(statement);
    }

    /**
    * Get the transaction ID.
    * @returns The transaction ID.
    */
    getTransactionId(): string {
        return this._transaction.getTransactionId();
    }

    /**
     * Create a writer for a parameter.
     *
     * Each transaction tracks the registered parameters to be used for the next execution. When the next execution
     * occurs, the parameters are used and then cleared. Parameters must then be registered for the next execution.
     *
     * Registering a parameter that has already been registered will clear the previously registered writer for that
     * parameter.
     *
     * @param paramNumber Represents the i-th parameter we're registering for the next execution in the transaction.
     *                    paramNumber is a 1-based counter.
     * @returns Binary writer for the parameter.
     * @throws {@linkcode ClientException} when the parameter number is less than 1.
     */
    registerParameter(paramNumber: number): Writer {
        return this._transaction.registerParameter(paramNumber);
    }
}
