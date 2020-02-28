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

import { QLDBSession } from "aws-sdk";
import { ClientConfiguration } from "aws-sdk/clients/qldbsession";

import { version } from "../package.json";
import { Communicator } from "./Communicator";
import { DriverClosedError } from "./errors/Errors";
import { Executable } from "./Executable.js";
import { debug } from "./LogUtil";
import { QldbSession } from "./QldbSession";
import { QldbSessionImpl } from "./QldbSessionImpl";
import { QldbWriter } from "./QldbWriter.js";
import { Result } from "./Result";
import { TransactionExecutor } from "./TransactionExecutor.js";

/**
 * Represents a factory for creating sessions to a specific ledger within QLDB. This class or
 * {@linkcode PooledQldbDriver} should be the main entry points to any interaction with QLDB.
 * {@linkcode QldbDriver.getSession} will create a {@linkcode QldbSession} to the specified edger within QLDB as a
 * communication channel. Any sessions acquired should be cleaned up with {@linkcode QldbSession.close} to free up
 * resources.
 *
 * This factory does not attempt to re-use or manage sessions in any way. It is recommended to use
 * {@linkcode PooledQldbDriver} for both less resource usage and lower latency.
 */
export class QldbDriver implements Executable {
    protected _qldbClient: QLDBSession;
    protected _ledgerName: string;
    protected _retryLimit: number;
    protected _isClosed: boolean;

    /**
     * Creates a QldbDriver.
     * @param qldbClientOptions The object containing options for configuring the low level client.
     *                          See {@link https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/QLDBSession.html#constructor-details|Low Level Client Constructor}.
     * @param ledgerName The QLDB ledger name.
     * @param retryLimit The number of automatic retries for statement executions using convenience methods on sessions
                         when an OCC conflict or retriable exception occurs. This value must not be negative.
     * @throws RangeError if `retryLimit` is less than 0.
     */
    constructor(ledgerName: string, qldbClientOptions: ClientConfiguration = {}, retryLimit: number = 4) {
        if (retryLimit < 0) {
            throw new RangeError("Value for retryLimit cannot be negative.");
        }
        qldbClientOptions.customUserAgent = `QLDB Driver for Node.js v${version}`;
        qldbClientOptions.maxRetries = 0;

        this._qldbClient = new QLDBSession(qldbClientOptions);
        this._ledgerName = ledgerName;
        this._retryLimit = retryLimit;
        this._isClosed = false;
    }

    /**
     * Close this driver.
     */
    close(): void {
        this._isClosed = true;
    }

    /**
     * Implicitly start a transaction within a new session, execute the lambda, commit the transaction, and close the
     * session, retrying up to the retry limit if an OCC conflict or retriable exception occurs.
     *
     * @param queryLambda A lambda representing the block of code to be executed within the transaction. This cannot
     *                    have any side effects as it may be invoked multiple times, and the result cannot be trusted
     *                    until the transaction is committed.
     * @param retryIndicator An optional lambda that is invoked when the `querylambda` is about to be retried due to an
     *                       OCC conflict or retriable exception.
     * @returns Promise which fulfills with the return value of the `queryLambda` which could be a {@linkcode Result}
     *          on the result set of a statement within the lambda.
     * @throws {@linkcode DriverClosedError} when this driver is closed.
     */
    async executeLambda(
        queryLambda: (transactionExecutor: TransactionExecutor) => any,
        retryIndicator?: (retryAttempt: number) => void
    ): Promise<any> {
        let session: QldbSession = null;
        try  {
            session = await this.getSession();
            return await session.executeLambda(queryLambda, retryIndicator);
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * Implicitly start a transaction within a new session, execute the statement, commit the transaction, and close the
     * session, retrying up to the retry limit if an OCC conflict or retriable exception occurs.
     *
     * @param statement The statement to execute.
     * @param parameters An optional list of QLDB writers containing Ion values to execute.
     * @param retryIndicator An optional lambda that is invoked when the `statement` is about to be retried due to an
     *                       OCC conflict or retriable exception.
     * @returns Promise which fulfills with a Result.
     * @throws {@linkcode DriverClosedError} when this driver is closed.
     */
    async executeStatement(
        statement: string,
        parameters: QldbWriter[] = [],
        retryIndicator?: (retryAttempt: number) => void
    ): Promise<Result> {
        let session: QldbSession = null;
        try  {
            session = await this.getSession();
            return await session.executeStatement(statement, parameters, retryIndicator);
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    /**
     * Create and return a newly instantiated QldbSession object. This will implicitly start a new session with QLDB.
     * @returns Promise which fulfills with a QldbSession.
     * @throws {@linkcode DriverClosedError} when this driver is closed.
     */
    async getSession(): Promise<QldbSession> {
        this._throwIfClosed();
        debug("Creating a new session.");
        const communicator: Communicator = await Communicator.create(this._qldbClient, this._ledgerName);
        return new QldbSessionImpl(communicator, this._retryLimit);
    }

    /**
     * Check and throw if this driver is closed.
     * @throws {@linkcode DriverClosedError} when this driver is closed.
     */
    protected _throwIfClosed(): void {
        if (this._isClosed) {
            throw new DriverClosedError();
        }
    }
}
