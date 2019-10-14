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

import { CommitTransactionResult, ExecuteStatementResult } from "aws-sdk/clients/qldbsession";
import { makeBinaryWriter, toBase64, Writer } from "ion-js";

import { Communicator } from "./Communicator";
import { ClientException, isOccConflictException, TransactionClosedError } from "./errors/Errors";
import { warn } from "./logUtil";
import { QldbHash } from "./QldbHash";
import { Result } from "./Result";
import { ResultStream } from "./ResultStream";

/**
 * A class representing a QLDB transaction.
 *
 * Every transaction is tied to a parent (Pooled)QldbSession, meaning that if the parent session is closed or
 * invalidated, the child transaction is automatically closed and cannot be used. Only one transaction can be active at
 * any given time per parent session, and thus every transaction should call {@linkcode Transaction.abort} or 
 * {@linkcode Transaction.commit} when it is no longer needed, or when a new transaction is desired from the parent 
 * session.
 *
 * An InvalidSessionException indicates that the parent session is dead, and a new transaction cannot be created
 * without a new (Pooled)QldbSession being created from the parent driver.
 *
 * Any unexpected errors that occur within a transaction should not be retried using the same transaction, as the state
 * of the transaction is now ambiguous.
 *
 * When an OCC conflict occurs, the transaction is closed and must be handled manually by creating a new transaction
 * and re-executing the desired queries.
 *
 * Child {@linkcode ResultStream} objects will be closed when this transaction is aborted or committed.
 */
export class Transaction {
    _communicator: Communicator;
    _txnId: string;
    _isClosed: boolean;
    _resultStreams: ResultStream[];
    _registeredParameters: Writer[];
    _txnHash: QldbHash;

    /**
     * Create a Transaction.
     * @param communicator The Communicator object representing a communication channel with QLDB.
     * @param txnId The ID of the transaction.
     */
    constructor(communicator: Communicator, txnId: string) {
        this._communicator = communicator;
        this._txnId = txnId;
        this._isClosed = false;
        this._resultStreams = [];
        this._registeredParameters = [];
        this._txnHash = QldbHash.toQldbHash(txnId);
    }

    /**
     * Abort this transaction and close child ResultStream objects. No-op if already closed by commit or previous abort.
     * @returns Promise which fulfills with void.
     */
    async abort(): Promise<void> {
        if (this._isClosed) {
            return;
        }
        this._internalClose();
        await this._communicator.abortTransaction();
    }

    /**
     * Commits and closes child ResultStream objects.
     * @returns Promise which fulfills with void.
     * @throws {@linkcode TransactionClosedError} when this transaction is closed.
     * @throws {@linkcode ClientException} when the commit digest from commit transaction result does not match.
     */
    async commit(): Promise<void> {
        if (this._isClosed) {
            throw new TransactionClosedError();
        }
        try {
            let commitTxnResult: CommitTransactionResult = await this._communicator.commit(
                this._txnId, 
                this._txnHash.getQldbHash()
            );
            if (toBase64(this._txnHash.getQldbHash()) !== toBase64(<Uint8Array>(commitTxnResult.CommitDigest))) {
                throw new ClientException(
                    `Transaction's commit digest did not match returned value from QLDB. 
                    Please retry with a new transaction. Transaction ID: ${this._txnId}`
                );
            }
            this._isClosed = true;
        } catch (e) {
            if (isOccConflictException(e)) {
                throw e;
            }
            try {
                await this._communicator.abortTransaction();
            } catch (e2) {
                warn("Ignored error aborting transaction after a failed commit: " + e2);
            }
            throw e;
        } finally {
            this._internalClose();
        }
    }

    /**
     * Execute the specified statement in the current transaction.
     * @param statement A statement to execute against QLDB as a string.
     * @returns Promise which fulfills with a fully-buffered Result.
     */
    async executeInline(statement: string): Promise<Result> {
        let result: ExecuteStatementResult = await this._sendExecute(statement);
        let inlineResult = Result.create(this._txnId, result.FirstPage, this._communicator);
        return inlineResult;
    }

    /**
     * Execute the specified statement in the current transaction.
     * @param statement A statement to execute against QLDB as a string.
     * @returns Promise which fulfills with a ResultStream.
     */
    async executeStream(statement: string): Promise<ResultStream> {
        let result: ExecuteStatementResult = await this._sendExecute(statement);
        let resultStream = new ResultStream(this._txnId, result.FirstPage, this._communicator);
        this._resultStreams.push(resultStream);
        return resultStream;
    }

    /**
     * Retrieve the transaction ID associated with this transaction.
     * @returns The transaction ID.
     */
    getTransactionId(): string {
        return this._txnId;
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
        if (paramNumber < 1) {
            throw new ClientException("The 1-based parameter number cannot be less than 1.");
        }
        let ionWriter: Writer = makeBinaryWriter();
        this._registeredParameters[paramNumber-1] = ionWriter;
        return ionWriter;
    }

    /**
     * As parameters are allowed to be stored non-sequentially, checks if there any gaps
     * in the list of registered parameters.
     * @returns A string of missing parameters, if any.
     */
    private _findMissingParams(): string {
        let missingParams: string = "";
        for (var paramNum = 0; paramNum < this._registeredParameters.length; paramNum++) {
            if (typeof this._registeredParameters[paramNum] === 'undefined') {
                if (missingParams === "") {
                    missingParams += (paramNum + 1);
                } else {
                    missingParams = missingParams + ", " + (paramNum + 1);
                }
            }
        }
        return missingParams;
    }

    /**
     * Mark the transaction as closed, and stop streaming for any ResultStream objects.
     */
    private _internalClose() {
        this._isClosed = true;
        while (this._resultStreams.length !== 0) {
            this._resultStreams.pop().close();
        }
    }

    /**
     * Helper method to execute statement against QLDB. When an execution is successful, the list of registered 
     * parameters is cleared. Parameters must then be registered for the next execution.
     * @param statement A statement to execute against QLDB as a string.
     * @returns Promise which fulfills with a ExecuteStatementResult object.
     * @throws {@linkcode TransactionClosedError} when transaction is closed.
     * @throws {@linkcode ClientException} when there are missing parameters.
     */
    private async _sendExecute(statement: string): Promise<ExecuteStatementResult> {
        if (this._isClosed) {
            throw new TransactionClosedError();
        }
    	let missingParams: string = this._findMissingParams();
        if (missingParams !== "") {
            throw new ClientException(`Parameter #${missingParams} is/are missing.`);
        }
        this._updateHash(statement, this._registeredParameters);
        let result: ExecuteStatementResult = await this._communicator.executeStatement(this._txnId, statement, 
            this._registeredParameters);
        this._registeredParameters = [];
        return result;
    }

    /**
     * Update the transaction hash given the statement and parameters for an execute statement.
     * @param statement The statement to update the hash on.
     * @param parameters The parameters to update the hash on.
     */
    private _updateHash(statement: string, parameters: Writer[]): void {
        let statementHash: QldbHash = QldbHash.toQldbHash(statement);
        for (var i = 0; i < parameters.length; i++) {
            statementHash = statementHash.dot(QldbHash.toQldbHash(parameters[i]));
        }
        this._txnHash = this._txnHash.dot(statementHash);
    }
}
