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
import {
    CommitDigest,
    CommitTransactionResult,
    ExecuteStatementResult,
    Page,
    PageToken,
    SendCommandRequest,
    SendCommandResult,
    ValueHolder,
} from "aws-sdk/clients/qldbsession";
import { Writer } from "ion-js";
import { inspect } from "util";

import { debug, warn } from "./logUtil";

/**
 * A class representing an independent session to a QLDB ledger that handles endpoint requests. This class is used in
 * {@linkcode QldbDriver} and {@linkcode QldbSessionImpl}. This class is not meant to be used directly by developers.
 */
export class Communicator {
    private _qldbClient: QLDBSession;
    private _ledgerName: string;
    private _sessionToken: string;

    /**
     * Creates a Communicator.
     * @param qldbClient The low level service client.
     * @param ledgerName The QLDB ledger name.
     * @param sessionToken The initial session token representing the session connection.
     */
    private constructor(qldbClient: QLDBSession, ledgerName: string, sessionToken: string) {
        this._qldbClient = qldbClient;
        this._ledgerName = ledgerName;
        this._sessionToken = sessionToken;
    }

    /**
     * Static factory method that creates a Communicator object.
     * @param qldbClient The low level client that communicates with QLDB.
     * @param ledgerName The QLDB ledger name.
     * @returns Promise which fulfills with a Communicator.
     */
    static async create(qldbClient: QLDBSession, ledgerName: string): Promise<Communicator> {
        const request: SendCommandRequest = {
            StartSession: {
                LedgerName: ledgerName
            }
        };
        const result: SendCommandResult = await qldbClient.sendCommand(request).promise();
        return new Communicator(qldbClient, ledgerName, result.StartSession.SessionToken);
    }

    /**
     * Send request to abort the currently active transaction.
     * @returns Promise which fulfills with void.
     */
    async abortTransaction(): Promise<void> {
        const request: SendCommandRequest = {
            SessionToken: this._sessionToken,
            AbortTransaction: {}
        };
        await this._sendCommand(request);
    }

    /**
     * Send request to commit the currently active transaction.
     * @param txnId The ID of the transaction.
     * @param commitDigest The digest hash of the transaction to commit.
     * @returns Promise which fulfills with the commit transaction response returned from QLDB.
     */
    async commit(txnId: string, commitDigest: CommitDigest): Promise<CommitTransactionResult> {
        const request: SendCommandRequest = {
            SessionToken: this._sessionToken,
            CommitTransaction: {
                TransactionId: txnId,
                CommitDigest: commitDigest
            }
        };
        const result: SendCommandResult = await this._sendCommand(request);
        return result.CommitTransaction;
    }

    /**
     * Send an execute statement request with parameters to QLDB.
     * @param txnId The ID of the transaction.
     * @param statement The statement to execute.
     * @param parameters The parameters of the statement contained in Writers.
     * @returns Promise which fulfills with the execute statement response returned from QLDB.
     */
    async executeStatement(txnId: string, statement: string, parameters: Writer[]): Promise<ExecuteStatementResult> {
        const valueHolder: ValueHolder[] = this._ionToValueHolder(parameters);
        const request: SendCommandRequest = {
            SessionToken: this._sessionToken,
            ExecuteStatement: {
                Statement: statement,
                TransactionId: txnId,
                Parameters: valueHolder
            }
        };
        const result: SendCommandResult = await this._sendCommand(request);
        return result.ExecuteStatement;
    }

    /**
     * Send request to end the independent session represented by the instance of this class.
     * @returns Promise which fulfills with void.
     */
    async endSession(): Promise<void> {
        const request: SendCommandRequest = {
            SessionToken: this._sessionToken,
            EndSession: {}
        };
        try {
            await this._sendCommand(request);
        } catch (e) {
            // We will only log issues ending the session, as QLDB will clean them after a timeout.
            warn(`Errors ending session: ${e}.`);
        }
    }

    /**
     * Send fetch result request to QLDB, retrieving the next chunk of data for the result.
     * @param txnId The ID of the transaction.
     * @param pageToken The token to fetch the next page.
     * @returns Promise which fulfills with the page from the fetch page response.
     */
    async fetchPage(txnId: string, pageToken: PageToken): Promise<Page> {
        const request: SendCommandRequest = {
            SessionToken: this._sessionToken,
            FetchPage: {
                TransactionId: txnId,
                NextPageToken: pageToken
            }
        };
        let result: SendCommandResult = await this._sendCommand(request);
        return result.FetchPage.Page;
    }

    /**
     * Get the QLDB ledger name.
     * @returns The QLDB ledger name.
     */
    getLedgerName(): string {
        return this._ledgerName;
    }

    /**
     * Get the low-level service client that communicates with QLDB.
     * @returns The low-level service client.
     */
    getLowLevelClient(): QLDBSession {
        return this._qldbClient;
    }

    /**
     * Get the session token representing the session connection.
     * @returns The session token.
     */
    getSessionToken(): string {
        return this._sessionToken;
    }

    /**
     * Send a request to start a transaction.
     * @returns Promise which fulfills with the transaction ID.
     */
    async startTransaction(): Promise<string> {
        const request: SendCommandRequest = {
            SessionToken: this._sessionToken,
            StartTransaction: {}
        };
        const result: SendCommandResult = await this._sendCommand(request);
        return result.StartTransaction.TransactionId;
    }

    /**
     * Convert the given list of parameters into an array of ValueHolders.
     * @param parameters List of binary writers for a given query.
     * @returns The parameters converted to array of ValueHolders.
     */
    private _ionToValueHolder(parameters: Writer[]): ValueHolder[] {
        const valueHolderList: ValueHolder[] = [];
        parameters.forEach((writer: Writer) => {
            const byteBuffer: Uint8Array = writer.getBytes();
            const valueHolder: ValueHolder = {
                IonBinary: byteBuffer
            };
            valueHolderList.push(valueHolder);
        });
        return valueHolderList;
    }

    /**
     * Call the sendCommand method of the low level service client.
     * @param request A SendCommandRequest object containing the request information to be sent to QLDB.
     * @returns Promise which fulfills with a SendCommandResult object.
     */
    private async _sendCommand(request: SendCommandRequest): Promise<SendCommandResult> {
        const result: SendCommandResult = await this._qldbClient.sendCommand(request).promise();
        debug(`Received response: ${inspect(result, { depth: 2 })}`);
        return result;
    }
}
