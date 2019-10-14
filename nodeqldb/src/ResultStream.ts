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

import { Page } from "aws-sdk/clients/qldbsession";
import { makeReader } from "ion-js";
import { Readable } from "stream";

import { Communicator } from "./Communicator";
import { ClientException } from "./errors/Errors";
import { Result } from "./Result";

/**
 * A class representing the result of a statement returned from QLDB as a stream.
 * Extends Readable from the Node.JS Stream API interface.
 * The stream will always operate in object mode.
 */
export class ResultStream extends Readable {
    private _communicator: Communicator;
    private _cachedPage: Page;
    private _txnId: string;
    private _shouldPushCachedPage: boolean;
    private _lastRetrievedIndex: number;
    private _isClosed: boolean;

    /**
     * Create a ResultStream.
     * @param txnId The ID of the transaction the statement was executed in.
     * @param firstPage The initial page returned from the statement execution.
     * @param communicator The Communicator used for the statement execution.
     */
    constructor(txnId: string, firstPage: Page, communicator: Communicator) {
        super({ objectMode: true });
        this._communicator = communicator;
        this._cachedPage = firstPage;
        this._txnId = txnId;
        this._shouldPushCachedPage = true;
        this._lastRetrievedIndex = 0;
        this._isClosed = false;
    }

    /**
     * Close this ResultStream. No-op if already closed.
     */
    close(): void {
        if (this._isClosed) {
            return;
        }
        this._isClosed = true;
    }

    /**
     * Implementation of the `readable.read` method for the Node Streams Readable Interface.
     * @param size The number of bytes to read asynchronously. This is currently not being used as only object mode is 
     * supported.
     * @throws {@linkcode ClientException} when this ResultStream is closed.
     */
    _read(size: number): void {
        if (this._isClosed) {
            throw new ClientException("Result stream is closed. Cannot stream data.");
        }
        let childRead = async () => {
            if (this._shouldPushCachedPage) {
                this._shouldPushCachedPage = false;
            } else {
                this._lastRetrievedIndex = 0;
                do {
                    this._cachedPage = await this._communicator.fetchPage(this._txnId, this._cachedPage.NextPageToken);
                } while (this._cachedPage.NextPageToken !== null && this._cachedPage.Values.length === 0);
            }
            for (let i = this._lastRetrievedIndex; i < this._cachedPage.Values.length; i++) {
                let reader = makeReader(Result._handleBlob(this._cachedPage.Values[i].IonBinary));
                if (!this.push(reader)) {
                    this._lastRetrievedIndex = i + 1;
                    this._shouldPushCachedPage = this._lastRetrievedIndex < this._cachedPage.Values.length;
                    return;
                }
            }
            if (!this._cachedPage.NextPageToken) {
                this.push(null);
                this._isClosed = true;
            }
        };
        childRead();
    }
}
