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

import { Page, ValueHolder} from "aws-sdk/clients/qldbsession";
import { makeReader, Reader} from "ion-js";

import { Communicator } from "../Communicator";
import { Result } from "../Result";
import { ResultStream } from "../ResultStream";

const chai = require("chai");
const chaiAsPromised = require("chai-as-promised");
chai.use(chaiAsPromised);
const ionJs = require('ionJs')
const { Readable } = require("stream");
const sinon = require("sinon");
const sandbox = sinon.createSandbox();

let mockMessage: string = "foo";
let mockNextPageToken: string = "nextPageToken";
let mockTransactionId: string = "txnId";

describe('Result test', function() {
    let mockCommunicator: Communicator = sandbox.mock(Communicator);
    let mockValueHolder: ValueHolder[] = [];
    let mockPage: Page = {Values: mockValueHolder};
    let mockPageWithToken: Page = {Values: mockValueHolder, NextPageToken: mockNextPageToken};

    after(function () {
        sandbox.restore();
    });

    it('Test makeResult Static Factory Method', async function() {
        let result = await Result.create(mockTransactionId, mockPage, mockCommunicator);
        chai.expect(result).to.be.an.instanceOf(Result);
    });

    it('Test makeResult Static Factory Method with Undefined Page Object', async function() {
        let result = await Result.create(mockTransactionId, undefined, mockCommunicator);
        chai.expect(result).to.be.an.instanceOf(Result);
    });

    it('Test makeResult with Exception Thrown from Communicator', async function() {
        mockCommunicator.fetchPage = async function() {throw new Error(mockMessage)};
        await chai.expect(Result.create(mockTransactionId, mockPageWithToken, mockCommunicator)).to.be.rejected;
    });

    it('Test makeBufferedResult Static Factory Method', async function() {
        let mockResultStream: ResultStream = new Readable({
            objectMode: true,
            read: function (size) {
                return this.push(null);
            }
        });
        let result = await Result.bufferResultStream(mockResultStream);
        chai.expect(result).to.be.an.instanceOf(Result);
    });

    it('Test getResultList with makeResult Static Factory Method', async function() {
        let value1: ValueHolder = {IonBinary: "a"};
        let value2: ValueHolder = {IonBinary: "b"};
        let value3: ValueHolder = {IonBinary: "c"};
        let value4: ValueHolder = {IonBinary: "d"};
        let allValues: ValueHolder[] = [value1, value2, value3, value4];

        let finalMockPage: Page = {Values: allValues};
        mockCommunicator.fetchPage = async function() {return finalMockPage};
        let result: Result = await Result.create(mockTransactionId, mockPageWithToken, mockCommunicator);
        let resultList = result.getResultList();

        chai.assert.equal(allValues.length, resultList.length);
        for (var i = 0; i < allValues.length; i++) {
            chai.assert.equal(JSON.stringify(ionJs.makeReader(allValues[i].IonBinary)), JSON.stringify(resultList[i]));
        }
    });

    it('Test getResultList with makeBufferedResult Static Factory Method', async function() {
        let value1: ValueHolder = {IonBinary: "a"};
        let value2: ValueHolder = {IonBinary: "b"};
        let value3: ValueHolder = {IonBinary: "c"};
        let value4: ValueHolder = {IonBinary: "d"};
        let readers: Reader[] = [ionJs.makeReader(value1.IonBinary), 
                                 ionJs.makeReader(value2.IonBinary),
                                 ionJs.makeReader(value3.IonBinary),
                                 ionJs.makeReader(value4.IonBinary)];
        let eventCount = 0;
        let mockResultStream: ResultStream = new Readable({
            objectMode: true,
            read: function (size) {
                if (eventCount < readers.length) {
                    eventCount = eventCount + 1;
                    return this.push(readers[eventCount-1]);
                } else {
                    return this.push(null);
                }
            }
        });

        let result = await Result.bufferResultStream(mockResultStream);
        let resultList = result.getResultList();

        chai.assert.equal(readers.length, resultList.length);
        for (var i = 0; i < readers.length; i++) {
            chai.assert.equal(JSON.stringify(readers[i]), JSON.stringify(resultList[i]));
        }
    });
})