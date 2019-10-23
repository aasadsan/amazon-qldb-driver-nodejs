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

import { makeBinaryWriter, Writer } from "ion-js";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import { Result } from "../Result";
import { ResultStream } from "../ResultStream";
import { Transaction } from "../Transaction";
import { TransactionExecutor } from "../TransactionExecutor";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testStatement: string = "SELECT * FROM foo";
const testMessage: string = "foo";
const testTransactionId: string = "txnId";
const testWriter: Writer = makeBinaryWriter();
testWriter.writeString(testMessage);

const mockResult: Result = <Result><any> sandbox.mock(Result);
const mockResultStream: ResultStream = <ResultStream><any> sandbox.mock(ResultStream);
const mockTransaction: Transaction = <Transaction><any> sandbox.mock(Transaction);

let transactionExecutor: TransactionExecutor;

describe("TransactionExecutor test", () => {

    beforeEach(() => {
        transactionExecutor = new TransactionExecutor(mockTransaction);
    });

    afterEach(() => {
        sandbox.restore();
    });

    it("Test abort", () => {
        chai.expect(() => {
            transactionExecutor.abort();
        }).to.throw();
    });

    it("Test clearParameters", () => {
        mockTransaction.clearParameters = () => {};
        const transactionClearSpy = sandbox.spy(mockTransaction, "clearParameters");
        transactionExecutor.clearParameters();
        sinon.assert.calledOnce(transactionClearSpy);
    });

    it("Test executeInline", async () => {
        mockTransaction.executeInline = async () => {
            return mockResult;
        };
        const transactionExecuteSpy = sandbox.spy(mockTransaction, "executeInline");
        const result = await transactionExecutor.executeInline(testStatement);
        chai.assert.equal(mockResult, result);
        sinon.assert.calledOnce(transactionExecuteSpy);
        sinon.assert.calledWith(transactionExecuteSpy, testStatement);
    });

    it("Test executeInline with exception", async () => {
        mockTransaction.executeInline = async () => {
            throw new Error(testMessage);
        };
        const transactionExecuteSpy = sandbox.spy(mockTransaction, "executeInline");
        const errorMessage = await chai.expect(transactionExecutor.executeInline(testStatement)).to.be.rejected;
        chai.assert.equal(errorMessage.name, "Error");
        sinon.assert.calledOnce(transactionExecuteSpy);
        sinon.assert.calledWith(transactionExecuteSpy, testStatement);
    });

    it("Test executeStream", async () => {
        mockTransaction.executeStream = async () => {
            return mockResultStream;
        };
        const transactionExecuteSpy = sandbox.spy(mockTransaction, "executeStream");
        const resultStream = await transactionExecutor.executeStream(testStatement);
        chai.assert.equal(mockResultStream, resultStream);
        sinon.assert.calledOnce(transactionExecuteSpy);
        sinon.assert.calledWith(transactionExecuteSpy, testStatement);
    });

    it("Test executeStream with exception", async () => {
        mockTransaction.executeStream = async () => {
            throw new Error(testMessage);
        };
        const transactionExecuteSpy = sandbox.spy(mockTransaction, "executeStream");
        const errorMessage = await chai.expect(transactionExecutor.executeStream(testStatement)).to.be.rejected;
        chai.assert.equal(errorMessage.name, "Error");
        sinon.assert.calledOnce(transactionExecuteSpy);
        sinon.assert.calledWith(transactionExecuteSpy, testStatement);
    });

    it("Test getTransactionId", async () => {
        mockTransaction.getTransactionId = () => {
            return testTransactionId;
        };
        const transactionIdSpy = sandbox.spy(mockTransaction, "getTransactionId");
        const transactionId = transactionExecutor.getTransactionId();
        chai.assert.equal(transactionId, testTransactionId);
        sinon.assert.calledOnce(transactionIdSpy);
    });

    it("Test registerParameter", async () => {
        mockTransaction.registerParameter = () => {
            return testWriter;
        };
        const registerParamSpy = sandbox.spy(mockTransaction, "registerParameter");
        const writer = transactionExecutor.registerParameter(1);
        chai.assert.equal(writer, testWriter);
        sinon.assert.calledOnce(registerParamSpy);
        sinon.assert.calledWith(registerParamSpy, 1);
    });
});
