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

import { CommitTransactionResult, ExecuteStatementResult, Page, PageToken } from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import { toBase64, Writer } from "ion-js";
import * as sinon from "sinon";

import { Communicator } from "../Communicator";
import * as Errors from "../errors/Errors";
import * as logUtil from "../logUtil";
import { QldbHash } from "../QldbHash";
import { Result } from "../Result";
import { ResultStream } from "../ResultStream";
import { Transaction } from "../Transaction";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testMessage: string = "foo";
const testMessage2: string = "bar";
const testStatement: string = "SELECT * FROM foo";
const testPageToken: PageToken = "foo";
const testTransactionId: string = "txnId";
const testHash: Uint8Array = new Uint8Array([1, 2, 3]);
const testCommitTransactionResult: CommitTransactionResult = {
    TransactionId: testTransactionId,
    CommitDigest: QldbHash.toQldbHash(testTransactionId).getQldbHash()
};
const pageToken: Page = {NextPageToken: testPageToken};
const testExecuteStatementResult: ExecuteStatementResult = {
    FirstPage: {
        NextPageToken: testPageToken
    }
};

const mockCommunicator: Communicator = <Communicator><any> sandbox.mock(Communicator);
const mockResult: Result = <Result><any> sandbox.mock(Result);

let transaction: Transaction;

describe("Transaction test", () => {

    beforeEach(() => {
        transaction = new Transaction(mockCommunicator, testTransactionId);
        mockCommunicator.executeStatement = async () => {
            return testExecuteStatementResult;
        };
        mockCommunicator.commit = async () => {
            return testCommitTransactionResult;
        };
        mockCommunicator.abortTransaction = async () => {};
    });

    afterEach(() => {
        sandbox.restore();
    });

    it("Test abort", async () => {
        const abortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
        const transactionInternalCloseSpy = sandbox.spy(transaction as any, "_internalClose");
        await transaction.abort();
        await transaction.abort();
        sinon.assert.calledOnce(abortSpy);
        sinon.assert.calledOnce(transactionInternalCloseSpy);
    });

    it("Test abort with exception", async () => {
        mockCommunicator.abortTransaction = async () => {
            throw new Error(testMessage);
        };
        const abortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
        await chai.expect(transaction.abort()).to.be.rejected;
        sinon.assert.calledOnce(abortSpy);
    });

    it("Test abort after commit", async () => {
        const abortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
        await transaction.commit();
        // This should be a no-op.
        await transaction.abort();
        sinon.assert.notCalled(abortSpy);
    });

    it("Test clearParameters", async () => {
        const writer: Writer = transaction.registerParameter(1);
        const closeSpy = sandbox.spy(writer, "close");
        chai.assert.equal(transaction["_registeredParameters"].length, 1);
        transaction.clearParameters();
        chai.assert.equal(transaction["_registeredParameters"].length, 0);
        sinon.assert.calledOnce(closeSpy);
    });

    it("Test clearParameters", async () => {
        const writer: Writer = transaction.registerParameter(1);
        writer.close = function() {
            throw new Error(testMessage);
        };
        const warnSpy = sandbox.spy(logUtil, "warn");
        chai.assert.equal(transaction["_registeredParameters"].length, 1);
        transaction.clearParameters();
        chai.assert.equal(transaction["_registeredParameters"].length, 0);
        sinon.assert.calledOnce(warnSpy);
    });

    it("Test commit", async () => {
        const commitSpy = sandbox.spy(mockCommunicator, "commit");
        const transactionInternalCloseSpy = sandbox.spy(transaction as any, "_internalClose");
        await transaction.commit();
        sinon.assert.calledOnce(commitSpy);
        sinon.assert.calledOnce(transactionInternalCloseSpy);
    });

    it("Test commit after commit", async () => {
        const commitSpy = sandbox.spy(mockCommunicator, "commit");
        await transaction.commit();
        await chai.expect(transaction.commit()).to.be.rejected;
        sinon.assert.calledOnce(commitSpy);
    });

    it("Test commit with non-matching hash", async () => {
        const invalidHashCommitTransactionResult: CommitTransactionResult = {
            TransactionId: testTransactionId,
            CommitDigest: testHash
        };
        mockCommunicator.commit = async () => {
            return invalidHashCommitTransactionResult;
        };
        const commitSpy = sandbox.spy(mockCommunicator, "commit");
        const error = await chai.expect(transaction.commit()).to.be.rejected;
        chai.assert.equal(error.name, "ClientException");
        sinon.assert.calledOnce(commitSpy);
    });

    it("Test commit with exception", async () => {
        mockCommunicator.commit = async () => {
            throw new Error(testMessage);
        };
        const isOccStub = sandbox.stub(Errors, "isOccConflictException");
        isOccStub.returns(false);
        const commitSpy = sandbox.spy(mockCommunicator, "commit");
        await chai.expect(transaction.commit()).to.be.rejected;
        sinon.assert.calledOnce(isOccStub);
        sinon.assert.calledOnce(commitSpy);
        isOccStub.restore();
    });

    it("Test commit with isOccConflictException as true", async () => {
        mockCommunicator.commit = async () => {
            throw new Error(testMessage);
        };
        const isOccStub = sandbox.stub(Errors, "isOccConflictException");
        isOccStub.returns(true);
        const commitSpy = sandbox.spy(mockCommunicator, "commit");
        await chai.expect(transaction.commit()).to.be.rejected;
        sinon.assert.calledOnce(commitSpy);
        sinon.assert.calledOnce(isOccStub);
        isOccStub.restore();
    });

    it("Test commit with isOccConflictException as false", async () => {
        mockCommunicator.commit = async () => {
            throw new Error(testMessage);
        };
        const isOccStub = sandbox.stub(Errors, "isOccConflictException");
        isOccStub.returns(false);
        const commitSpy = sandbox.spy(mockCommunicator, "commit");
        const abortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
        await chai.expect(transaction.commit()).to.be.rejected;
        sinon.assert.calledOnce(commitSpy);
        sinon.assert.calledOnce(abortSpy);
        sinon.assert.calledOnce(isOccStub);
        isOccStub.restore();
    });

    it("Test commit with non-OccConflictException exception and abortTransaction throws error", async () => {
        mockCommunicator.commit = async () => {
            throw new Error("mockMessage");
        };
        mockCommunicator.abortTransaction = async () => {
            throw new Error("foo2");
        };
        const isOccStub = sandbox.stub(Errors, "isOccConflictException");
        const logSpy = sandbox.spy(logUtil, "warn");
        isOccStub.returns(false);
        const commitSpy = sandbox.spy(mockCommunicator, "commit");
        const abortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
        await chai.expect(transaction.commit()).to.be.rejected;
        sinon.assert.calledOnce(commitSpy);
        sinon.assert.calledOnce(abortSpy);
        sinon.assert.calledOnce(isOccStub);
        sinon.assert.calledOnce(logSpy);
        isOccStub.restore();
    });

    it("Test executeInline with no parameters", async () => {
        Result.create = async () => {
            return mockResult
        };
        const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
        const result: Result = await transaction.executeInline(testStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, []);
        chai.assert.equal(result, mockResult);
    });

    it("Test executeInline with parameters", async () => {
        Result.create = async () => {
            return mockResult
        };
        const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
        const writer1: Writer = transaction.registerParameter(1);
        writer1.writeString(testMessage);
        const writer2: Writer = transaction.registerParameter(2);
        writer2.writeString(testMessage2);
        const result: Result = await transaction.executeInline(testStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, [writer1, writer2]);
        chai.assert.equal(result, mockResult);
    });

    it("Test executeInline with updated parameter", async () => {
        Result.create = async () => {
            return mockResult
        };
        const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
        const writer1: Writer = transaction.registerParameter(1);
        writer1.writeString(testMessage);
        const updatedWriter1: Writer = transaction.registerParameter(1);
        updatedWriter1.writeString(testMessage2);
        const result: Result = await transaction.executeInline(testStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, [updatedWriter1]);
        chai.assert.equal(result, mockResult);
    });

    it("Test executeInline with non-sequential parameter registration", async () => {
        Result.create = async () => {
            return mockResult
        };
        const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
        const writer2: Writer = transaction.registerParameter(2);
        writer2.writeString(testMessage);
        const writer1: Writer = transaction.registerParameter(1);
        writer1.writeString(testMessage2);
        const result: Result = await transaction.executeInline(testStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, [writer1, writer2]);
        chai.assert.equal(result, mockResult);
    });

    it("Test executeInline with missing parameters", async () => {
        const writer1: Writer = transaction.registerParameter(1);
        writer1.writeString(testMessage);
        const writer3: Writer = transaction.registerParameter(3);
        writer3.writeString(testMessage2);
        await chai.expect(transaction.executeInline(testStatement)).to.be.rejected;
    });

    it("Test executeInline with exception", async () => {
        mockCommunicator.executeStatement = async () => {
            throw new Error(testMessage);
        };
        const isOccStub = sandbox.stub(Errors, "isOccConflictException");
        isOccStub.returns(false);
        const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
        await chai.expect(transaction.executeInline(testStatement)).to.be.rejected;
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, []);
        isOccStub.restore();
    });

    it("Test executeInline twice", async () => {
        const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
        await transaction.executeInline(testStatement);
        await transaction.executeInline(testStatement);
        sinon.assert.calledTwice(executeSpy);
    });

    it("Test executeStream with no parameters", async () => {
        const sampleResultStreamObject: ResultStream = new ResultStream(testTransactionId, pageToken, mockCommunicator);
        const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
        const result: ResultStream = await transaction.executeStream(testStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, []);
        chai.assert.equal(JSON.stringify(result), JSON.stringify(sampleResultStreamObject));
    });

    it("Test executeStream with parameters", async () => {
        const sampleResultStreamObject: ResultStream = new ResultStream(testTransactionId, pageToken, mockCommunicator);
        const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
        const writer1: Writer = transaction.registerParameter(1);
        writer1.writeString(testMessage);
        const writer2: Writer = transaction.registerParameter(2);
        writer2.writeString(testMessage2);
        const result: ResultStream = await transaction.executeStream(testStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, [writer1, writer2]);
        chai.assert.equal(JSON.stringify(result), JSON.stringify(sampleResultStreamObject));
    });

    it("Test executeStream with updated parameter", async () => {
        const sampleResultStreamObject: ResultStream = new ResultStream(testTransactionId, pageToken, mockCommunicator);
        const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
        const writer1: Writer = transaction.registerParameter(1);
        writer1.writeString(testMessage);
        const updatedWriter1: Writer = transaction.registerParameter(1);
        updatedWriter1.writeString(testMessage2);
        const result: ResultStream = await transaction.executeStream(testStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, [updatedWriter1]);
        chai.assert.equal(JSON.stringify(result), JSON.stringify(sampleResultStreamObject));
    });

    it("Test executeStream with non-sequential parameter registration", async () => {
        const sampleResultStreamObject: ResultStream = new ResultStream(testTransactionId, pageToken, mockCommunicator);
        const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
        const writer2: Writer = transaction.registerParameter(2);
        writer2.writeString(testMessage);
        const writer1: Writer = transaction.registerParameter(1);
        writer1.writeString(testMessage2);
        const result: ResultStream = await transaction.executeStream(testStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, [writer1, writer2]);
        chai.assert.equal(JSON.stringify(result), JSON.stringify(sampleResultStreamObject));
    });

    it("Test executeStream with missing parameters", async () => {
        const writer1: Writer = transaction.registerParameter(1);
        writer1.writeString(testMessage);
        const writer3: Writer = transaction.registerParameter(3);
        writer3.writeString(testMessage2);
        await chai.expect(transaction.executeStream(testStatement)).to.be.rejected;
    });

    it("Test executeStream with exception", async () => {
        mockCommunicator.executeStatement = async () => {
            throw new Error(testMessage);
        };
        const isOccStub = sandbox.stub(Errors, "isOccConflictException");
        isOccStub.returns(false);
        const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
        await chai.expect(transaction.executeStream(testStatement)).to.be.rejected;
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, []);
        isOccStub.restore();
    });

    it("Test executeStream twice", async () => {
        const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
        await transaction.executeStream(testStatement);
        await transaction.executeStream(testStatement);
        sinon.assert.calledTwice(executeSpy);
    });

    it("Test getTransactionId", () => {
        const transactionIdSpy = sandbox.spy(transaction, "getTransactionId");
        const transactionId: string = transaction.getTransactionId();
        chai.assert.equal(transactionId, testTransactionId);
        sinon.assert.calledOnce(transactionIdSpy);
    });

    it("Test registering invalid parameter number zero", () => {
        chai.expect(() => {
            transaction.registerParameter(0);
        }).to.throw();
    });

    it("Test registering negative parameter number", () => {
        chai.expect(() => {
            transaction.registerParameter(-1);
        }).to.throw();
    });

    it("Test _findMissingParams with no missing parameters", () => {
        transaction.registerParameter(1);
        transaction.registerParameter(2);
        const missingParamList: string = transaction["_findMissingParams"]();
        chai.assert.equal(missingParamList, "");
    });

    it("Test _findMissingParams with missing parameters", () => {
        transaction.registerParameter(1);
        transaction.registerParameter(5);
        const missingParamList: string = transaction["_findMissingParams"]();
        chai.assert.equal(missingParamList, "2, 3, 4");
    });

    it("Test _internalClose", async () => {
        const sampleResultStreamObject: ResultStream = new ResultStream(testTransactionId, pageToken, mockCommunicator);
        const sampleResultStreamObject2: ResultStream = new ResultStream(testTransactionId, pageToken, mockCommunicator);
        transaction["_resultStreams"] = [sampleResultStreamObject, sampleResultStreamObject2];
        transaction["_internalClose"]();
        chai.expect(transaction["_isClosed"]).to.be.true;
        chai.assert.equal(transaction["_resultStreams"].length, 0);
        chai.assert.equal(sampleResultStreamObject["_isClosed"], true);
        chai.assert.equal(sampleResultStreamObject2["_isClosed"], true);
    });

    it("Test _sendExecute after commit", async () => {
        await transaction.commit();
        await chai.expect(transaction["_sendExecute"](testStatement)).to.be.rejected;
    });

    it("Test _sendExecute when closed", async () => {
        transaction["_isClosed"] = true;
        await chai.expect(transaction["_sendExecute"](testStatement)).to.be.rejected;
    });

    it("Test _updateHash", async () => {
        const updatedHash: Uint8Array = transaction["_txnHash"].dot(QldbHash.toQldbHash(testStatement)).getQldbHash();

        const toQldbHashSpy = sandbox.spy(QldbHash, "toQldbHash");
        transaction["_updateHash"](testStatement, []);
        sinon.assert.calledOnce(toQldbHashSpy);
        sinon.assert.calledWith(toQldbHashSpy, testStatement);

        chai.assert.equal(toBase64(transaction["_txnHash"].getQldbHash()), toBase64(updatedHash));
        toQldbHashSpy.restore();
    });

    it("Test _updateHash with parameters", async () => {
        const parameter1: Writer = transaction.registerParameter(1);
        const parameter2: Writer = transaction.registerParameter(2);
        const parameters: Writer[] = [parameter1, parameter2];

        let testStatementHash: QldbHash = QldbHash.toQldbHash(testStatement);
        parameters.forEach((writer: Writer) => {
            testStatementHash = testStatementHash.dot(QldbHash.toQldbHash(writer.getBytes()));
        });
        const updatedHash: Uint8Array = transaction["_txnHash"].dot(testStatementHash).getQldbHash();

        const toQldbHashSpy = sandbox.spy(QldbHash, "toQldbHash");
        transaction["_updateHash"](testStatement, parameters);
        sinon.assert.calledThrice(toQldbHashSpy);
        sinon.assert.calledWith(toQldbHashSpy, testStatement);
        sinon.assert.calledWith(toQldbHashSpy, parameter1.getBytes());
        sinon.assert.calledWith(toQldbHashSpy, parameter2.getBytes());

        chai.assert.equal(toBase64(transaction["_txnHash"].getQldbHash()), toBase64(updatedHash));
        toQldbHashSpy.restore();
    });
});
