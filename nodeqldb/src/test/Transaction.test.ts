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

import { 
    CommitTransactionResult,
    ExecuteStatementResult, 
    Page, 
    PageToken
} from "aws-sdk/clients/qldbsession";

import { Communicator } from "../Communicator";
import * as Errors from "../errors/Errors";
import * as logUtil from "../logUtil";
import { Result } from "../Result";
import { ResultStream } from "../ResultStream";
import { Transaction } from "../Transaction";

const chai = require("chai");
const chaiAsPromised = require("chai-as-promised");
chai.use(chaiAsPromised);
const sinon = require("sinon");
const sandbox = sinon.createSandbox();

let mockMessage: string = "foo";
let mockMessage2: string = "bar";
let mockStatement: string = "SELECT * FROM foo";
let mockPageToken: PageToken = "foo";

describe('Transaction test', function() {
    let mockCommunicator: Communicator = sandbox.mock(Communicator);
    let mockExecuteStatementResult: ExecuteStatementResult = {FirstPage: {NextPageToken: mockPageToken}};
    //TODO: Commit digest hash. 
    let mockCommitTransactionResult: CommitTransactionResult = undefined
    let mockResult: Result = sandbox.mock(Result);
    let mockTransactionId: string = "txnId";
    let pageToken: Page = {NextPageToken: mockPageToken};
    let transaction: Transaction;

    beforeEach(function () {
        transaction = new Transaction(mockCommunicator, mockTransactionId);
        mockCommunicator.executeStatement = async function () {return mockExecuteStatementResult;};
        mockCommunicator.commit = async function () {return mockCommitTransactionResult;};
        mockCommunicator.abortTransaction = async function () {};
    });

    after(function () {
        sandbox.restore();
    });
    
    it('Test Get Transaction ID', function() {
        let transactionIdSpy = sandbox.spy(transaction, "getTransactionId");
        let transactionId: string = transaction.getTransactionId();
        chai.assert.equal(transactionId, mockTransactionId);
        sinon.assert.calledOnce(transactionIdSpy);
    });

    it('Test Abort', async function() {
        let abortSpy = sandbox.spy(mockCommunicator, "abort");
        await transaction.abort();
        await transaction.abort();
        sinon.assert.calledOnce(abortSpy);
    });

    it('Test Abort with Exception', async function() {
        mockCommunicator.abortTransaction = async function () {throw new Error(mockMessage);};
        let abortSpy = sandbox.spy(mockCommunicator, "abort");
        await chai.expect(transaction.abort()).to.be.rejected;
        sinon.assert.calledOnce(abortSpy);
    });

    it('Test Commit', async function() {
        let commitSpy = sandbox.spy(mockCommunicator, "commit");
        await transaction.commit();
        sinon.assert.calledOnce(commitSpy);
    });

    it('Test Commit with Exception', async function() {
        mockCommunicator.commit = async function () {throw new Error(mockMessage);};
        let isOccStub = sandbox.stub(Errors, "isOccConflictException");
        isOccStub.returns(false);
        let commitSpy = sandbox.spy(mockCommunicator, "commit");
        await chai.expect(transaction.commit()).to.be.rejected;
        sinon.assert.calledOnce(isOccStub);
        sinon.assert.calledOnce(commitSpy);
        isOccStub.restore();
    });

    it('Test Commit with OccConflict', async function() {
        mockCommunicator.commit = async function () {throw new Error(mockMessage);};
        let isOccStub = sandbox.stub(Errors, "isOccConflictException");
        isOccStub.returns(true);
        let commitSpy = sandbox.spy(mockCommunicator, "commit");
        await chai.expect(transaction.commit()).to.be.rejected;
        sinon.assert.calledOnce(commitSpy);
        sinon.assert.calledOnce(isOccStub);
        isOccStub.restore();
    });

    it('Test Commit with Non-OccConflict', async function() {
        mockCommunicator.commit = async function () {throw new Error(mockMessage);};
        let isOccStub = sandbox.stub(Errors, "isOccConflictException");
        isOccStub.returns(false);
        let commitSpy = sandbox.spy(mockCommunicator, "commit");
        let abortSpy = sandbox.spy(mockCommunicator, "abort");
        await chai.expect(transaction.commit()).to.be.rejected;
        sinon.assert.calledOnce(commitSpy);
        sinon.assert.calledOnce(abortSpy);
        sinon.assert.calledOnce(isOccStub);
        isOccStub.restore();
    });

    it('Test Commit with Non-OccConflict and Abort Throws Error', async function() {
        mockCommunicator.commit = async function () {throw new Error("mockMessage");};
        mockCommunicator.abortTransaction = async function () {throw new Error("foo2");};
        let isOccStub = sandbox.stub(Errors, "isOccConflictException");
        let logSpy = sandbox.spy(logUtil, "warn");
        isOccStub.returns(false);
        let commitSpy = sandbox.spy(mockCommunicator, "commit");
        let abortSpy = sandbox.spy(mockCommunicator, "abort");
        await chai.expect(transaction.commit()).to.be.rejected;
        sinon.assert.calledOnce(commitSpy);
        sinon.assert.calledOnce(abortSpy);
        sinon.assert.calledOnce(isOccStub);
        sinon.assert.calledOnce(logSpy);
        isOccStub.restore();
    });

    it('Test Execute Inline with No Parameters', async function() {
        Result.create = async function() {return mockResult};
        let executeSpy = sandbox.spy(mockCommunicator, "execute");
        let result: Result = await transaction.executeInline(mockStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, mockStatement, mockTransactionId, undefined);
        chai.assert.equal(result, mockResult);
    });

    it('Test Execute Inline with Parameters', async function() {
        Result.create = async function() {return mockResult};
        let executeSpy = sandbox.spy(mockCommunicator, "execute");
        let writer1 = transaction.registerParameter(1);
        writer1.writeString(mockMessage);
        let writer2 = transaction.registerParameter(2);
        writer2.writeString(mockMessage2);
        let result: Result = await transaction.executeInline(mockStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, mockStatement, mockTransactionId, [writer1, writer2]);
        chai.assert.equal(result, mockResult);
    });

    it('Test Execute Inline with Updated Parameter', async function() {
        Result.create = async function() {return mockResult};
        let executeSpy = sandbox.spy(mockCommunicator, "execute");
        let writer1 = transaction.registerParameter(1);
        writer1.writeString(mockMessage);
        let updatedWriter1 = transaction.registerParameter(1);
        updatedWriter1.writeString(mockMessage2);
        let result: Result = await transaction.executeInline(mockStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, mockStatement, mockTransactionId, [updatedWriter1]);
        chai.assert.equal(result, mockResult);
    });

    it('Test Execute Inline with Non-Sequential Parameter Registration', async function() {
        Result.create = async function() {return mockResult};
        let executeSpy = sandbox.spy(mockCommunicator, "execute");
        let writer2 = transaction.registerParameter(2);
        writer2.writeString(mockMessage);
        let writer1 = transaction.registerParameter(1);
        writer1.writeString(mockMessage2);
        let result: Result = await transaction.executeInline(mockStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, mockStatement, mockTransactionId, [writer1, writer2]);
        chai.assert.equal(result, mockResult);
    });

    it('Test Execute Inline with Missing Parameters', async function() {
        let writer1 = transaction.registerParameter(1);
        writer1.writeString(mockMessage);
        let writer3 = transaction.registerParameter(3);
        writer3.writeString(mockMessage2);
        await chai.expect(transaction.executeInline(mockStatement)).to.be.rejected;
    });

    it('Test Execute Inline with Exception', async function() {
        mockCommunicator.executeStatement = async function () {throw new Error(mockMessage);};
        let isOccStub = sandbox.stub(Errors, "isOccConflictException");
        isOccStub.returns(false);
        let executeSpy = sandbox.spy(mockCommunicator, "execute");
        await chai.expect(transaction.executeInline(mockStatement)).to.be.rejected;
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, mockStatement, mockTransactionId, undefined);
        isOccStub.restore();
    });

    it('Test Execute Stream with No Parameters', async function() {
        let sampleResultStreamObject = new ResultStream(mockTransactionId, pageToken, mockCommunicator);
        let executeSpy = sandbox.spy(mockCommunicator, "execute");
        let result: ResultStream = await transaction.executeStream(mockStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, mockStatement, mockTransactionId, undefined);
        chai.assert.equal(JSON.stringify(result), JSON.stringify(sampleResultStreamObject));
    });

    it('Test Execute Stream with Parameters', async function() {
        let sampleResultStreamObject = new ResultStream(mockTransactionId, pageToken, mockCommunicator);
        let executeSpy = sandbox.spy(mockCommunicator, "execute");
        let writer1 = transaction.registerParameter(1);
        writer1.writeString(mockMessage);
        let writer2 = transaction.registerParameter(2);
        writer2.writeString(mockMessage2);
        let result: ResultStream = await transaction.executeStream(mockStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, mockStatement, mockTransactionId, [writer1, writer2]);
        chai.assert.equal(JSON.stringify(result), JSON.stringify(sampleResultStreamObject));
    });

    it('Test Execute Stream with Updated Parameter', async function() {
        let sampleResultStreamObject = new ResultStream(mockTransactionId, pageToken, mockCommunicator);
        let executeSpy = sandbox.spy(mockCommunicator, "execute");
        let writer1 = transaction.registerParameter(1);
        writer1.writeString(mockMessage);
        let updatedWriter1 = transaction.registerParameter(1);
        updatedWriter1.writeString(mockMessage2);
        let result: ResultStream = await transaction.executeStream(mockStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, mockStatement, mockTransactionId, [updatedWriter1]);
        chai.assert.equal(JSON.stringify(result), JSON.stringify(sampleResultStreamObject));
    });

    it('Test Execute Stream with Non-Sequential Parameter Registration', async function() {
        let sampleResultStreamObject = new ResultStream(mockTransactionId, pageToken, mockCommunicator);
        let executeSpy = sandbox.spy(mockCommunicator, "execute");
        let writer2 = transaction.registerParameter(2);
        writer2.writeString(mockMessage);
        let writer1 = transaction.registerParameter(1);
        writer1.writeString(mockMessage2);
        let result: ResultStream = await transaction.executeStream(mockStatement);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, mockStatement, mockTransactionId, [writer1, writer2]);
        chai.assert.equal(JSON.stringify(result), JSON.stringify(sampleResultStreamObject));
    });

    it('Test Execute Stream with Missing Parameters', async function() {
        let writer1 = transaction.registerParameter(1);
        writer1.writeString(mockMessage);
        let writer3 = transaction.registerParameter(3);
        writer3.writeString(mockMessage2);
        await chai.expect(transaction.executeStream(mockStatement)).to.be.rejected;
    });

    it('Test Execute Stream with Exception', async function() {
        mockCommunicator.executeStatement = async function () {throw new Error(mockMessage);};
        let isOccStub = sandbox.stub(Errors, "isOccConflictException");
        isOccStub.returns(false);
        let executeSpy = sandbox.spy(mockCommunicator, "execute");
        await chai.expect(transaction.executeStream(mockStatement)).to.be.rejected;
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, mockStatement, mockTransactionId, undefined);
        isOccStub.restore();
    });

    it('Test Registering Invalid Parameter Number Zero', function() {
        chai.expect(function() {
            transaction.registerParameter(0);
        }).to.throw();
    });

    it('Test Registering Negative Parameter Number', function() {
        chai.expect(function() {
            transaction.registerParameter(-1);
        }).to.throw();
    });

    it('Test FindMissingParams with No Missing Parameters', function() {
        transaction.registerParameter(1);
        transaction.registerParameter(2);
        let findMissingParams = transaction["_findMissingParams"];
        let missingParamList = findMissingParams();
        chai.assert.equal(missingParamList, "");
    });

    it('Test FindMissingParams with Missing Parameters', function() {
        transaction.registerParameter(1);
        transaction.registerParameter(5);
        let findMissingParams = transaction["_findMissingParams"];
        let missingParamList = findMissingParams();
        chai.assert.equal(missingParamList, "2, 3, 4");
    });

    it('Test Abort after Commit', async function() {
        let abortSpy = sandbox.spy(mockCommunicator, "abort");
        await transaction.commit();
        // This should be a no-op.
        await transaction.abort();
        sinon.assert.notCalled(abortSpy);
    });

    it('Test Commit after Commit', async function() {
        let commitSpy = sandbox.spy(mockCommunicator, "commit");
        await transaction.commit();
        await chai.expect(transaction.commit()).to.be.rejected;
        sinon.assert.calledOnce(commitSpy);
    });

    it('Test Execute Inline after Commit', async function() {
        await transaction.commit();
        await chai.expect(transaction.executeInline(mockStatement)).to.be.rejected;
    });

    it('Test Execute Stream after Commit', async function() {
        await transaction.commit();
        await chai.expect(transaction.executeStream(mockStatement)).to.be.rejected;
    });

    it('Test Execute Inline Twice', async function() {
        let executeSpy = sandbox.spy(mockCommunicator, "execute");
        await transaction.executeInline(mockStatement);
        await transaction.executeInline(mockStatement);
        sinon.assert.calledTwice(executeSpy);
    });

    it('Test Execute Stream Twice', async function() {
        let executeSpy = sandbox.spy(mockCommunicator, "execute");
        await transaction.executeStream(mockStatement);
        await transaction.executeStream(mockStatement);
        sinon.assert.calledTwice(executeSpy);
    });

    it('Test Execute Inline, Commit, Execute Stream', async function() {
        let executeSpy = sandbox.spy(mockCommunicator, "execute");
        await transaction.executeInline(mockStatement);
        await transaction.commit();
        await chai.expect(transaction.executeStream(mockStatement)).to.be.rejected;
        sinon.assert.calledOnce(executeSpy);
    });
})