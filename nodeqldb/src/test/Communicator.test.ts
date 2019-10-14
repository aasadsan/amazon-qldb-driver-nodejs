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
    ClientConfiguration,
    ExecuteStatementResult, 
    Page, 
    PageToken,
    SendCommandRequest, 
    SendCommandResult
} from "aws-sdk/clients/qldbsession";
import { IonTypes, makeBinaryWriter, Writer } from "ion-js";

import * as Errors from "../errors/Errors";
import { Communicator } from "../Communicator";
import * as logUtil from "../logUtil";

const chai = require("chai");
const chaiAsPromised = require("chai-as-promised");
chai.use(chaiAsPromised);
const sinon = require("sinon");
const sandbox = sinon.createSandbox();

let mockLedgerName: string = "fakeLedgerName";
let mockMessage: string = "foo";
let mockPageToken: PageToken = "pageToken";
let mockWriter: Writer = makeBinaryWriter();
let mockParameters: Writer[] = [mockWriter];
let mockStatement: string = "SELECT * FROM foo";
let mockTransactionId: string = "txnId";

describe('Communicator test', function() {
    let communicator: Communicator;
    let mockSessionToken: string = "sessionToken";
    let mockLowLevelClientOptions: ClientConfiguration = {
        region: "fakeRegion"
    };
    let mockPage: Page = {};
    let mockExecuteStatementResult: ExecuteStatementResult = {FirstPage: mockPage};
    let mockQldbLowLevelClient: QLDBSession;
    let mockSendCommandResult: SendCommandResult = {StartSession: {SessionToken: mockSessionToken},
                                                    StartTransaction: {TransactionId: mockTransactionId}, 
                                                    FetchPage: {Page: mockPage}, 
                                                    ExecuteStatement: mockExecuteStatementResult};
    let sendCommandStub;

    beforeEach(async function () {
        mockQldbLowLevelClient = new QLDBSession(mockLowLevelClientOptions);
        sendCommandStub = sandbox.stub(mockQldbLowLevelClient, "sendCommand");
        sendCommandStub.returns({promise: () => {return mockSendCommandResult}});
        communicator = await Communicator.create(mockQldbLowLevelClient, mockLedgerName);
    });

    afterEach(function () {
        sendCommandStub.restore();
    });

    after(function () {
        sandbox.restore();
    });

    it('Test Start Transaction', async function() {
        let txnId: string = await communicator.startTransaction();
        let mockRequest: SendCommandRequest = {
            SessionToken: mockSessionToken,
            StartTransaction: {}
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, mockRequest);
        chai.assert.equal(txnId, mockTransactionId);
    });

    it('Test Abort', async function() {
        await communicator.abortTransaction();
        let mockRequest: SendCommandRequest = {
            SessionToken: mockSessionToken,
            AbortTransaction: {}
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, mockRequest);
    });

    it('Test Commit', async function() {
        //TODO: Commit digest hash.
        await communicator.commit(mockTransactionId, undefined);
        let mockRequest: SendCommandRequest = {
            SessionToken: mockSessionToken,
            CommitTransaction: {
              TransactionId: mockTransactionId,
              CommitDigest: undefined
            }
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, mockRequest);
    });

    it('Test Close', async function() {
        await communicator.endSession();
        let mockRequest = {
            EndSession: {}
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, mockRequest);
    });

    it('Test Close With Exception', async function() {
        sendCommandStub.returns({promise: () => {throw new Error(mockMessage)}});
        let logSpy = sandbox.spy(logUtil, "warn");
        await communicator.endSession();
        let mockRequest = {
            EndSession: {}
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, mockRequest);
        sinon.assert.calledOnce(logSpy);
    });

    it('Test Fetch Result', async function() {
        let page: Page = await communicator.fetchPage(mockTransactionId, mockPageToken);
        let mockRequest: SendCommandRequest = {
            SessionToken: mockSessionToken,
            FetchPage: {
              TransactionId: mockTransactionId,
              NextPageToken: mockPageToken
            }
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, mockRequest);
        chai.assert.equal(page, mockPage);
    });

    it('Test Send Command', async function() {
        let mockSendCommandRequest: SendCommandRequest = {};
        let sendCommand = communicator["_sendCommand"];
        let result: SendCommandResult = await sendCommand(mockSendCommandRequest);
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, mockSendCommandRequest);
        chai.assert.equal(result, mockSendCommandResult);
    });

    it('Test Send Command With InvalidSessionException', async function() {
        let isInvalidSessionStub = sandbox.stub(Errors, "isInvalidSessionException");
        isInvalidSessionStub.returns(true);
        sendCommandStub.returns({promise: () => {throw new Error(mockMessage)}});
        let mockSendCommandRequest: SendCommandRequest = {};
        let sendCommand = communicator["_sendCommand"];
        await chai.expect(sendCommand(mockSendCommandRequest)).to.be.rejected;
        isInvalidSessionStub.restore();
    });

    it('Test Send Command With Exception Not InvalidSessionException', async function() {
        let isInvalidSessionStub = sandbox.stub(Errors, "isInvalidSessionException");
        isInvalidSessionStub.returns(false);
        sendCommandStub.returns({promise: () => {throw new Error(mockMessage)}});
        let mockSendCommandRequest: SendCommandRequest = {};
        let sendCommand = communicator["_sendCommand"];
        await chai.expect(sendCommand(mockSendCommandRequest)).to.be.rejected;
        isInvalidSessionStub.restore();
    });

    it('Test Execute', async function() {
        let result: ExecuteStatementResult = await communicator.executeStatement(mockTransactionId, mockStatement, 
            undefined);
        let mockRequest: SendCommandRequest = {
            SessionToken: mockSessionToken,
            ExecuteStatement: {
              Statement: mockStatement,
              TransactionId: mockTransactionId, 
              Parameters: []
            }
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, mockRequest);
        chai.assert.equal(result, mockExecuteStatementResult);
    });

    it('Test Execute with Parameters', async function() {
        let result: ExecuteStatementResult = await communicator.executeStatement(mockStatement, mockTransactionId, 
            mockParameters);
        let ionToValueHolder = communicator["_ionToValueHolder"];
        let sentParameters = ionToValueHolder(mockParameters);
        let mockRequest: SendCommandRequest = {
            SessionToken: mockSessionToken,
            ExecuteStatement: {
              Statement: mockStatement,
              TransactionId: mockTransactionId, 
              Parameters: sentParameters
            }
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, mockRequest);
        chai.assert.equal(result, mockExecuteStatementResult);
    });

    it('Test Execute Raises Exception', async function() {
        sendCommandStub.returns({promise: () => {throw new Error(mockMessage)}});
        let mockRequest: SendCommandRequest = {
            SessionToken: mockSessionToken,
            ExecuteStatement: {
              Statement: mockStatement,
              TransactionId: mockTransactionId, 
              Parameters: []
            }
        };
        await chai.expect(communicator.executeStatement(mockStatement, mockTransactionId, undefined)).to.be.rejected;
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, mockRequest);
    });

    it('Test Get Session Token', function() {
        let sessionToken: string = communicator.getSessionToken()
        chai.assert.equal(sessionToken, mockSessionToken);
    });

    it('Test Get LowLevelClient', function() {
        let lowLevelClient: QLDBSession = communicator.getLowLevelClient();
        chai.assert.equal(lowLevelClient, mockQldbLowLevelClient);
    });

    it('Test IonToValueHolder Method', function() {
        let ionWriter: Writer = makeBinaryWriter();
        let communicatorIonWriter: Writer = makeBinaryWriter();
        let object = [{PersonId: "123"}];
        ionWriter.stepIn(IonTypes.STRUCT);
        for (const key of Object.keys(object)) {
            ionWriter.writeFieldName(key);
            ionWriter.writeString(object[key]);
        }
        communicatorIonWriter.stepIn(IonTypes.STRUCT)
        for (const key of Object.keys(object)) {
            communicatorIonWriter.writeFieldName(key);
            communicatorIonWriter.writeString(object[key]);
        }
        let valueHolder = [{IonBinary: ionWriter.getBytes()}];
        let ionToValueHolder = communicator["_ionToValueHolder"];
        let communicatorValueHolder = ionToValueHolder([communicatorIonWriter]);
        chai.assert.equal(JSON.stringify(valueHolder), JSON.stringify(communicatorValueHolder));
    });
})