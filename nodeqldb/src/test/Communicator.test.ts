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

// Test environment imports
import "mocha";

import { QLDBSession } from "aws-sdk";
import {
    ClientConfiguration,
    CommitTransactionResult,
    ExecuteStatementResult,
    Page,
    PageToken,
    SendCommandRequest,
    SendCommandResult,
    ValueHolder
} from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import { Communicator } from "../Communicator";
import * as logUtil from "../logUtil";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testLedgerName: string = "fakeLedgerName";
const testMessage: string = "foo";
const testPageToken: PageToken = "pageToken";
const testSessionToken: string = "sessionToken";
const testValueHolder: ValueHolder = {
    IonBinary: 'test'
};
const testParameters: ValueHolder[] = [testValueHolder];
const testStatement: string = "SELECT * FROM foo";
const testTransactionId: string = "txnId";
const testHashToQldb: Uint8Array = new Uint8Array([1, 2, 3]);
const testHashFromQldb: Uint8Array = new Uint8Array([4, 5, 6]);
const testLowLevelClientOptions: ClientConfiguration = {
    region: "fakeRegion"
};

const testPage: Page = {};
const testExecuteStatementResult: ExecuteStatementResult = {
    FirstPage: testPage
};
const testCommitTransactionResult: CommitTransactionResult = {
    TransactionId: testTransactionId,
    CommitDigest: testHashFromQldb
};
const testSendCommandResult: SendCommandResult = {
    StartSession: {
        SessionToken: testSessionToken
    },
    StartTransaction: {
        TransactionId: testTransactionId
    },
    FetchPage: {
        Page: testPage
    },
    ExecuteStatement: testExecuteStatementResult,
    CommitTransaction: testCommitTransactionResult
};

let sendCommandStub: sinon.SinonStub;
let testQldbLowLevelClient: QLDBSession;
let communicator: Communicator;

describe("Communicator test", () => {

    beforeEach(async () => {
        testQldbLowLevelClient = new QLDBSession(testLowLevelClientOptions);
        sendCommandStub = sandbox.stub(testQldbLowLevelClient, "sendCommand");
        sendCommandStub.returns({
            promise: () => {
                return testSendCommandResult;
            }
        });
        communicator = await Communicator.create(testQldbLowLevelClient, testLedgerName);
    });

    afterEach(() => {
        sandbox.restore();
    });

    it("Test abortTransaction", async () => {
        await communicator.abortTransaction();
        const testRequest: SendCommandRequest = {
            SessionToken: testSessionToken,
            AbortTransaction: {}
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, testRequest);
    });

    it("Test commit", async () => {
        const commitResult: CommitTransactionResult = await communicator.commit(testTransactionId, testHashToQldb);
        const testRequest: SendCommandRequest = {
            SessionToken: testSessionToken,
            CommitTransaction: {
                TransactionId: testTransactionId,
                CommitDigest: testHashToQldb
            }
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, testRequest);
        chai.assert.equal(commitResult, testSendCommandResult.CommitTransaction);
    });

    it("Test executeStatement with no parameters", async () => {
        const result: ExecuteStatementResult = await communicator.executeStatement(
            testTransactionId,
            testStatement,
            []
        );
        const testRequest: SendCommandRequest = {
            SessionToken: testSessionToken,
            ExecuteStatement: {
                Statement: testStatement,
                TransactionId: testTransactionId,
                Parameters: []
            }
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, testRequest);
        chai.assert.equal(result, testExecuteStatementResult);
    });

    it("Test executeStatement with parameters", async () => {
        const result: ExecuteStatementResult = await communicator.executeStatement(testTransactionId, testStatement,
            testParameters);
        const testRequest: SendCommandRequest = {
            SessionToken: testSessionToken,
            ExecuteStatement: {
                Statement: testStatement,
                TransactionId: testTransactionId,
                Parameters: testParameters
            }
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, testRequest);
        chai.assert.equal(result, testExecuteStatementResult);
    });

    it("Test executeStatement with exception", async () => {
        sendCommandStub.returns({
            promise: () => {
                throw new Error(testMessage);
            }
        });
        const testRequest: SendCommandRequest = {
            SessionToken: testSessionToken,
            ExecuteStatement: {
                Statement: testStatement,
                TransactionId: testTransactionId,
                Parameters: []
            }
        };
        await chai.expect(communicator.executeStatement(testTransactionId, testStatement, [])).to.be.rejected;
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, testRequest);
    });

    it("Test endSession", async () => {
        await communicator.endSession();
        const testRequest: SendCommandRequest = {
            EndSession: {},
            SessionToken: testSessionToken
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, testRequest);
    });

    it("Test endSession with exception", async () => {
        sendCommandStub.returns({
            promise: () => {
                throw new Error(testMessage);
            }
        });
        const logSpy = sandbox.spy(logUtil, "warn");
        await communicator.endSession();
        const testRequest: SendCommandRequest = {
            EndSession: {},
            SessionToken: testSessionToken
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, testRequest);
        sinon.assert.calledOnce(logSpy);
    });

    it("Test fetchPage", async () => {
        const page: Page = await communicator.fetchPage(testTransactionId, testPageToken);
        const testRequest: SendCommandRequest = {
            SessionToken: testSessionToken,
            FetchPage: {
                TransactionId: testTransactionId,
                NextPageToken: testPageToken
            }
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, testRequest);
        chai.assert.equal(page, testPage);
    });

    it("Test getLedgerName", () => {
        const ledgerName: string = communicator.getLedgerName();
        chai.assert.equal(ledgerName, testLedgerName);
    });

    it("Test getLowLevelClient", () => {
        const lowLevelClient: QLDBSession = communicator.getLowLevelClient();
        chai.assert.equal(lowLevelClient, testQldbLowLevelClient);
    });

    it("Test getSessionToken", () => {
        const sessionToken: string = communicator.getSessionToken();
        chai.assert.equal(sessionToken, testSessionToken);
    });

    it("Test startTransaction", async () => {
        const txnId: string = await communicator.startTransaction();
        const testRequest: SendCommandRequest = {
            SessionToken: testSessionToken,
            StartTransaction: {}
        };
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, testRequest);
        chai.assert.equal(txnId, testTransactionId);
    });

    it("Test _sendCommand", async () => {
        const mockSendCommandRequest: SendCommandRequest = {};
        const result: SendCommandResult = await communicator["_sendCommand"](mockSendCommandRequest);
        sinon.assert.calledTwice(sendCommandStub);
        sinon.assert.calledWith(sendCommandStub, mockSendCommandRequest);
        chai.assert.equal(result, testSendCommandResult);
    });

    it("Test _sendCommand with exception", async () => {
        sendCommandStub.returns({
            promise: () => {
                throw new Error(testMessage);
            }
        });
        const mockSendCommandRequest: SendCommandRequest = {};
        const sendCommand = communicator["_sendCommand"];
        await chai.expect(sendCommand(mockSendCommandRequest)).to.be.rejected;
    });
});
