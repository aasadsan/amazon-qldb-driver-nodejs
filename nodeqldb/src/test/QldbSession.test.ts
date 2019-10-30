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
    ExecuteStatementResult,
    Page,
    PageToken,
    ValueHolder
} from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import { makeReader, Reader } from "ion-js";
import * as sinon from "sinon";
import { Readable } from "stream";
import { format } from "util";

import { Communicator } from "../Communicator";
import * as Errors from "../errors/Errors";
import * as logUtil from "../logUtil";
import { QldbSessionImpl } from "../QldbSessionImpl";
import { createQldbWriter, QldbWriter } from "../QldbWriter";
import { Result } from "../Result";
import { ResultStream } from "../ResultStream";
import { Transaction } from "../Transaction";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testRetryLimit: number = 4;
const testLedgerName: string = "fakeLedgerName";
const testSessionToken: string = "sessionToken";
const testTransactionId: string = "txnId";
const testMessage: string = "foo";
const testTableNames: string[] = ["Vehicle", "Person"];
const testStatement: string = "SELECT * FROM foo";

const TEST_SLEEP_CAP_MS: number = 5000;
const TEST_SLEEP_BASE_MS: number = 10;

const testValueHolder: ValueHolder[] = [{IonBinary: "{ hello:\"world\" }"}];
const testPageToken: PageToken = "foo";
const testExecuteStatementResult: ExecuteStatementResult = {
    FirstPage: {
        NextPageToken: testPageToken,
        Values: testValueHolder
    }
};
const mockLowLevelClientOptions: ClientConfiguration = {
    region: "fakeRegion"
};
const testQldbLowLevelClient: QLDBSession = new QLDBSession(mockLowLevelClientOptions);
const testPage: Page = {};

const mockCommunicator: Communicator = <Communicator><any> sandbox.mock(Communicator);
const mockResult: Result = <Result><any> sandbox.mock(Result);
const mockTransaction: Transaction = <Transaction><any> sandbox.mock(Transaction);

const resultStreamObject: ResultStream = new ResultStream(testTransactionId, testPage, mockCommunicator);
let qldbSession: QldbSessionImpl;

describe("QldbSession test", () => {

    beforeEach(() => {
        qldbSession = new QldbSessionImpl(mockCommunicator, testRetryLimit);
        mockCommunicator.endSession = async () => {};
        mockCommunicator.getLedgerName = () => {
            return testLedgerName;
        };
        mockCommunicator.getSessionToken = () => {
            return testSessionToken;
        };
        mockCommunicator.startTransaction = async () => {
            return testTransactionId;
        };
        mockCommunicator.abortTransaction = async () => {};
        mockCommunicator.executeStatement = async () => {
            return testExecuteStatementResult;
        };
        mockCommunicator.getLowLevelClient = () => {
            return testQldbLowLevelClient;
        };
    });

    afterEach(() => {
        sandbox.restore();
    });

    it("Test close", async () => {
        const communicatorEndSpy = sandbox.spy(mockCommunicator, "endSession");
        await qldbSession.close();
        chai.assert.equal(qldbSession["_isClosed"], true);
        sinon.assert.calledOnce(communicatorEndSpy);
    });

    it("Test close when session closed", async () => {
        const communicatorEndSpy = sandbox.spy(mockCommunicator, "endSession");
        qldbSession["_isClosed"] = true;
        await qldbSession.close();
        sinon.assert.notCalled(communicatorEndSpy);
    });

    it("Test executeLambda with executeInline", async () => {
        qldbSession.startTransaction = async () => {
            return mockTransaction;
        };
        mockTransaction.executeInline = async () => {
            return mockResult;
        };
        mockTransaction.commit = async () => {};

        const executeInlineSpy = sandbox.spy(mockTransaction, "executeInline");
        const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
        const commitSpy = sandbox.spy(mockTransaction, "commit");

        const result = await qldbSession.executeLambda(async (txn) => {
            return await txn.executeInline(testStatement);
        });
        sinon.assert.calledOnce(executeInlineSpy);
        sinon.assert.calledWith(executeInlineSpy, testStatement);
        sinon.assert.calledOnce(startTransactionSpy);
        sinon.assert.calledOnce(commitSpy);
        chai.assert.equal(result, mockResult);
    });

    it("Test executeLambda with executeStream", async () => {
        const resultStub = sandbox.stub(Result, "bufferResultStream");
        resultStub.returns(Promise.resolve(mockResult));

        qldbSession.startTransaction = async () => {
            return mockTransaction;
        };
        mockTransaction.executeStream = async () => {
            return resultStreamObject;
        };
        mockTransaction.commit = async () => {};

        const executeStreamSpy = sandbox.spy(mockTransaction, "executeStream");
        const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
        const commitSpy = sandbox.spy(mockTransaction, "commit");

        const result = await qldbSession.executeLambda(async (txn) => {
            return await txn.executeStream(testStatement);
        });
        sinon.assert.calledOnce(executeStreamSpy);
        sinon.assert.calledWith(executeStreamSpy, testStatement);
        sinon.assert.calledOnce(startTransactionSpy);
        sinon.assert.calledOnce(resultStub);
        sinon.assert.calledOnce(commitSpy);
        chai.assert.equal(result, mockResult);
    });

    it("Test executeLambda when session closed", async () => {
        qldbSession["_isClosed"] = true;

        const error = await chai.expect(qldbSession.executeLambda(async (txn) => {
            return await txn.executeInline(testStatement);
        })).to.be.rejected;
        chai.assert.equal(error.name, "SessionClosedError");
    });

    it("Test executeLambda with client error", async () => {
        qldbSession.startTransaction = async () => {
            throw new Error(testMessage);
        };

        const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
        const noThrowAbortSpy = sandbox.spy(qldbSession as any, "_noThrowAbort");
        const throwIfClosedSpy = sandbox.spy(qldbSession as any, "_throwIfClosed");

        await chai.expect(qldbSession.executeLambda(async (txn) => {
            return await txn.executeInline(testStatement);
        })).to.be.rejected;
        sinon.assert.calledOnce(startTransactionSpy);
        sinon.assert.calledOnce(noThrowAbortSpy);
        sinon.assert.calledOnce(throwIfClosedSpy);
    });

    it("Test executeLambda with OccConflictException", async () => {
        const isOccStub = sandbox.stub(Errors, "isOccConflictException");
        isOccStub.returns(true);

        const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
        const noThrowAbortSpy = sandbox.spy(qldbSession as any, "_noThrowAbort");
        const logSpy = sandbox.spy(logUtil, "warn");

        await chai.expect(qldbSession.executeLambda(async (txn) => {
            throw new Error(testMessage);
        }, () => {})).to.be.rejected;

        sinon.assert.callCount(startTransactionSpy, testRetryLimit + 1);
        sinon.assert.callCount(noThrowAbortSpy, testRetryLimit + 1);
        sinon.assert.callCount(logSpy, testRetryLimit);
    });

    it("Test executeLambda with retriable exception", async () => {
        const isRetriableStub = sandbox.stub(Errors, "isRetriableException");
        isRetriableStub.returns(true);

        const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
        const noThrowAbortSpy = sandbox.spy(qldbSession as any, "_noThrowAbort");
        const logSpy = sandbox.spy(logUtil, "warn");

        await chai.expect(qldbSession.executeLambda(async (txn) => {
            throw new Error(testMessage);
        }, () => {})).to.be.rejected;

        sinon.assert.callCount(startTransactionSpy, testRetryLimit + 1);
        sinon.assert.callCount(noThrowAbortSpy, testRetryLimit + 1);
        sinon.assert.callCount(logSpy, testRetryLimit);
    });

    it("Test executeLambda with InvalidSessionException", async () => {
        const isInvalidSessionStub = sandbox.stub(Errors, "isInvalidSessionException");
        isInvalidSessionStub.returns(true);

        const logWarnSpy = sandbox.spy(logUtil, "warn");
        const logInfoSpy = sandbox.spy(logUtil, "info");

        Communicator.create = async () => {
            return mockCommunicator;
        };
        const communicatorSpy = sandbox.spy(Communicator, "create");

        await chai.expect(qldbSession.executeLambda(async (txn) => {
            throw new Error(testMessage);
        }, () => {})).to.be.rejected;

        sinon.assert.callCount(logWarnSpy, testRetryLimit);
        sinon.assert.callCount(logInfoSpy, testRetryLimit);
        sinon.assert.callCount(communicatorSpy, testRetryLimit);
    });

    it("Test executeLambda with defined retryIndicator", async () => {
        const isRetriableStub = sandbox.stub(Errors, "isRetriableException");
        isRetriableStub.returns(true);
        const retryIndicator = () =>
            logUtil.log("Retrying test retry indicator...");

        const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
        const noThrowAbortSpy = sandbox.spy(qldbSession as any, "_noThrowAbort");
        const logSpy = sandbox.spy(logUtil, "warn");
        const retryIndicatorSpy = sandbox.spy(logUtil, "log");

        await chai.expect(qldbSession.executeLambda(async (txn) => {
            throw new Error(testMessage);
        }, retryIndicator)).to.be.rejected;

        sinon.assert.callCount(startTransactionSpy, testRetryLimit + 1);
        sinon.assert.callCount(noThrowAbortSpy, testRetryLimit + 1);
        sinon.assert.callCount(logSpy, testRetryLimit);
        sinon.assert.callCount(retryIndicatorSpy, testRetryLimit);
        sinon.assert.alwaysCalledWith(retryIndicatorSpy, "Retrying test retry indicator...");
    });

    it("Test executeLambda with LambdaAbortedError", async () => {
        const lambdaAbortedError: Errors.LambdaAbortedError = new Errors.LambdaAbortedError();
        await chai.expect(qldbSession.executeLambda(async (txn) => {
            throw lambdaAbortedError;
        }, () => {})).to.be.rejected;
    });

    it("Test executeStatement", async () => {
        const executeStub = sandbox.stub(qldbSession, "executeLambda");
        executeStub.returns(Promise.resolve(mockResult));
        const result: Result = await qldbSession.executeStatement(testStatement);
        chai.assert.equal(result, mockResult);
        sinon.assert.calledOnce(executeStub);
    });

    it("Test executeStatement with parameters", async () => {
        qldbSession.startTransaction = async () => {
            return mockTransaction;
        };

        const mockQldbWriter = <QldbWriter><any>sandbox.mock(createQldbWriter);
        const executeInlineSpy = sandbox.spy(mockTransaction, "executeInline");
        const result: Result = await qldbSession.executeStatement(testStatement, [mockQldbWriter]);
        chai.assert.equal(result, mockResult);
        sinon.assert.calledWith(executeInlineSpy, testStatement, [mockQldbWriter]);
    });

    it("Test getLedgerName", () => {
        const communicatorLedgerSpy = sandbox.spy(mockCommunicator, "getLedgerName");
        const ledgerName: string = qldbSession.getLedgerName();
        chai.assert.equal(ledgerName, testLedgerName);
        sinon.assert.calledOnce(communicatorLedgerSpy);
    });

    it("Test getSessionToken", () => {
        const communicatorTokenSpy = sandbox.spy(mockCommunicator, "getSessionToken");
        const sessionToken: string = qldbSession.getSessionToken();
        chai.assert.equal(sessionToken, testSessionToken);
        sinon.assert.calledOnce(communicatorTokenSpy);
    });

    it("Test getTableNames", async () => {
        const executeStub = sandbox.stub(qldbSession, "executeLambda");
        executeStub.returns(Promise.resolve(testTableNames));
        const listOfTableNames: string[] = await qldbSession.getTableNames();
        chai.assert.equal(listOfTableNames.length, testTableNames.length);
        chai.assert.equal(listOfTableNames, testTableNames);
    });

    it("Test startTransaction", async () => {
        const communicatorTransactionSpy = sandbox.spy(mockCommunicator, "startTransaction");
        const transaction = await qldbSession.startTransaction();
        chai.expect(transaction).to.be.an.instanceOf(Transaction);
        chai.assert.equal(transaction["_txnId"], testTransactionId);
        sinon.assert.calledOnce(communicatorTransactionSpy);
    });

    it("Test startTransaction when closed", async () => {
        const communicatorTransactionSpy = sandbox.spy(mockCommunicator, "startTransaction");
        qldbSession["_isClosed"] = true;
        const error = await chai.expect(qldbSession.startTransaction()).to.be.rejected;
        chai.assert.equal(error.name, "SessionClosedError");
        sinon.assert.notCalled(communicatorTransactionSpy);
    });

    it("Test _abortOrClose", async () => {
        const communicatorAbortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
        chai.assert.equal(await qldbSession._abortOrClose(), true);
        sinon.assert.calledOnce(communicatorAbortSpy);
    });

    it("Test _abortOrClose with exception", async () => {
        mockCommunicator.abortTransaction = async () => {
            throw new Error(testMessage);
        };
        chai.assert.equal(await qldbSession._abortOrClose(), false);
        chai.assert.equal(await qldbSession["_isClosed"], true);
    });

    it("Test _abortOrClose when closed", async () => {
        const communicatorAbortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
        qldbSession["_isClosed"] = true;
        chai.assert.equal(await qldbSession._abortOrClose(), false);
        sinon.assert.notCalled(communicatorAbortSpy);
    });

    it("Test _throwIfClosed when not closed", () => {
        chai.expect(qldbSession["_throwIfClosed"]()).to.not.throw;
    });

    it("Test _throwIfClosed when closed", () => {
        qldbSession["_isClosed"] = true;
        chai.expect(() => {
            qldbSession["_throwIfClosed"]();
        }).to.throw(Errors.SessionClosedError);
    });

    it("Test _noThrowAbort with null Transaction", async () => {
        const communicatorAbortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
        await qldbSession["_noThrowAbort"](null);
        sinon.assert.calledOnce(communicatorAbortSpy);
    });

    it("Test _noThrowAbort with Transaction", async () => {
        mockTransaction.abort = async () => {};
        const communicatorAbortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
        const transactionAbortSpy = sandbox.spy(mockTransaction, "abort");
        await qldbSession["_noThrowAbort"](mockTransaction);
        sinon.assert.notCalled(communicatorAbortSpy);
        sinon.assert.calledOnce(transactionAbortSpy);
    });

    it("Test _noThrowAbort with exception", async () => {
        mockTransaction.abort = async () => {
            throw new Error(testMessage);
        };
        const logSpy = sandbox.spy(logUtil, "warn");
        const communicatorAbortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
        const transactionAbortSpy = sandbox.spy(mockTransaction, "abort");
        await qldbSession["_noThrowAbort"](mockTransaction);
        sinon.assert.notCalled(communicatorAbortSpy);
        sinon.assert.calledOnce(transactionAbortSpy);
        sinon.assert.calledOnce(logSpy);
    });

    it("Test _retrySleep", async () => {
        const mathRandStub = sandbox.stub(Math, "random");
        mathRandStub.returns(1);
        const attemptNumber: number = 1;
        const exponentialBackoff: number = Math.min(TEST_SLEEP_CAP_MS, Math.pow(TEST_SLEEP_BASE_MS, attemptNumber));
        const sleepValue: number = 1 * (exponentialBackoff + 1);

        const clock = sinon.useFakeTimers();
        const timeoutSpy = sandbox.spy(clock, "setTimeout");
        await qldbSession["_retrySleep"](attemptNumber);
        sinon.assert.calledOnce(timeoutSpy);
        sinon.assert.calledWith(timeoutSpy, sinon.match.any, sleepValue);
    });

    it("Test _tableNameHelper", async () => {
        const value1: ValueHolder = {IonBinary: format("{ name:\"%s\" }", testTableNames[0])};
        const value2: ValueHolder = {IonBinary: format("{ name:\"%s\" }", testTableNames[1])};
        const readers: Reader[] = [
            makeReader(Result._handleBlob(value1.IonBinary)),
            makeReader(Result._handleBlob(value2.IonBinary))
        ];
        let eventCount: number = 0;
        const mockResultStream: Readable = new Readable({
            objectMode: true,
            read: function(size) {
                if (eventCount < readers.length) {
                    eventCount += 1;
                    return this.push(readers[eventCount-1]);
                } else {
                    return this.push(null);
                }
            }
        });
        const tableNames: string[] = await qldbSession["_tableNameHelper"](<ResultStream> mockResultStream);
        tableNames.forEach((tableName, i) => {
            chai.assert.equal(tableName, testTableNames[i]);
        });
    });

    it("Test _tableNameHelper with Reader without struct", async () => {
        const value1: ValueHolder = {IonBinary: "notAStruct"};
        const readers: Reader[] = [makeReader(Result._handleBlob(value1.IonBinary))];
        let eventCount: number = 0;
        const mockResultStream: Readable = new Readable({
            objectMode: true,
            read: function(size) {
                if (eventCount < readers.length) {
                    eventCount += 1;
                    return this.push(readers[eventCount-1]);
                } else {
                    return this.push(null);
                }
            }
        });
        const error =
            await chai.expect(qldbSession["_tableNameHelper"](<ResultStream> mockResultStream)).to.be.rejected;
        chai.assert.equal(error.name, "ClientException");
    });

    it("Test _tableNameHelper with Reader without string", async () => {
        const value1: ValueHolder = {IonBinary: "{ structKeyName:1 }"};
        const readers: Reader[] = [makeReader(Result._handleBlob(value1.IonBinary))];
        let eventCount: number = 0;
        const mockResultStream: Readable = new Readable({
            objectMode: true,
            read: function(size) {
                if (eventCount < readers.length) {
                    eventCount += 1;
                    return this.push(readers[eventCount-1]);
                } else {
                    return this.push(null);
                }
            }
        });
        const error =
            await chai.expect(qldbSession["_tableNameHelper"](<ResultStream> mockResultStream)).to.be.rejected;
        chai.assert.equal(error.name, "ClientException");
    });
});
