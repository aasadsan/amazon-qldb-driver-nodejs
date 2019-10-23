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

import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import * as logUtil from "../logUtil";
import { SessionClosedError } from "../errors/Errors";
import { PooledQldbSession } from "../PooledQldbSession";
import { QldbSessionImpl } from "../QldbSessionImpl";
import { Result } from "../Result";
import { Transaction } from "../Transaction";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testLambda = () => logUtil.log("Test returning session to pool...");
const testLedgerName: string = "fakeLedgerName";
const testMessage: string = "foo";
const testSessionToken: string = "sessionToken";
const testStatement: string = "SELECT * FROM foo";
const testTableNames: string[] = ["Vehicle", "Person"];

const mockQldbSession: QldbSessionImpl = <QldbSessionImpl><any> sandbox.mock(QldbSessionImpl);
const mockResult: Result = <Result><any> sandbox.mock(Result);
const mockTransaction: Transaction = <Transaction><any> sandbox.mock(Transaction);

let pooledQldbSession: PooledQldbSession;

describe("PooledQldbSession test", () => {

    beforeEach(() => {
        pooledQldbSession = new PooledQldbSession(mockQldbSession, testLambda);
        mockQldbSession.getLedgerName = () => {
            return testLedgerName;
        };
        mockQldbSession.executeStatement = async () => {
            return mockResult;
        };
        mockQldbSession.executeLambda = async () => {
            return mockResult;
        };
        mockQldbSession.getSessionToken = () => {
            return testSessionToken;
        };
        mockQldbSession.getTableNames = async () => {
            return testTableNames;
        };
        mockQldbSession.startTransaction = async () => {
            return mockTransaction;
        };
    });

    afterEach(() => {
        sandbox.restore();
    });

    it("Test close", () => {
        const logSpy = sandbox.spy(logUtil, "log");
        pooledQldbSession.close();
        chai.assert.equal(pooledQldbSession["_isClosed"], true);
        sinon.assert.calledOnce(logSpy);
        sinon.assert.calledWith(logSpy, "Test returning session to pool...");
    });

    it("Test close when already closed", () => {
        const logSpy = sandbox.spy(logUtil, "log");
        pooledQldbSession["_isClosed"] = true;
        pooledQldbSession.close();
        sinon.assert.notCalled(logSpy);
    });

    it("Test executeLambda", async () => {
        const executeSpy = sandbox.spy(mockQldbSession, "executeLambda");
        const query = async (txn) => {
            return await txn.executeInline(testStatement);
        };
        const retryIndicator = () => {};
        const result: Result = await pooledQldbSession.executeLambda(query, retryIndicator);
        chai.assert.equal(result, mockResult);
        sinon.assert.calledOnce(executeSpy);
        sinon.assert.calledWith(executeSpy, query, retryIndicator);
    });

    it("Test executeLambda when closed", async () => {
        pooledQldbSession["_isClosed"] = true;
        const executeSpy = sandbox.spy(mockQldbSession, "executeLambda");
        const error = await chai.expect(pooledQldbSession.executeLambda(async (txn) => {
            return await txn.executeInline(testStatement);
        })).to.be.rejected;
        chai.assert.equal(error.name, "SessionClosedError");
        sinon.assert.notCalled(executeSpy);
    });

    it("Test executeLambda with exception", async () => {
        mockQldbSession.executeLambda = async () => {
            throw new Error(testMessage);
        };
        const executeSpy = sandbox.spy(mockQldbSession, "executeLambda");
        await chai.expect(pooledQldbSession.executeLambda(async (txn) => {
            return await txn.executeInline(testStatement);
        })).to.be.rejected;
        sinon.assert.calledOnce(executeSpy);
    });

    it("Test executeStatement", async () => {
        const executeSpy = sandbox.spy(mockQldbSession, "executeStatement");
        const result: Result = await pooledQldbSession.executeStatement(testStatement);
        chai.assert.equal(result, mockResult);
        sinon.assert.calledOnce(executeSpy);
    });

    it("Test executeStatement when closed", async () => {
        pooledQldbSession["_isClosed"] = true;
        const executeSpy = sandbox.spy(mockQldbSession, "executeStatement");
        const error = await chai.expect(pooledQldbSession.executeStatement(testStatement)).to.be.rejected;
        chai.assert.equal(error.name, "SessionClosedError");
        sinon.assert.notCalled(executeSpy);
    });

    it("Test executeStatement with exception", async () => {
        mockQldbSession.executeStatement = async () => {
            throw new Error(testMessage);
        };
        const executeSpy = sandbox.spy(mockQldbSession, "executeStatement");
        await chai.expect(pooledQldbSession.executeStatement(testStatement)).to.be.rejected;
        sinon.assert.calledOnce(executeSpy);
    });

    it("Test getLedgerName", () => {
        const ledgerNameSpy = sandbox.spy(mockQldbSession, "getLedgerName");
        const ledgerName: string = pooledQldbSession.getLedgerName();
        chai.assert.equal(ledgerName, testLedgerName);
        sinon.assert.calledOnce(ledgerNameSpy);
    });

    it("Test getLedgerName when closed", () => {
        pooledQldbSession["_isClosed"] = true;
        const ledgerNameSpy = sandbox.spy(mockQldbSession, "getLedgerName");
        chai.expect(() => {
            pooledQldbSession.getLedgerName();
        }).to.throw(SessionClosedError);
        sinon.assert.notCalled(ledgerNameSpy);
    });

    it("Test getSessionToken", () => {
        const sessionTokenSpy = sandbox.spy(mockQldbSession, "getSessionToken");
        const sessionToken: string = pooledQldbSession.getSessionToken();
        chai.assert.equal(sessionToken, testSessionToken);
        sinon.assert.calledOnce(sessionTokenSpy);
    });

    it("Test getSessionToken when closed", () => {
        pooledQldbSession["_isClosed"] = true;
        const sessionTokenSpy = sandbox.spy(mockQldbSession, "getSessionToken");
        chai.expect(() => {
            pooledQldbSession.getSessionToken();
        }).to.throw(SessionClosedError);
        sinon.assert.notCalled(sessionTokenSpy);
    });

    it("Test getTableNames", async () => {
        const tableNamesSpy = sandbox.spy(mockQldbSession, "getTableNames");
        const tableNames: string[] = await pooledQldbSession.getTableNames();
        chai.assert.equal(tableNames, testTableNames);
        sinon.assert.calledOnce(tableNamesSpy);
    });

    it("Test getTableNames when closed", async () => {
        pooledQldbSession["_isClosed"] = true;
        const tableNamesSpy = sandbox.spy(mockQldbSession, "getTableNames");
        const error = await chai.expect(pooledQldbSession.getTableNames()).to.be.rejected;
        chai.assert.equal(error.name, "SessionClosedError");
        sinon.assert.notCalled(tableNamesSpy);
    });

    it("Test startTransaction", async () => {
        const transactionSpy = sandbox.spy(mockQldbSession, "startTransaction");
        const transaction: Transaction = await pooledQldbSession.startTransaction();
        chai.assert.equal(transaction, mockTransaction);
        sinon.assert.calledOnce(transactionSpy);
    });

    it("Test startTransaction when closed", async () => {
        pooledQldbSession["_isClosed"] = true;
        const transactionSpy = sandbox.spy(mockQldbSession, "startTransaction");
        const error = await chai.expect(pooledQldbSession.startTransaction()).to.be.rejected;
        chai.assert.equal(error.name, "SessionClosedError");
        sinon.assert.notCalled(transactionSpy);
    });

    it("Test startTransaction with exception", async () => {
        mockQldbSession.startTransaction = async () => {
            throw new Error(testMessage);
        };
        const transactionSpy = sandbox.spy(mockQldbSession, "startTransaction");
        await chai.expect(pooledQldbSession.startTransaction()).to.be.rejected;
        sinon.assert.calledOnce(transactionSpy);
    });

    it("Test _throwIfClosed when closed", () => {
        pooledQldbSession["_isClosed"] = true;
        chai.expect(() => {
            pooledQldbSession["_throwIfClosed"]();
        }).to.throw(SessionClosedError);
    });

    it("Test _throwIfClosed when not closed", () => {
        pooledQldbSession["_throwIfClosed"]();
        chai.assert.equal(pooledQldbSession["_isClosed"], false);
        pooledQldbSession.close();
        chai.assert.equal(pooledQldbSession["_isClosed"], true);
    });
});
