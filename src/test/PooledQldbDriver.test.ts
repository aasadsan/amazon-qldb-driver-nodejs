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
import { ClientConfiguration, SendCommandResult, ValueHolder } from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import { Agent } from "https";
import * as Errors from "../errors/Errors";
import Semaphore from "semaphore-async-await";
import * as sinon from "sinon";

import { DriverClosedError, SessionPoolEmptyError } from "../errors/Errors";
import * as LogUtil from "../LogUtil";
import { PooledQldbDriver } from "../PooledQldbDriver";
import { QldbSession } from "../QldbSession";
import { Result } from "../Result";
import { TransactionExecutor } from "../TransactionExecutor";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testDefaultTimeout: number = 30000;
const testDefaultRetryLimit: number = 4;
const testLedgerName: string = "LedgerName";
const testMaxRetries: number = 0;
const testMaxSockets: number = 10;
const testMessage: string = "testMessage";
const mockSessionToken: string = "sessionToken1";
const testTableNames: string[] = ["Vehicle", "Person"];
const testSendCommandResult: SendCommandResult = {
    StartSession: {
        SessionToken: "sessionToken"
    }
};

let pooledQldbDriver: PooledQldbDriver;
let sendCommandStub;
let testQldbLowLevelClient: QLDBSession;

const mockAgent: Agent = <Agent><any> sandbox.mock(Agent);
mockAgent.maxSockets = testMaxSockets;
const testLowLevelClientOptions: ClientConfiguration = {
    region: "fakeRegion",
    httpOptions: {
        agent: mockAgent
    }
};

const mockResult: Result = <Result><any> sandbox.mock(Result);
const mockQldbSession: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
mockQldbSession.executeLambda = async () => {
    return mockResult;
}
mockQldbSession.endSession = () => {
    return;
}
mockQldbSession.getSessionToken = () => {
    return mockSessionToken;
}

mockQldbSession.isSessionOpen = () => {
    return true;
}

describe("PooledQldbDriver", () => {
    beforeEach(() => {
        testQldbLowLevelClient = new QLDBSession(testLowLevelClientOptions);
        sendCommandStub = sandbox.stub(testQldbLowLevelClient, "sendCommand");
        sendCommandStub.returns({
            promise: () => {
                return testSendCommandResult;
            }
        });

        pooledQldbDriver = new PooledQldbDriver(testLedgerName, testLowLevelClientOptions);
    });

    afterEach(() => {
        mockAgent.maxSockets = testMaxSockets;
        sandbox.restore();
    });

    describe("#constructor()", () => {
        it("should have all attributes equal to mock values when constructor called", () => {
            chai.assert.equal(pooledQldbDriver["_ledgerName"], testLedgerName);
            chai.assert.equal(pooledQldbDriver["_retryLimit"], testDefaultRetryLimit);
            chai.assert.equal(pooledQldbDriver["_isClosed"], false);
            chai.assert.instanceOf(pooledQldbDriver["_qldbClient"], QLDBSession);
            chai.assert.equal(pooledQldbDriver["_qldbClient"].config.maxRetries, testMaxRetries);
            chai.assert.equal(pooledQldbDriver["_timeoutMillis"], testDefaultTimeout);
            chai.assert.equal(pooledQldbDriver["_poolLimit"], mockAgent.maxSockets);
            chai.assert.equal(pooledQldbDriver["_availablePermits"], mockAgent.maxSockets);
            chai.assert.deepEqual(pooledQldbDriver["_sessionPool"], []);
            chai.assert.instanceOf(pooledQldbDriver["_semaphore"], Semaphore);
            chai.assert.equal(pooledQldbDriver["_semaphore"]["permits"], mockAgent.maxSockets);
        });

        it("should throw a RangeError when timeOutMillis less than zero passed in", () => {
            const constructorFunction: () => void = () => {
                new PooledQldbDriver(testLedgerName, testLowLevelClientOptions, 4, 0, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

        it("should throw a RangeError when retryLimit less than zero passed in", () => {
            const constructorFunction: () => void = () => {
                new PooledQldbDriver(testLedgerName, testLowLevelClientOptions, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

        it("should throw a RangeError when poolLimit greater than maxSockets", () => {
            const constructorFunction: () => void  = () => {
                new PooledQldbDriver(testLedgerName, testLowLevelClientOptions, 4, testMaxSockets + 1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

        it("should throw a RangeError when poolLimit less than zero", () => {
            const constructorFunction: () => void = () => {
                new PooledQldbDriver(testLedgerName, testLowLevelClientOptions, 4, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });
    });

    describe("#close()", () => {
        it("should close pooledQldbDriver and any session present in the pool when called", () => {
            const mockSession1: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            const mockSession2: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            mockSession1.endSession = () => {};
            mockSession2.endSession = () => {};

            const close1Spy = sandbox.spy(mockSession1, "endSession");
            const close2Spy = sandbox.spy(mockSession2, "endSession");

            pooledQldbDriver["_sessionPool"] = [mockSession1, mockSession2];
            pooledQldbDriver.close();

            sinon.assert.calledOnce(close1Spy);
            sinon.assert.calledOnce(close2Spy);
            chai.assert.equal(pooledQldbDriver["_isClosed"], true);
        });
    });

    describe("#executeLambda()", () => {
        it("should start a session and return the delegated call to the session", async () => {
            pooledQldbDriver["_sessionPool"] = [mockQldbSession];
            const semaphoreStub = sandbox.stub(pooledQldbDriver["_semaphore"], "waitFor");
            semaphoreStub.returns(Promise.resolve(true));

            const executeLambdaSpy = sandbox.spy(mockQldbSession, "executeLambda");
            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };

            const retryIndicator = (retry: number) => {
                return;
            };
            const result = await pooledQldbDriver.executeLambda(lambda, retryIndicator);

            chai.assert.equal(result, mockResult);
            sinon.assert.calledOnce(executeLambdaSpy);
            sinon.assert.calledWith(executeLambdaSpy, lambda, retryIndicator);
        });

        /**
         * This test covers the following rules:
         *   1) If the permit is available, and there is/are session(s) in the pool, then return the last session from the pool
         *   2) If the session throws InvalidSessionException, then do not return that session back to the pool. Also,
         *   the driver will proceed with the next available session in the pool.
         *   3) If the session is good, then return it to the pool.
         */
        it("should pick next session from the pool when the current session throws InvalidSessionException", async() => {
            const mockSession1: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            const mockSession2: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);

            const isInvalidSessionStub = sandbox.stub(Errors, "isInvalidSessionException");
            isInvalidSessionStub.returns(true);

            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };

            const retryIndicator = (retry: number) => {
                return;
            };

            mockSession1.executeLambda = async () => {
                mockSession1.closeSession();
                throw new Error("InvalidSession");
            };

            mockSession1.getSessionToken = () => {
                return "sessionToken1";
            }

            mockSession1.isSessionOpen = () => {
                return false;
            }

            mockSession2.executeLambda = async () => {
                return true;
            };

            mockSession2.getSessionToken = () => {
                return "sessionToken2";
            }

            mockSession2.isSessionOpen = () => {
                return true;
            }

            pooledQldbDriver["_sessionPool"] = [mockSession2, mockSession1];
            const semaphoreStub = sandbox.stub(pooledQldbDriver["_semaphore"], "waitFor");
            semaphoreStub.returns(Promise.resolve(true));

            let initialPermits = pooledQldbDriver["_availablePermits"];
            const result = await pooledQldbDriver.executeLambda(lambda, retryIndicator);

            //Ensure that the transaction was eventually completed
            chai.assert.isTrue(result);
            //Ensure that the mockSession1 is not returned back to the pool since it threw ISE. Only mockSession2 should be present
            chai.assert.equal(pooledQldbDriver["_sessionPool"].length, 1);
            chai.assert.equal(pooledQldbDriver["_sessionPool"][0].getSessionToken(), mockSession2.getSessionToken());
            // Ensure that although mockSession1 is not returned to the pool the total number of permits are same before beginning
            // the transaction
            chai.assert.equal(pooledQldbDriver["_availablePermits"], initialPermits);
        });

        it("should throw DriverClosedError wrapped in a rejected promise when closed", async () => {
            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };
            const retryIndicator = (retry: number) => {
                return;
            };

            pooledQldbDriver["_isClosed"] = true;
            const error = await chai.expect(pooledQldbDriver.executeLambda(lambda, retryIndicator)).to.be.rejected;
            chai.assert.instanceOf(error, DriverClosedError);
        });

        it("should return a SessionPoolEmptyError wrapped in a rejected promise when session pool empty", async () => {
            const semaphoreStub = sandbox.stub(pooledQldbDriver["_semaphore"], "waitFor");
            semaphoreStub.returns(Promise.resolve(false));

            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };

            const error = await chai.expect(pooledQldbDriver.executeLambda(lambda)).to.be.rejected;
            chai.assert.instanceOf(error, SessionPoolEmptyError);
        });

    });


    describe("#releaseSession()", () => {
        it("should return a session back to the session pool when called", () => {
            const logDebugSpy = sandbox.spy(LogUtil, "debug");
            const semaphoreReleaseSpy = sandbox.spy(pooledQldbDriver["_semaphore"], "release")
            pooledQldbDriver["_returnSessionToPool"](mockQldbSession);

            chai.assert.deepEqual(pooledQldbDriver["_sessionPool"], [mockQldbSession])
            chai.assert.deepEqual(pooledQldbDriver["_availablePermits"], testMaxSockets + 1)

            sinon.assert.calledOnce(logDebugSpy);
            sinon.assert.calledOnce(semaphoreReleaseSpy);
        });

        it("should NOT return a closed session back to the pool but should release the permit", () => {
            const semaphoreReleaseSpy = sandbox.spy(pooledQldbDriver["_semaphore"], "release");
            let initalPermits = pooledQldbDriver["_availablePermits"];

            mockQldbSession.isSessionOpen = () => {
                return false;
            };

            pooledQldbDriver["_returnSessionToPool"](mockQldbSession);
            //Since the session was not open, it won't be returneed to the pool
            chai.assert.deepEqual(pooledQldbDriver["_sessionPool"], []);
            //The permit is released even if session is not returned to the pool
            chai.assert.deepEqual(pooledQldbDriver["_availablePermits"], initalPermits + 1);
            sinon.assert.calledOnce(semaphoreReleaseSpy);
        });
    });

    describe("#getTableNames()", () => {
        it("should return a list of table names when called", async () => {
            const executeStub = sandbox.stub(pooledQldbDriver, "executeLambda");
            executeStub.returns(Promise.resolve(testTableNames));
            const listOfTableNames: string[] = await pooledQldbDriver.getTableNames();
            chai.assert.equal(listOfTableNames.length, testTableNames.length);
            chai.assert.equal(listOfTableNames, testTableNames);
        });

        it("should return a DriverClosedError wrapped in a rejected promise when closed", async () => {
            pooledQldbDriver["_isClosed"] = true;
            const error = await chai.expect(pooledQldbDriver.getTableNames()).to.be.rejected;
            chai.assert.instanceOf(error, DriverClosedError);
        });
    });
});
