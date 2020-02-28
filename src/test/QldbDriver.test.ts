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
import { ClientConfiguration, SendCommandResult } from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import { DriverClosedError } from "../errors/Errors";
import { QldbDriver } from "../QldbDriver";
import { QldbSession } from "../QldbSession";
import { Result } from "../Result";
import { TransactionExecutor } from "../TransactionExecutor";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testDefaultRetryLimit: number = 4;
const testLedgerName: string = "LedgerName";
const testLowLevelClientOptions: ClientConfiguration = {
    region: "fakeRegion"
};
const testMaxRetries: number = 0;
const testSendCommandResult: SendCommandResult = {
    StartSession: {
        SessionToken: "sessionToken"
    }
};
const mockResult: Result = <Result><any> sandbox.mock(Result);
const mockQldbSession: QldbSession = <QldbSession><any> sandbox.mock(QLDBSession);
mockQldbSession.executeLambda = async () => {
    return mockResult;
}
mockQldbSession.executeStatement = async () => {
    return mockResult;
}
mockQldbSession.close = () => {
    return;
}

let qldbDriver: QldbDriver;
let sendCommandStub;
let testQldbLowLevelClient: QLDBSession;

describe("QldbDriver", () => {
    beforeEach(() => {
        testQldbLowLevelClient = new QLDBSession(testLowLevelClientOptions);
        sendCommandStub = sandbox.stub(testQldbLowLevelClient, "sendCommand");
        sendCommandStub.returns({
            promise: () => {
                return testSendCommandResult;
            }
        });

        qldbDriver = new QldbDriver(testLedgerName, testLowLevelClientOptions);
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe("#constructor()", () => {
        it("should have all attributes equal to mock values when constructor called", () => {
            chai.assert.equal(qldbDriver["_ledgerName"], testLedgerName);
            chai.assert.equal(qldbDriver["_retryLimit"], testDefaultRetryLimit);
            chai.assert.equal(qldbDriver["_isClosed"], false);
            chai.assert.instanceOf(qldbDriver["_qldbClient"], QLDBSession);
            chai.assert.equal(qldbDriver["_qldbClient"].config.maxRetries, testMaxRetries);
        });

        it("should throw RangeError when retryLimit less than zero passed in", () => {
            const constructorFunction: Function = () => {
                new QldbDriver(testLedgerName, testLowLevelClientOptions, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });
    });

    describe("#close()", () => {
        it("should close qldbDriver when called", () => {
            qldbDriver.close();
            chai.assert.equal(qldbDriver["_isClosed"], true);
        });
    });

    describe("#executeLambda()", () => {
        it("should start a session and return the delegated call to the session", async () => {
            const getSessionStub = sandbox.stub(qldbDriver, "getSession");
            getSessionStub.returns(Promise.resolve(mockQldbSession));
            const executeLambdaSpy = sandbox.spy(mockQldbSession, "executeLambda");
            const closeSessionSpy = sandbox.spy(mockQldbSession, "close");
            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };
            const retryIndicator = (retry: number) => {
                return;
            };
            const result = await qldbDriver.executeLambda(lambda, retryIndicator);

            chai.assert.equal(result, mockResult);
            sinon.assert.calledOnce(executeLambdaSpy);
            sinon.assert.calledWith(executeLambdaSpy, lambda, retryIndicator);
            sinon.assert.calledOnce(closeSessionSpy);
        });

        it("should throw DriverClosedError wrapped in a rejected promise when closed", async () => {
            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };
            const retryIndicator = (retry: number) => {
                return;
            };
            
            qldbDriver["_isClosed"] = true;
            const error = await chai.expect(qldbDriver.executeLambda(lambda, retryIndicator)).to.be.rejected;
            chai.assert.instanceOf(error, DriverClosedError);
        });
    });

    describe("#executeStatement()", () => {
        it("should start a session and return the delegated call to the session", async () => {
            const getSessionStub = sandbox.stub(qldbDriver, "getSession");
            getSessionStub.returns(Promise.resolve(mockQldbSession));
            const executeStatementSpy = sandbox.spy(mockQldbSession, "executeStatement");
            const closeSessionSpy = sandbox.spy(mockQldbSession, "close");
            const statement = "statement";
            const parameters = [];
            const retryIndicator = (retry: number) => {
                return;
            };
            const result = await qldbDriver.executeStatement(statement, parameters, retryIndicator);

            chai.assert.equal(result, mockResult);
            sinon.assert.calledOnce(executeStatementSpy);
            sinon.assert.calledWith(executeStatementSpy, statement, parameters, retryIndicator);
            sinon.assert.calledOnce(closeSessionSpy);
        });

        it("should throw DriverClosedError wrapped in a rejected promise when closed", async () => {
            const statement = "statement";
            const parameters = [];
            const retryIndicator = (retry: number) => {
                return;
            };
            
            qldbDriver["_isClosed"] = true;
            const error = await chai.expect(qldbDriver.executeStatement(statement, parameters, retryIndicator)).to.be.rejected;
            chai.assert.instanceOf(error, DriverClosedError);
        });
    });

    describe("#getSession()", () => {
        it("should return a QldbSession object when called", async () => {
            qldbDriver["_qldbClient"] = testQldbLowLevelClient;

            const qldbSession: QldbSession = await qldbDriver.getSession();
            chai.assert.equal(qldbSession["_retryLimit"], testDefaultRetryLimit);
            chai.assert.equal(qldbSession["_communicator"]["_ledgerName"], testLedgerName);
            chai.assert.equal(qldbSession["_communicator"]["_qldbClient"], testQldbLowLevelClient);
        });

        it("should return a DriverClosedError wrapped in a rejected promise when closed", async () => {
            qldbDriver["_isClosed"] = true;
            const error = await chai.expect(qldbDriver.getSession()).to.be.rejected;
            chai.assert.instanceOf(error, DriverClosedError);
        });
    });
});
