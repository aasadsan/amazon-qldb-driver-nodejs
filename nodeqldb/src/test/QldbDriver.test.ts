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
import { ClientConfiguration, SendCommandResult } from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import { DriverClosedError } from "../errors/Errors";
import { QldbDriver } from "../QldbDriver";
import { QldbSession } from "../QldbSession";

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

let qldbDriver: QldbDriver;
let sendCommandStub;
let testQldbLowLevelClient: QLDBSession;

describe("QldbDriver test", () => {
    beforeEach(() => {
        testQldbLowLevelClient = new QLDBSession(testLowLevelClientOptions);
        sendCommandStub = sandbox.stub(testQldbLowLevelClient, "sendCommand");
        sendCommandStub.returns({
            promise: () => {
                return testSendCommandResult;
            }
        });

        qldbDriver = new QldbDriver(testLowLevelClientOptions, testLedgerName);
    });

    afterEach(() => {
        sandbox.restore();
    });

    it("Test constructor", () => {
        chai.assert.equal(qldbDriver["_ledgerName"], testLedgerName);
        chai.assert.equal(qldbDriver["_retryLimit"], testDefaultRetryLimit);
        chai.assert.equal(qldbDriver["_isClosed"], false);
        chai.assert.instanceOf(qldbDriver["_qldbClient"], QLDBSession);
        chai.assert.equal(qldbDriver["_qldbClient"].config.maxRetries, testMaxRetries);
    });

    it("Test constructor with retryLimit less than zero", () => {
        const constructorFunction: Function = () => {
            new QldbDriver(testLowLevelClientOptions, testLedgerName, -1);
        };
        chai.assert.throws(constructorFunction, RangeError);
    });

    it("Test close", () => {
        qldbDriver.close();
        chai.assert.equal(qldbDriver["_isClosed"], true);
    });

    it("Test getSession", async () => {
        qldbDriver["_qldbClient"] = testQldbLowLevelClient;

        const qldbSession: QldbSession = await qldbDriver.getSession();
        chai.assert.equal(qldbSession["_retryLimit"], testDefaultRetryLimit);
        chai.assert.equal(qldbSession["_communicator"]["_ledgerName"], testLedgerName);
        chai.assert.equal(qldbSession["_communicator"]["_qldbClient"], testQldbLowLevelClient);
    });

    it("Test getSession when driver is closed", async () => {
        qldbDriver["_isClosed"] = true;
        const error = await chai.expect(qldbDriver.getSession()).to.be.rejected;
        chai.assert.instanceOf(error, DriverClosedError);
    });
});
