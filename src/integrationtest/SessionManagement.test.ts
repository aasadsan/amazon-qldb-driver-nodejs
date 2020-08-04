/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import { AWSError } from "aws-sdk";
import { ClientConfiguration } from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import { dom } from "ion-js";

import { isTransactionExpiredException, DriverClosedError, SessionPoolEmptyError } from "../errors/Errors";
import { QldbDriver } from "../QldbDriver";
import { Result } from "../Result";
import { TransactionExecutor } from "../TransactionExecutor";
import * as constants from "./TestConstants";
import { TestUtils } from "./TestUtils";
import { defaultRetryPolicy } from "../retry/DefaultRetryPolicy";

chai.use(chaiAsPromised);

describe("SessionManagement", function() {
    this.timeout(0);
    let testUtils: TestUtils;
    let config: ClientConfiguration; 

    before(async () => {
        testUtils = new TestUtils(constants.LEDGER_NAME);
        config = testUtils.createClientConfiguration();

        await testUtils.runForceDeleteLedger();
        await testUtils.runCreateLedger();

        // Create table
        const driver: QldbDriver = new QldbDriver(constants.LEDGER_NAME, config);
        const statement: string = `CREATE TABLE ${constants.TABLE_NAME}`;
        const count: number = await driver.executeLambda(async (txn: TransactionExecutor): Promise<number> => {
            const result: Result = await txn.execute(statement);
            const resultSet: dom.Value[] = result.getResultList();
            return resultSet.length;
        });
        chai.assert.equal(count, 1);
        await new Promise(r => setTimeout(r, 3000));
    });

    after(async () => {
        await testUtils.runDeleteLedger();
    });

    it("Throws exception when connecting to a non-existent ledger", async () => {
        const driver: QldbDriver = new QldbDriver("NonExistentLedger", config);
        let error: AWSError;
        try {
            error = await chai.expect(driver.executeLambda(async (txn: TransactionExecutor) => {
                await txn.execute(`SELECT name FROM ${constants.TABLE_NAME} WHERE name='Bob'`);
            })).to.be.rejected;

        } finally {
            chai.assert.equal(error.code, "BadRequestException");
            driver.close();
        }
    });

    it("Can get a session when the pool has no sessions and hasn't hit the pool limit", async () => {
        // Start a pooled driver with default pool limit so it doesn't have sessions in the pool
        // and has not hit the limit
        const driver: QldbDriver = new QldbDriver(constants.LEDGER_NAME, config);
        try {
            // Execute a statement to implicitly create a session and return it to the pool
            await driver.executeLambda(async (txn: TransactionExecutor) => {
                await txn.execute(`SELECT name FROM ${constants.TABLE_NAME} WHERE name='Bob'`);
            });
        } finally {
            driver.close();
        }
    });

    it("Throws exception when all the sessions are busy and pool limit is reached", async () => {
        // Set the timeout to 1ms and pool limit to 1
        const driver: QldbDriver = new  QldbDriver(constants.LEDGER_NAME, config, 1, defaultRetryPolicy);
        try {
            // Execute and do not wait for the promise to resolve, exhausting the pool
            driver.executeLambda(async (txn: TransactionExecutor) => {
                await txn.execute(`SELECT name FROM ${constants.TABLE_NAME} WHERE name='Bob'`);
            });
            // Attempt to implicitly get a session by executing
            await driver.executeLambda(async (txn: TransactionExecutor) => {
                await txn.execute(`SELECT name FROM ${constants.TABLE_NAME} WHERE name='Bob'`);
            });
            chai.assert.fail("SessionPoolEmptyError was not thrown")
        } catch (e) {
            if (!(e instanceof SessionPoolEmptyError)) {
                throw e;
            }
        } finally {
            driver.close();
        }
    });

    it("Throws exception when the driver has been closed", async () => {
        const driver: QldbDriver = new QldbDriver(constants.LEDGER_NAME, config);
        driver.close();
        try {
            await driver.executeLambda(async (txn: TransactionExecutor) => {
                await txn.execute(`SELECT name FROM ${constants.TABLE_NAME} WHERE name='Bob'`);
            });
        } catch (e) {
            if (!(e instanceof DriverClosedError)) {
                throw e;
            }
        }
    });

    it("Throws exception when transaction expires due to timeout", async() => {
        const driver: QldbDriver = new QldbDriver(constants.LEDGER_NAME, config);
        let error;
        try {
            error = await chai.expect(driver.executeLambda(async (txn: TransactionExecutor) => {
                //Wait for transaction to expire
                await new Promise(resolve => setTimeout(resolve, 40000));
            })).to.be.rejected;
            console.log("error in the test ", error);
        } finally {
            console.log("error in the finally block", error);
            chai.assert.isTrue(isTransactionExpiredException(error));
        }
    });
});
