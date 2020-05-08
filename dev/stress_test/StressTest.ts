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

import { isInvalidSessionException, PooledQldbDriver, QldbSession, Transaction } from "amazon-qldb-driver-nodejs";
import { Readable } from "stream";

import { log } from "./logUtil";
import { Metric } from "./Metric";

const DEFAULT_DURATION_MS: string = "10000";
const DEFAULT_MULTI_QUERY_TXN : string = "false";
const DEFAULT_NUMBER_OF_CONCURRENT_SESSIONS: string = "5";
const FILE_NAME: string = "StressTestResults";
const LEDGER_NAME: string = "MultiThreadStressTest";
const TABLE_NAME: string = "StressTest";
const SELECT_QUERY: string = `SELECT * FROM ${TABLE_NAME}`;

let args: string[] = [DEFAULT_NUMBER_OF_CONCURRENT_SESSIONS, DEFAULT_DURATION_MS, DEFAULT_MULTI_QUERY_TXN];

/**
 * Iterate through result stream and update fetch page metric.
 * @param resultStream A a result stream containing the statement execution returned from QLDB.
 * @param fetchPageMetric The metric measuring the amount of time it took to iterate through results.
 * @returns Promise which fulfills with void. 
 */
async function fetchPages(resultStream: Readable, fetchPageMetric: Metric): Promise<void> {
    return new Promise(res => {
        let i: number = 0;
        let startFetchPageTime: number = Date.now();
        resultStream.on("data", (reader) => {
            if (0 !== i && 0 === (i % 200)) {
                fetchPageMetric.give(Date.now() - startFetchPageTime);
                startFetchPageTime = Date.now();
            }
            i++;
        }).on("end", () => {
            res();
        });
    });
}

/**
 * Query QLDB and read through all the results of the query, feeding data points to the metrics, until the testing 
 * duration is reached.
 * @param pooledQldbDriver The pooled QLDB driver to create sessions and transactions to execute queries on.
 * @param isFinished Represents the state of if the testing duration has been reached.
 * @param queriesPerTransaction The number of queries to execute per transaction.
 * @param startTransactionMetric The metric for starting a transaction.
 * @param executeMetric The metric for executing a query.
 * @param fetchPageMetric The metric for reading the next page of the result.
 * @returns Promise which fulfills with void. 
 */
async function testSuite(
    pooledQldbDriver: PooledQldbDriver,
    isFinished: { value: boolean },
    queriesPerTransaction: number,
    startTransactionMetric: Metric,
    executeMetric: Metric,
    fetchPageMetric: Metric
): Promise<void> {
    let pooledQldbSession: QldbSession;
    try {
        pooledQldbSession = await pooledQldbDriver.getSession();

        while (!isFinished.value) {
            try {
                let transaction: Transaction = await startTransaction(pooledQldbSession, startTransactionMetric);
                let i: number = 0;
                while (i < queriesPerTransaction && !isFinished.value) {
                    let startExecuteTime: number = Date.now();
                    let resultStream: Readable = await transaction.executeStream(SELECT_QUERY);
                    executeMetric.give(Date.now() - startExecuteTime);
                    await fetchPages(resultStream, fetchPageMetric);
                    i++;
                }
                await transaction.abort();
            } catch (e) {
                if (isInvalidSessionException(e)) {
                    log(e);
                    pooledQldbSession.close();
                    pooledQldbSession = await pooledQldbDriver.getSession();
                } else {
                    throw e;
                }
            }
        }
    } catch (e) {
        log(e);
    } finally {
        if (null != pooledQldbSession) {
            pooledQldbSession.close();
        }
    }
}

/**
 * Start a transaction with the given pooled QLDB session and update start transaction metric.
 * @param pooledQldbSession The pooled session object to start a transaction.
 * @param startTransactionMetric The metric measuring the amount of time it took to start a transaction.
 * @returns Promise which fulfills with the new transaction. 
 */
async function startTransaction(
    pooledQldbSession: QldbSession,
    startTransactionMetric: Metric
): Promise<Transaction> {
    const startTransactionTime: number = Date.now();
    const transaction: Transaction = await pooledQldbSession.startTransaction();
    startTransactionMetric.give(Date.now() - startTransactionTime);
    return transaction;
}

/**
 * Run the stress test and write the results to a log file.
 * 
 * The command line arguments; the first 3 configure the test. An invalid or no input causes the default to be used.
 * 1. The number of concurrent sesssions. Must be an integer greater than 0. Defaults to 5.
 * 2. The duration of the test in milliseconds. Must be an integer greater than 0. Defaults to 10000.
 * 3. Flag indicating whether or not to use multiple queries per transaction. Must be "true" to enable.
 *    Defaults to "false".
 */
var main = async () => {
    const commandLineArgs: string[] = process.argv.slice(2);
    for (let i: number = 0; i < commandLineArgs.length; i++) {
        if (2 != i) {
            try {
                if (parseInt(commandLineArgs[i]) < 1) {
                    throw TypeError("Value cannot be less than 1.")
                }
                args[i] = commandLineArgs[i];
            } catch (TypeError) {
                log(
                    "Input value for number of concurrent sessions or duration is not a number;" +
                    "falling back to default values."
                );
            }
        } else {
            args[i] = commandLineArgs[i];
        }
    }

    let queriesPerTransaction: number = 1;
    if ("true" === args[2]) {
        queriesPerTransaction = Number.MAX_SAFE_INTEGER;
    }

    const isFinished: { value: boolean } = { value: false };
    const pooledQldbDriver: PooledQldbDriver = new PooledQldbDriver(LEDGER_NAME);

    const startTransactionMetric: Metric = new Metric("StartTransaction", FILE_NAME);
    const executeMetric: Metric = new Metric("ExecuteMetric", FILE_NAME);
    const fetchPageMetric: Metric = new Metric("FetchPageMetric", FILE_NAME);
    const promises: Promise<void>[] = [];

    setTimeout(() => {
        isFinished.value = true;
    }, parseInt(args[1]));

    for (let i: number = 0; i < parseInt(args[0]); i++) {
        promises.push(testSuite(
            pooledQldbDriver,
            isFinished,
            queriesPerTransaction,
            startTransactionMetric,
            executeMetric,
            fetchPageMetric
            )
        );
    }

    await Promise.all(promises);

    startTransactionMetric.print();
    executeMetric.print();
    fetchPageMetric.print();
}

if (require.main === module) {
    main();
}