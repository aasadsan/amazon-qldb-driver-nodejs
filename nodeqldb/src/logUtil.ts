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

import { config } from "aws-sdk";

/**
 * Logs a debug level message.
 * @param line The message to be logged.
 */
export function debug(line: any): void {
    if (isLoggerSet()) {
        _prepend(line, "DEBUG");
    }
}

/**
 * Logs an error level message.
 * @param line The message to be logged.
 */
export function error(line: any): void {
    if (isLoggerSet()) {
        _prepend(line, "ERROR");
    }
}

/**
 * Logs an info level message.
 * @param line The message to be logged.
 */
export function info(line: any): void {
    if (isLoggerSet()) {
        _prepend(line, "INFO");
    }
}

/**
 * @returns A boolean indicating whether a logger has been set within the AWS SDK.
 */
export function isLoggerSet(): boolean {
    return config.logger !== null;
}

/**
 * Logs a message.
 * @param line The message to be logged.
 */
export function log(line: any): void {
    if (isLoggerSet()) {
        _prepend(line, "LOG");
    }
}

/**
 * Logs a warning level message.
 * @param line The message to be logged.
 */
export function warn(line: any): void {
    if (isLoggerSet()) {
        _prepend(line, "WARN");
    }
}

/**
 * Prepends a string identifier indicating the log level to the given log message, & writes or logs the given message
 * using the logger set in the AWS SDK.
 * @param line The message to be logged.
 * @param level The log level.
 */
function _prepend(line: any, level: string): void {
    if (typeof config.logger.log === "function") {
        config.logger.log(`[${level}][Javascript QLDB Driver, Version: ${process.env.npm_package_version}] ${line}`);
    } else if (typeof config.logger.write === "function") {
        config.logger.write(
            `[${level}][Javascript QLDB Driver, Version: ${process.env.npm_package_version}] ${line}\n`
        );
    }
}
