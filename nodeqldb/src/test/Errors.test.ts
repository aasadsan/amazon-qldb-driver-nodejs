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

import { AWSError } from "aws-sdk";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import {
    ClientException,
    DriverClosedError,
    isInvalidParameterException,
    isInvalidSessionException,
    isOccConflictException,
    isResourceNotFoundException,
    isResourcePreconditionNotMetException,
    isRetriableException,
    LambdaAbortedError,
    SessionClosedError,
    SessionPoolEmptyError,
    TransactionClosedError
} from "../errors/Errors";
import * as logUtil from "../logUtil";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testMessage: string = "foo";
const mockError: AWSError = <AWSError><any> sandbox.mock(AWSError);

describe("Errors test", () => {

    afterEach(() => {
        mockError.code = undefined;
        mockError.statusCode = undefined;
        sandbox.restore();
    });

    it("Test ClientException", () => {
        const logSpy = sandbox.spy(logUtil, "error");
        const error = new ClientException(testMessage);
        chai.assert.equal(error.name, "ClientException");
        chai.assert.equal(error.message, testMessage);
        sinon.assert.calledOnce(logSpy);
    });

    it("Test DriverClosedError", () => {
        const logSpy = sandbox.spy(logUtil, "error");
        const error = new DriverClosedError();
        chai.assert.equal(error.name, "DriverClosedError");
        sinon.assert.calledOnce(logSpy);
    });

    it("Test LambdaAbortedError", () => {
        const logSpy = sandbox.spy(logUtil, "error");
        const error = new LambdaAbortedError();
        chai.assert.equal(error.name, "LambdaAbortedError");
        sinon.assert.calledOnce(logSpy);
    });

    it("Test SessionClosedError", () => {
        const logSpy = sandbox.spy(logUtil, "error");
        const error = new SessionClosedError();
        chai.assert.equal(error.name, "SessionClosedError");
        sinon.assert.calledOnce(logSpy);
    });

    it("Test SessionPoolEmptyError", () => {
        const logSpy = sandbox.spy(logUtil, "error");
        const error = new SessionPoolEmptyError(1);
        chai.assert.equal(error.name, "SessionPoolEmptyError");
        sinon.assert.calledOnce(logSpy);
    });

    it("Test TransactionClosedError", () => {
        const logSpy = sandbox.spy(logUtil, "error");
        const error = new TransactionClosedError();
        chai.assert.equal(error.name, "TransactionClosedError");
        sinon.assert.calledOnce(logSpy);
    });

    it("Test isInvalidParameterException true", () => {
        mockError.code = "InvalidParameterException";
        chai.assert.isTrue(isInvalidParameterException(mockError));
    });

    it("Test isInvalidParameterException false", () => {
        mockError.code = "NotInvalidParameterException";
        chai.assert.isFalse(isInvalidParameterException(mockError));
    });

    it("Test isInvalidSessionException true", () => {
        mockError.code = "InvalidSessionException";
        chai.assert.isTrue(isInvalidSessionException(mockError));
    });

    it("Test isInvalidSessionException false", () => {
        mockError.code = "NotInvalidSessionException";
        chai.assert.isFalse(isInvalidSessionException(mockError));
    });

    it("Test isOccConflictException true", () => {
        mockError.code = "OccConflictException";
        chai.assert.isTrue(isOccConflictException(mockError));
    });

    it("Test isOccConflictException false", () => {
        mockError.code = "NotOccConflictException";
        chai.assert.isFalse(isOccConflictException(mockError));
    });

    it("Test isResourceNotFoundException true", () => {
        mockError.code = "ResourceNotFoundException";
        chai.assert.isTrue(isResourceNotFoundException(mockError));
    });

    it("Test isResourceNotFoundException false", () => {
        mockError.code = "NotResourceNotFoundException";
        chai.assert.isFalse(isResourceNotFoundException(mockError));
    });

    it("Test isResourcePreconditionNotMetException true", () => {
        mockError.code = "ResourcePreconditionNotMetException";
        chai.assert.isTrue(isResourcePreconditionNotMetException(mockError));
    });

    it("Test isResourcePreconditionNotMetException false", () => {
        mockError.code = "NotResourcePreconditionNotMetException";
        chai.assert.isFalse(isResourcePreconditionNotMetException(mockError));
    });

    it("Test isRetriableException with statusCode 500", () => {
        mockError.code = "NotRetriableException";
        mockError.statusCode = 500;
        chai.assert.isTrue(isRetriableException(mockError));
    });

    it("Test isRetriableException with statusCode 503", () => {
        mockError.code = "NotRetriableException";
        mockError.statusCode = 503;
        chai.assert.isTrue(isRetriableException(mockError));
    });

    it("Test isRetriableException with code NoHttpResponseException", () => {
        mockError.code = "NoHttpResponseException";
        mockError.statusCode = 200;
        chai.assert.isTrue(isRetriableException(mockError));
    });

    it("Test isRetriableException with code SocketTimeoutException", () => {
        mockError.code = "SocketTimeoutException";
        mockError.statusCode = 200;
        chai.assert.isTrue(isRetriableException(mockError));
    });

    it("Test isRetriableException false", () => {
        mockError.code = "NotRetriableException";
        mockError.statusCode = 200;
        chai.assert.isFalse(isRetriableException(mockError));
    });
});
