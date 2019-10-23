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

describe("Errors test", () => {

    afterEach(() => {
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
        const mockError = {code: "InvalidParameterException"};
        chai.assert.isTrue(isInvalidParameterException(mockError));
    });

    it("Test isInvalidParameterException false", () => {
        const mockError = {code: "NotInvalidParameterException"};
        chai.assert.isFalse(isInvalidParameterException(mockError));
    });

    it("Test isInvalidSessionException true", () => {
        const mockError = {code: "InvalidSessionException"};
        chai.assert.isTrue(isInvalidSessionException(mockError));
    });

    it("Test isInvalidSessionException false", () => {
        const mockError = {code: "NotInvalidSessionException"};
        chai.assert.isFalse(isInvalidSessionException(mockError));
    });

    it("Test isOccConflictException true", () => {
        const mockError = {code: "OccConflictException"};
        chai.assert.isTrue(isOccConflictException(mockError));
    });

    it("Test isOccConflictException false", () => {
        const mockError = {code: "NotOccConflictException"};
        chai.assert.isFalse(isOccConflictException(mockError));
    });

    it("Test isResourceNotFoundException true", () => {
        const mockError = {code: "ResourceNotFoundException"};
        chai.assert.isTrue(isResourceNotFoundException(mockError));
    });

    it("Test isResourceNotFoundException false", () => {
        const mockError = {code: "NotResourceNotFoundException"};
        chai.assert.isFalse(isResourceNotFoundException(mockError));
    });

    it("Test isResourcePreconditionNotMetException true", () => {
        const mockError = {code: "ResourcePreconditionNotMetException"};
        chai.assert.isTrue(isResourcePreconditionNotMetException(mockError));
    });

    it("Test isResourcePreconditionNotMetException false", () => {
        const mockError = {code: "NotResourcePreconditionNotMetException"};
        chai.assert.isFalse(isResourcePreconditionNotMetException(mockError));
    });

    it("Test isRetriableException with statusCode 500", () => {
        const mockError = {statusCode: 500, code: "NotRetriableException"};
        chai.assert.isTrue(isRetriableException(mockError));
    });

    it("Test isRetriableException with statusCode 503", () => {
        const mockError = {statusCode: 503, code: "NotRetriableException"};
        chai.assert.isTrue(isRetriableException(mockError));
    });

    it("Test isRetriableException with code NoHttpResponseException", () => {
        const mockError = {code: "NoHttpResponseException", statusCode: 200};
        chai.assert.isTrue(isRetriableException(mockError));
    });

    it("Test isRetriableException with code SocketTimeoutException", () => {
        const mockError = {code: "SocketTimeoutException", statusCode: 200};
        chai.assert.isTrue(isRetriableException(mockError));
    });

    it("Test isRetriableException false", () => {
        const mockError = {code: "NotRetriableException", statusCode: 200};
        chai.assert.isFalse(isRetriableException(mockError));
    });
});
