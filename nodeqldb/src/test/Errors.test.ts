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

import { 
  ClientException,
  LambdaAbortedError,
  SessionClosedError,
  TransactionClosedError,
  DriverClosedError,
  isOccConflictException, 
  isRetriableException,
  isInvalidSessionException,
  isInvalidParameterException } from "../errors/Errors";
  import * as logUtil from "../logUtil";

const chai = require("chai");
const chaiAsPromised = require("chai-as-promised");
chai.use(chaiAsPromised);
const sinon = require("sinon");
const sandbox = sinon.createSandbox();

let mockMessage: string = "foo";

describe('Errors test', function() {

    afterEach(function () {
      sandbox.restore();
    });
    
    it('Test ClientException', function() {
      let logSpy = sandbox.spy(logUtil, "error");
      let error = new ClientException(mockMessage);
      chai.assert.equal(error.name, "ClientException");
      chai.assert.equal(error.message, mockMessage);
      sinon.assert.calledOnce(logSpy);
    });

    it('Test SessionClosedError', function() {
      let logSpy = sandbox.spy(logUtil, "error");
      let error = new SessionClosedError();
      chai.assert.equal(error.name, "SessionClosedError");
      sinon.assert.calledOnce(logSpy);
    });

    it('Test TransactionClosedError', function() {
      let logSpy = sandbox.spy(logUtil, "error");
      let error = new TransactionClosedError();
      chai.assert.equal(error.name, "TransactionClosedError");
      sinon.assert.calledOnce(logSpy);
    });

    it('Test DriverClosedError', function() {
      let logSpy = sandbox.spy(logUtil, "error");
      let error = new DriverClosedError();
      chai.assert.equal(error.name, "DriverClosedError");
      sinon.assert.calledOnce(logSpy);
    });

    it('Test LambdaAbortedError', function() {
      let logSpy = sandbox.spy(logUtil, "error");
      let error = new LambdaAbortedError();
      chai.assert.equal(error.name, "LambdaAbortedError");
      sinon.assert.calledOnce(logSpy);
    });

    it('Test IsOccConflictException True', function() {
      let mockError = {code: "OccConflictException"};
      chai.assert.isTrue(isOccConflictException(mockError));
    });

    it('Test IsOccConflictException False', function() {
      let mockError = {code: "NotOccConflictException"};
      chai.assert.isFalse(isOccConflictException(mockError));
    });

    it('Test IsInvalidSessionException True', function() {
      let mockError = {code: "InvalidSessionException"};
      chai.assert.isTrue(isInvalidSessionException(mockError));
    });

    it('Test IsInvalidSessionException False', function() {
      let mockError = {code: "NotInvalidSessionException"};
      chai.assert.isFalse(isInvalidSessionException(mockError));
    });

    it('Test IsInvalidParameterException True', function() {
      let mockError = {code: "InvalidParameterException"};
      chai.assert.isTrue(isInvalidParameterException(mockError));
    });

    it('Test IsInvalidParameterException False', function() {
      let mockError = {code: "NotInvalidParameterException"};
      chai.assert.isFalse(isInvalidParameterException(mockError));
    });

    it('Test IsRetriableException StatusCode 500', function() {
      let mockError = {statusCode: 500, code: "NotRetriableException"};
      chai.assert.isTrue(isRetriableException(mockError));
    });

    it('Test IsRetriableException StatusCode 503', function() {
      let mockError = {statusCode: 503, code: "NotRetriableException"};
      chai.assert.isTrue(isRetriableException(mockError));
    });

    it('Test IsRetriableException Code NoHttpResponseException', function() {
      let mockError = {code: "NoHttpResponseException", statusCode: 200};
      chai.assert.isTrue(isRetriableException(mockError));
    });

    it('Test IsRetriableException Code SocketTimeoutException', function() {
      let mockError = {code: "SocketTimeoutException", statusCode: 200};
      chai.assert.isTrue(isRetriableException(mockError));
    });

    it('Test IsRetriableException False', function() {
      let mockError = {code: "NotRetriableException", statusCode: 200};
      chai.assert.isFalse(isRetriableException(mockError));
    });
})