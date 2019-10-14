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

import { Writer, makeBinaryWriter } from "ion-js";

import * as logUtil from "../logUtil";
import { Result } from "../Result";
import { ResultStream } from "../ResultStream";
import { Transaction } from "../Transaction";
import { TransactionExecutor } from "../TransactionExecutor";

const chai = require("chai");
const chaiAsPromised = require("chai-as-promised");
chai.use(chaiAsPromised);
const sinon = require("sinon");
const sandbox = sinon.createSandbox();

let mockStatement: string = "SELECT * FROM foo";
let mockMessage: string = "foo";
let mockTransactionId: string = "txnId";

describe('TransactionExecutor test', function() {
  let mockResult: Result = sandbox.mock(Result);
  let mockResultStream: ResultStream = sandbox.mock(ResultStream);
  let mockTransaction: Transaction = sandbox.mock(Transaction);
  let mockWriter: Writer = makeBinaryWriter();
  mockWriter.writeString(mockMessage);
  let transactionExecutor: TransactionExecutor;

  beforeEach(function () {
    transactionExecutor = new TransactionExecutor(mockTransaction);
  });

  after(function () {
    sandbox.restore();
  });

  it('Test Abort', async function() {
    mockTransaction.abort = async function () {};
    let abortSpy = sandbox.spy(mockTransaction, "abort");
    let errorMessage = await chai.expect(transactionExecutor.abort()).to.be.rejected;
    chai.assert.equal(errorMessage.name, "LambdaAbortedError");
    sinon.assert.calledOnce(abortSpy);
  });

  it('Test Abort When Exception', async function() {
    mockTransaction.abort = async function () {throw new Error(mockMessage);};
    let abortSpy = sandbox.spy(mockTransaction, "abort");
    let logWarnSpy = sandbox.spy(logUtil, "warn");
    let logErrorSpy = sandbox.spy(logUtil, "error");
    let errorMessage = await chai.expect(transactionExecutor.abort()).to.be.rejected;
    chai.assert.equal(errorMessage.name, "LambdaAbortedError");
    sinon.assert.calledOnce(logWarnSpy);
    sinon.assert.calledOnce(logErrorSpy);
    sinon.assert.calledOnce(abortSpy);
  });
  
  it('Test Execute Query Inline', async function() {
    mockTransaction.executeInline = async function () {return mockResult};
    let transactionExecuteSpy = sandbox.spy(mockTransaction, "executeInline");
    let result = await transactionExecutor.executeInline(mockStatement);
    chai.assert.equal(mockResult, result);
    sinon.assert.calledOnce(transactionExecuteSpy);
    sinon.assert.calledWith(transactionExecuteSpy, mockStatement);
  });

  it('Test Execute Query Stream', async function() {
    mockTransaction.executeStream = async function () {return mockResultStream};
    let transactionExecuteSpy = sandbox.spy(mockTransaction, "executeStream");
    let resultStream = await transactionExecutor.executeStream(mockStatement);
    chai.assert.equal(mockResultStream, resultStream);
    sinon.assert.calledOnce(transactionExecuteSpy);
    sinon.assert.calledWith(transactionExecuteSpy, mockStatement);
  });

  it('Test Execute Query Inline with Error', async function() {
    mockTransaction.executeInline = async function () {throw new Error(mockMessage);};
    let transactionExecuteSpy = sandbox.spy(mockTransaction, "executeInline");
    let errorMessage = await chai.expect(transactionExecutor.executeInline(mockStatement)).to.be.rejected;
    chai.assert.equal(errorMessage.name, "Error");
    sinon.assert.calledOnce(transactionExecuteSpy);
    sinon.assert.calledWith(transactionExecuteSpy, mockStatement);
  });

  it('Test Execute Query Stream with Error', async function() {
    mockTransaction.executeStream = async function () {throw new Error(mockMessage);};
    let transactionExecuteSpy = sandbox.spy(mockTransaction, "executeStream");
    let errorMessage = await chai.expect(transactionExecutor.executeStream(mockStatement)).to.be.rejected;
    chai.assert.equal(errorMessage.name, "Error");
    sinon.assert.calledOnce(transactionExecuteSpy);
    sinon.assert.calledWith(transactionExecuteSpy, mockStatement);
  });    

  it('Test Get Transaction ID', async function() {
    mockTransaction.getTransactionId = function () {return mockTransactionId;};
    let transactionIdSpy = sandbox.spy(mockTransaction, "getTransactionId");
    let transactionId = transactionExecutor.getTransactionId();
    chai.assert.equal(transactionId, mockTransactionId);
    sinon.assert.calledOnce(transactionIdSpy);
  }); 
  
  it('Test Register Parameter', async function() {
    mockTransaction.registerParameter = function () {return mockWriter;};
    let registerParamSpy = sandbox.spy(mockTransaction, "registerParameter");
    let writer = transactionExecutor.registerParameter(1);
    chai.assert.equal(writer, mockWriter);
    sinon.assert.calledOnce(registerParamSpy);
    sinon.assert.calledWith(registerParamSpy, 1);
  }); 
})