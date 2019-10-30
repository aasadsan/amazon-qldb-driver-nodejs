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

import { Page, ValueHolder } from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as ionJs from "ion-js";
import { Lock } from "semaphore-async-await";
import * as sinon from "sinon";

import { Communicator } from "../Communicator";
import { ClientException } from "../errors/Errors";
import { Result } from "../Result";
import { ResultStream } from "../ResultStream";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testTransactionId: string = "txnId";
const testValueHolder: ValueHolder = {
    IonBinary: "ionBinary"
};
const testValues: ValueHolder[] = [testValueHolder, testValueHolder, testValueHolder];
const testPage: Page = {
    Values: testValues
};
const testPageWithToken: Page = {
    Values: testValues,
    NextPageToken: "nextPageToken"
};

const mockCommunicator: Communicator = <Communicator><any> sandbox.mock(Communicator);
mockCommunicator.fetchPage = async () => {
    return testPage;
};

let resultStream: ResultStream;

describe("ResultStream test", () => {

    beforeEach(() => {
        resultStream = new ResultStream(testTransactionId, testPageWithToken, mockCommunicator);
    });

    afterEach(() => {
        sandbox.restore();
    });

    it("Test constructor", () => {
        chai.assert.equal(mockCommunicator, resultStream["_communicator"]);
        chai.assert.equal(testPageWithToken, resultStream["_cachedPage"]);
        chai.assert.equal(testTransactionId, resultStream["_txnId"]);
        chai.assert.isTrue(resultStream["_shouldPushCachedPage"]);
        chai.assert.equal(0, resultStream["_lastRetrievedIndex"]);
        chai.assert.isFalse(resultStream["_isClosed"]);
        chai.expect(resultStream["_lock"]).to.be.an.instanceOf(Lock);
    });

    it("Test close", () => {
        chai.assert.isFalse(resultStream["_isClosed"]);

        resultStream.close();
        chai.assert.isTrue(resultStream["_isClosed"]);
    });

    it("Test _read when not closed", () => {
        resultStream["_pushPageValues"] = async (): Promise<void> => {
            return;
        }
        const _pushPageValuesSpy = sandbox.spy(resultStream as any, "_pushPageValues");
        resultStream._read();
        sinon.assert.calledOnce(_pushPageValuesSpy);
    });

    it("Test _read when closed", () => {
        let caughtError: Error;
        resultStream.close();
        const _pushPageValuesSpy = sandbox.spy(resultStream as any, "_pushPageValues");

        try {
            resultStream._read();
        } catch (e) {
            caughtError = e;
        } finally {
            chai.expect(caughtError).to.be.an.instanceOf(ClientException);
            sinon.assert.notCalled(_pushPageValuesSpy);
        }
    });

    it("Test _pushPageValues when _shouldPushCachedPage and fully pushes and has next token", async () => {
        const _lockAcquireSpy = sandbox.spy(resultStream["_lock"] as any, "acquire");
        const _lockReleaseSpy = sandbox.spy(resultStream["_lock"] as any, "release");
        const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
        sandbox.stub(Result, "_handleBlob");
        const makeReaderStub = sandbox.stub(ionJs as any, "makeReader");
        makeReaderStub.onCall(0).returns(1);
        makeReaderStub.onCall(1).returns(2);
        makeReaderStub.returns(3);
        const pushStub = sandbox.stub(resultStream, "push");
        pushStub.returns(true);

        await resultStream["_pushPageValues"]();

        sinon.assert.notCalled(fetchPageSpy);
        sinon.assert.calledThrice(pushStub);
        sinon.assert.calledWith(pushStub.getCall(0), 1);
        sinon.assert.calledWith(pushStub.getCall(1), 2);
        sinon.assert.calledWith(pushStub.getCall(2), 3);
        sinon.assert.calledOnce(_lockAcquireSpy);
        sinon.assert.calledOnce(_lockReleaseSpy);
        chai.assert.isFalse(resultStream["_shouldPushCachedPage"]);
        chai.assert.isFalse(resultStream["_isClosed"]);
    });

    it("Test _pushPageValues when _shouldPushCachedPage and fully pushes and not has next token", async () => {
        resultStream["_cachedPage"] = testPage;
        const _lockAcquireSpy = sandbox.spy(resultStream["_lock"] as any, "acquire");
        const _lockReleaseSpy = sandbox.spy(resultStream["_lock"] as any, "release");
        const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
        sandbox.stub(Result, "_handleBlob");
        const makeReaderStub = sandbox.stub(ionJs as any, "makeReader");
        makeReaderStub.onCall(0).returns(1);
        makeReaderStub.onCall(1).returns(2);
        makeReaderStub.returns(3);
        const pushStub = sandbox.stub(resultStream, "push");
        pushStub.returns(true);

        await resultStream["_pushPageValues"]();

        sinon.assert.notCalled(fetchPageSpy);
        chai.assert.equal(pushStub.callCount, 4);
        sinon.assert.calledWith(pushStub.getCall(0), 1);
        sinon.assert.calledWith(pushStub.getCall(1), 2);
        sinon.assert.calledWith(pushStub.getCall(2), 3);
        sinon.assert.calledWith(pushStub.getCall(3), null);
        sinon.assert.calledOnce(_lockAcquireSpy);
        sinon.assert.calledOnce(_lockReleaseSpy);
        chai.assert.isFalse(resultStream["_shouldPushCachedPage"]);
        chai.assert.isTrue(resultStream["_isClosed"]);
    });

    it("Test _pushPageValues when not _shouldPushCachedPage", async () => {
        resultStream["_shouldPushCachedPage"] = false;
        const _lockAcquireSpy = sandbox.spy(resultStream["_lock"] as any, "acquire");
        const _lockReleaseSpy = sandbox.spy(resultStream["_lock"] as any, "release");
        const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
        sandbox.stub(Result, "_handleBlob");
        const makeReaderStub = sandbox.stub(ionJs as any, "makeReader");
        makeReaderStub.onCall(0).returns(1);
        makeReaderStub.onCall(1).returns(2);
        makeReaderStub.returns(3);
        const pushStub = sandbox.stub(resultStream, "push");
        pushStub.returns(true);

        await resultStream["_pushPageValues"]();

        sinon.assert.called(fetchPageSpy);
        chai.assert.equal(pushStub.callCount, 4);
        sinon.assert.calledWith(pushStub.getCall(0), 1);
        sinon.assert.calledWith(pushStub.getCall(1), 2);
        sinon.assert.calledWith(pushStub.getCall(2), 3);
        sinon.assert.calledWith(pushStub.getCall(3), null);
        sinon.assert.calledOnce(_lockAcquireSpy);
        sinon.assert.calledOnce(_lockReleaseSpy);
        chai.assert.isFalse(resultStream["_shouldPushCachedPage"]);
        chai.assert.isTrue(resultStream["_isClosed"]);
    });

    it("Test _pushPageValues when not fully pushes", async () => {
        const _lockAcquireSpy = sandbox.spy(resultStream["_lock"] as any, "acquire");
        const _lockReleaseSpy = sandbox.spy(resultStream["_lock"] as any, "release");
        const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
        sandbox.stub(Result, "_handleBlob");
        const makeReaderStub = sandbox.stub(ionJs as any, "makeReader");
        makeReaderStub.onCall(0).returns(1);
        makeReaderStub.onCall(1).returns(2);
        makeReaderStub.onCall(2).returns(3);
        makeReaderStub.returns(4);
        const pushStub = sandbox.stub(resultStream, "push");
        pushStub.onCall(0).returns(true);
        pushStub.onCall(1).returns(false);
        pushStub.returns(true);

        await resultStream["_pushPageValues"]();

        sinon.assert.calledTwice(makeReaderStub);
        sinon.assert.calledTwice(pushStub);
        chai.assert.isTrue(resultStream["_shouldPushCachedPage"]);
        chai.assert.equal(1, resultStream["_lastRetrievedIndex"]);

        await resultStream["_pushPageValues"]();

        sinon.assert.notCalled(fetchPageSpy);
        chai.assert.equal(pushStub.callCount, 4);
        sinon.assert.calledWith(pushStub.getCall(0), 1);
        sinon.assert.calledWith(pushStub.getCall(1), 2);
        sinon.assert.calledWith(pushStub.getCall(2), 3);
        sinon.assert.calledWith(pushStub.getCall(3), 4);
        sinon.assert.calledTwice(_lockAcquireSpy);
        sinon.assert.calledTwice(_lockReleaseSpy);
        chai.assert.isFalse(resultStream["_shouldPushCachedPage"]);
        chai.assert.isFalse(resultStream["_isClosed"]);
    });
 });
