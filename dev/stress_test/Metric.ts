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

import { appendFileSync } from "fs";

/**
 * Represents a test metric, measuring the amount of time an action took.
 */
export class Metric {
    private _name: string;
    private _fileName: string;
    private _value: number = 0;
    private _valueCount: number = 0;
    private _minValue: number = 0;
    private _maxValue: number = 0;

    /**
     * Creates a Metric.
     * @param name The name of the metric.
     * @param fileName The filename to write metrics to.
     */
    constructor(name: string, fileName: string) {
        this._name = name;
        this._fileName = fileName;
    }

    /**
     * Give a data point, as a measure of time in milliseconds, to this metric.
     * @param value A data point, as a measure of time in milliseconds.
     */
    give(value: number): void {
        if (value > this._maxValue || 0 === this._maxValue) {
            this._maxValue = value;
        }
        if (value < this._minValue || 0 === this._minValue) {
            this._minValue = value;
        }
        this._value += value;
        this._valueCount++;
    }

    /**
     * Write all the metrics of the action to the file.
     */
    print(): void {
        const results: string = 
            `\n ${this._name} metrics:` +
            `\n Average time (ms): ${this._getAverage()}` +
            `\n Minimum time (ms): ${this._minValue}` +
            `\n Maximum time (ms): ${this._maxValue}` +
            `\n Number of requests: ${this._valueCount}`;

        appendFileSync(`${this._fileName}.txt`, results);
    }

    /**
     * Get the average time that the measured action took.
     * @returns The average time of the measured action in milliseconds.
     */
    private _getAverage(): number {
        if (0 === this._valueCount) {
            return 0;
        }
        return Math.fround(this._value / this._valueCount);
    }
 }
