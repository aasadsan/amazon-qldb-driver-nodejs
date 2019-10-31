# Stress Test Results

## Background

The goal of this stress test is to provide performance benchmarks and to keep metrics of different driver and SDK versions.

## Methodology

The stress test was designed to record latency for three key functionalites (start transaction, execute, and fetch page). Time is recorded when the functionality is invoked and stopped when the result is returned back. In order to test concurrency, the test is run in a series of promises which is called with Promise.all().

## Results
| Driver/SDK version | Start Transaction | Execute Transaction | Fetch page |
|----------------------------------------------------------------|---------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| Driver: 0.1.0-beta.1 ion-js SDK: 3.0.0 ion-hash-js SDK: 0.1.0  | Average time (ms): 355 Minimum time (ms): 316 Maximum time (ms): 690 Number of requests: 30 | Average time (ms): 354 Minimum time (ms): 316 Maximum time (ms): 690 Number of requests: 30 | Average time (ms): 340 Minimum time (ms): 305 Maximum time (ms): 939 Number of requests: 60 |
| Driver: 0.1.0-beta.1 ion-js SDK: 3.1.2 ion-hash-js SDK: 1.0.0 | Average time (ms): 343 Minimum time (ms): 316 Maximum time (ms): 484 Number of requests: 30 | Average time (ms): 402 Minimum time (ms): 333 Maximum time (ms): 1342 Number of requests: 30 | Average time (ms): 325 Minimum time (ms): 305 Maximum time (ms): 679 Number of requests: 60 |
