# Logging

### How to enable logging

All levels of logging are displayed and filtering by verbosity is not currently supported. To enable logging to the 
console:

1. The QLDB node driver has aws-sdk as a peer dependency. Therefore, the module that uses the driver as a dependency
   must install aws-sdk as it's own dependency.

2. Configure the AWS logger to log to the console, for example.

```javascript
import AWS = require("aws-sdk");

AWS.config.logger = console;
```

Third-party logging can also be used, provided it has log() or write() operations to write to a log file or server.
For more information: https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/logging-sdk-calls.html

An example of writing to a file with level DEBUG:
```javascript
import AWS = require("aws-sdk");
let logplease = require("logplease");

logplease.setLogfile("debug.log");
// Note that since AWS SDK only supports log(), all log statements regardless of level are logged.
// This setting just sets the log level of all log statements in the file to DEBUG.
logplease.setLogLevel("DEBUG");
let logger = logplease.create("QLDB app logplease logger");

AWS.config.logger = logger;
```
