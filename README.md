# AmazonQLDB Node.js Driver

This is the Node.js driver for Amazon Quantum Ledger Database (QLDB), which allows Node.js developers
to write software that makes use of AmazonQLDB.

## Requirements

### Basic Configuration

You need to set up your AWS security credentials and config before the driver is able to connect to AWS. 

Set up credentials (in e.g. `~/.aws/credentials`):

```
[default]
aws_access_key_id = <your access key id>
aws_secret_access_key = <your secret key>
```

Set up a default region (in e.g. `~/.aws/config`):

```
[default]
region = us-east-1 <or other region>
```

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html#SettingUp.Q.GetCredentials) page for more information.

The AWS SDK needs to have AWS_SDK_LOAD_CONFIG environment variable set to a truthy value as well in order to pull the
version from the ~./.aws/config file.

See [Setting Region](https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/setting-region.html) page for more information.

### TypeScript 3.5.x

The driver is written in, and requires, TypeScript 3.5.x. It will be automatically installed as a dependency. 
Please see the link below for more detail on TypeScript 3.5.x:

* [TypeScript 3.5.x](https://www.npmjs.com/package/typescript)

## Installing the Driver

To install the driver, run the following in the root directory of the project:

```npm install```

To build the driver, transpiling the TypeScript source code to JavaScript, run the following in the root directory:

```npm run build```

## Using the Driver as a Dependency

To use the driver, in your package that wishes to use the driver, run the following:

```npm install amazon-qldb-driver-nodejs```

The driver also has aws-sdk and ion-js as peer dependencies. Thus, they must also be dependencies of the package that
will be using the driver as a dependency.

```npm install aws-sdk```

```npm install ion-js```

Then from within your package, you can now use the driver by importing it. This example shows usage in TypeScript 
specifying the QLDB ledger name and a specific region:

```javascript
import { PooledQldbDriver, QldbSession } from "amazon-qldb-driver-nodejs";

const testServiceConfigOptions = {
    region: "us-east-1"
};

const qldbDriver: PooledQldbDriver = new PooledQldbDriver("testLedger", testServiceConfigOptions));
const qldbSession: QldbSession = await qldbDriver.getSession();

for (const table of await qldbSession.getTableNames()) {
    console.log(table);
}
```

## Development

### Running Tests

You can run the unit tests with this command:

```npm test```

or

```npm run testWithCoverage```

### Documentation 

TypeDoc is used for documentation. You can generate HTML locally with the following:

```npm run doc```
