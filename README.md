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

### Typescript 3.5.x

The driver requires Typescript 3.5.x. Please see the link below for more detail to install Typescript 3.5.x:

* [Typesccript 3.5.x Installation](https://www.npmjs.com/package/typescript)

## Installing the driver and running the driver

First, install the driver using npm:

```npm install amazon-qldb-driver-nodejs --save-dev```


Then from a Typescript file, call the driver and specify the ledger name:

```javascript
import { PooledQldbDriver, QldbSession } fromt "amazon-qldb-driver-nodejs";

const testServiceConfigOptions = {
    region: "us-east-1"
};

const qldbDriver: PooledQldbDriver = new PooledQldbDriver(testServiceConfigOptions, "testLedger");
const qldbSession: QldbSession = await qldbDriver.getSession();

for (const table of await qldbSession.getTableNames()) {
    console.log(table);
}
```

## Development

### Running Tests

You can run the unit tests with this command:

```
$ npm run testWithCoverage
```

The performance tests have a separate README.md within the performance folder.

### Documentation 

TypeDoc is used for documentation. You can generate HTML locally with the following:

```
# Install the global CLI
$ npm install --global typedoc

# Execute typedoc on your project
$ typedoc --out docs /src
```
