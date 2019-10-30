# Packages

### Driver

All source code files should live in `nodeqldb\src`.


### Unit Tests

All unit tests should live in `nodeqldb\src\test`.


### Sample App

All source code files should live in `samplecode\src` or its sub-directories.

Examples of driver usage all belong in the main directory.

`qldb` contains the document outlines for data returned from QLDB and any helper functions.

# Licenses

### Client and Unit Tests

All source files need to have the Apache license as a header.
```javascript
/** Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
```

### Sample App

 All source files need to have the MIT license as a header.
```javascript
/** Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
```

# Imports

### Client and Sample App

All imports should be ordered alphabetically, with third party imports at the top,
followed by local application imports, with a blank line separating the two categories.
Imports should also use the `import { foo } from "bar"` format.

e.g.:
```javascript
import { Writer } from "ion-js";
import { inspect } from "util";

import { isInvalidSessionException } from "./Errors";
```

# Function Ordering

### Client and Sample App

Functions should be ordered alphabetically, with the constructor at the top, followed by the static building function
if the constructor is private. Following this are the public functions, then the package-private ones, and finally the
private ones.

e.g.:
```javascript
class Foo {

    private constructor()  {}

    static async create(): Promise<Foo> {
        await importedModule.foo();
        return new Foo();
    }

    c(): string {
        return "c";
    }

    d(): string {
        return "d";
    }

    _b(): string {
        return "b";
    }

    private _a(): string {
        return "a";
    }
}
```

# Conditional Statements

### Client and Sample App

When comparing a value to a literal, the literal should be the first in the conditional expression, e.g.:
```javascript
let foo: number = 0;

if 0 === foo {
    // Example
}
```

To reduce clutter, any terminating lines resulting from a conditional statement should try to avoid the usage of
unnecessary `else`, e.g.:
```javascript
let foo: number = 0;

function good(): number {
    if 0 === foo {
        return 1;
    }
    return 2;
}


function bad(): number {
    if 0 === foo {
        return 1;
    } else {
        return 2;
    }
}
```

# Indentation

### Client, Unit Tests and Sample App

Each indentation should be 4 spaces, and tabs should not be used. No line should exceed 120 characters in length.

# Code spacing

### Client, Unit Tests and Sample App

Complex code should be separated into code blocks to improve readability.

# Use of Quotations

### Client, Unit Tests and Sample App

Double quotes should be used instead of single quotes where possible.
Backticks can be used when string interpolation is needed.

# Method and Variable Typing

### Client and Sample App

Although typing is optional in Typescript, typing for method parameters, method return types, and variable declarations
should be used.
```javascript
function good(foo: number): number {
    let bar: number = 0;
    if bar === foo {
        return 1;
    }
    return 0;
}
```

```javascript
function bad(foo) {
    let bar = 0;
    if bar === foo {
        return 1;
    }
    return 0;
}
```

# Function and Variable Scoping Syntax

### Client and Sample App

When transpiled into Javascript, the scoping of functions is somewhat lost, so keeping function styling consistent to
indicate that it should not be invoked is important. Functions that are used within the package but shouldn't be
invoked by a user of the package are prefixed with an underscore. Helper functions that exist within a class are denoted
with both the Typescript "private" modifier as well as prefixed with an underscore.

For variables, always use `const` when applicable. Else, use `let`. `var`, in almost all cases, should not be used.
```javascript
// Public
function public(): string {
    const foo = "foo";
    return foo;
}

// Package private
function _packagePrivate(): string {
    let foo = "fo";
    foo = "fo" + "o";
    return foo;
}

// Private
function private _private():string {
    return "foo";
}
```

# Loops and Anonymous Function Styling

### Client and Sample App

When iterating, use `forEach` when possible. When declaring anonymous functions, use the arrow syntax instead of the function syntax.

```javascript
const good = (foo: string[]): void => {
    foo.forEach((s: string): void => {
        console.log(s)
    });
}

const bad = function(foo: string[]): void {
    for (let i: number = 0; i < foo.length < i++) {
        console.log(foo[i]);
    }
    foo.forEach(function(s: string): void {
        console.log(s);
    });
}
```

# Docstrings

### Client and Sample App

Class and method docstrings will follow the TSDoc format:

```javascript
  /**
   * Description of the method
   *
   * @param x Description about the first parameter.
   * @param y Description about the second parameter.
   * @returns The return value of the method.
   */
  public static method(x: number, y: number): number {
    return (x + y) / 2.0;
  }
```

# Exceptions

### Client

Any exceptions thrown by the client should be unchecked exceptions. When possible, the client should not wrap exceptions from
the Amazon SDK, and they should bubble up as-is to the user.
