import { IOUsage as ConsumedIOs } from "aws-sdk/clients/qldbsession";

export class IOUsage {
    private _readIOs: number;

    constructor(readIOs: number) {
        this._readIOs = readIOs;
    }


    getReadIOs(): number {
        return this._readIOs;
    }

    private _setReadIOs(value: number) {
        this._readIOs = value;
    }

    accumulateIOUsage(consumedIOs: ConsumedIOs) {
        if (consumedIOs != null) {
            this._setReadIOs(this.getReadIOs() + consumedIOs.ReadIOs);
        }
    }

}
