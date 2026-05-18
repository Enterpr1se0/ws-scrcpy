import { MessageType } from './MessageType';
import Util from '../../app/Util';
import { CloseEventClass } from './CloseEventClass';

type BinaryPayload = ArrayBufferLike | ArrayBufferView;

export class Message {
    private static toArrayBuffer(data: Uint8Array): ArrayBuffer {
        const array = new Uint8Array(data.byteLength);
        array.set(data);
        return array.buffer;
    }

    public static parse(buffer: ArrayBuffer): Message {
        const view = Buffer.from(buffer);

        const type: MessageType = view.readUInt8(0);
        const channelId = view.readUInt32LE(1);
        const data: ArrayBuffer = buffer.slice(5);

        return new Message(type, channelId, data);
    }

    public static fromCloseEvent(id: number, code: number, reason?: string): Message {
        const reasonBuffer = reason ? Util.stringToUtf8ByteArray(reason) : Buffer.alloc(0);
        const buffer = Buffer.alloc(2 + 4 + reasonBuffer.byteLength);
        buffer.writeUInt16LE(code, 0);
        if (reasonBuffer.byteLength) {
            buffer.writeUInt32LE(reasonBuffer.byteLength, 2);
            buffer.set(reasonBuffer, 6);
        }
        return new Message(MessageType.CloseChannel, id, Message.toArrayBuffer(buffer));
    }

    public static createBuffer(type: MessageType, channelId: number, data?: BinaryPayload): Buffer {
        const input = data
            ? ArrayBuffer.isView(data)
                ? Buffer.from(data.buffer, data.byteOffset, data.byteLength)
                : Buffer.from(data)
            : undefined;
        const result = Buffer.alloc(5 + (input ? input.byteLength : 0));
        result.writeUInt8(type, 0);
        result.writeUInt32LE(channelId, 1);
        if (input?.byteLength) {
            result.set(input, 5);
        }
        return result;
    }

    public constructor(
        public readonly type: MessageType,
        public readonly channelId: number,
        public readonly data: ArrayBuffer,
    ) {}

    public toCloseEvent(): CloseEvent {
        let code: number | undefined;
        let reason: string | undefined;
        if (this.data && this.data.byteLength) {
            const buffer = Buffer.from(this.data);
            code = buffer.readUInt16LE(0);
            if (buffer.byteLength > 6) {
                const length = buffer.readUInt32LE(2);
                reason = Util.utf8ByteArrayToString(buffer.slice(6, 6 + length));
            }
        }
        return new CloseEventClass('close', {
            code,
            reason,
            wasClean: code === 1000,
        });
    }

    public toBuffer(): Buffer {
        return Message.createBuffer(this.type, this.channelId, this.data);
    }
}
