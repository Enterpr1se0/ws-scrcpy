export class Event2 {
    static NONE = 0 as const;
    static CAPTURING_PHASE = 1 as const;
    static AT_TARGET = 2 as const;
    static BUBBLING_PHASE = 3 as const;

    public cancelable: boolean;
    public bubbles: boolean;
    public composed: boolean;
    public type: string;
    public defaultPrevented: boolean;
    public timeStamp: number;
    public target: any;
    public readonly isTrusted: boolean = true;
    readonly AT_TARGET: 2 = Event2.AT_TARGET;
    readonly BUBBLING_PHASE: 3 = Event2.BUBBLING_PHASE;
    readonly CAPTURING_PHASE: 1 = Event2.CAPTURING_PHASE;
    readonly NONE: 0 = Event2.NONE;

    constructor(type: string, options = { cancelable: true, bubbles: true, composed: false }) {
        const { cancelable, bubbles, composed } = { ...options };
        this.cancelable = !!cancelable;
        this.bubbles = !!bubbles;
        this.composed = !!composed;
        this.type = `${type}`;
        this.defaultPrevented = false;
        this.timeStamp = Date.now();
        this.target = null;
    }

    stopImmediatePropagation() {
        // this[kStop] = true;
    }

    preventDefault() {
        this.defaultPrevented = true;
    }

    get currentTarget() {
        return this.target;
    }
    get srcElement() {
        return this.target;
    }

    composedPath() {
        return this.target ? [this.target] : [];
    }
    get returnValue() {
        return !this.defaultPrevented;
    }
    get eventPhase() {
        return this.target ? Event.AT_TARGET : Event.NONE;
    }
    get cancelBubble() {
        return false;
        // return this.propagationStopped;
    }
    set cancelBubble(value: any) {
        if (value) {
            this.stopPropagation();
        }
    }
    stopPropagation() {
        // this.propagationStopped = true;
    }
    initEvent(type: string, bubbles?: boolean, cancelable?: boolean): void {
        this.type = type;
        if (arguments.length > 1) {
            this.bubbles = !!bubbles;
        }
        if (arguments.length > 2) {
            this.cancelable = !!cancelable;
        }
    }
}

export const EventClass = typeof Event !== 'undefined' ? Event : Event2;
