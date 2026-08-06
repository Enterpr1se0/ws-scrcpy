import { Buffer as NodeBuffer } from 'buffer';

// The app (transplanted from a webpack setup with `buffer` ProvidePlugin)
// relies on the Node.js `Buffer` global being available in the browser.
const w = globalThis as unknown as { Buffer?: typeof NodeBuffer };
w.Buffer = NodeBuffer;

export {};
