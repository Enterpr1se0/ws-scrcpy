import type { Plugin } from 'vite';
import { readFileSync } from 'fs';

// Servers `.svg` imports as raw string content, mirroring the old
// `svg-inline-loader` behaviour used by the app (SVGs are inlined into the
// bundle as markup, not emitted as separate asset files).
export function viteSvgStringPlugin(): Plugin {
    return {
        name: 'ws-scrcpy-svg-string',
        enforce: 'pre',
        async load(id) {
            if (!/\.svg(\?[^#]*)?$/.test(id)) {
                return null;
            }
            // Let Vite handle explicit query-based imports (url / raw / asset)
            if (id.includes('?')) {
                return null;
            }
            const filePath = id.split('?')[0];
            const content = readFileSync(filePath, 'utf8');
            return {
                code: `export default ${JSON.stringify(content)};`,
                map: null,
            };
        },
    };
}
