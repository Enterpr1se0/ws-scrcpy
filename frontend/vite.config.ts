import path from 'path';
import { defineConfig } from 'vite';
import { viteSvgStringPlugin } from './scripts/vite-plugin-svg-string';

const PROJECT_ROOT = path.resolve(__dirname);

export default defineConfig({
    root: PROJECT_ROOT,
    publicDir: path.join(PROJECT_ROOT, 'src/public'),
    plugins: [viteSvgStringPlugin()],
    resolve: {
        alias: {
            events: "events",
            path: 'path-browserify',
        },
    },
    optimizeDeps: {
        include: ['h264-converter', 'h264-converter/dist/h264-parser', 'h264-converter/dist/util/NALU'],
    },
    build: {
        outDir: path.join(PROJECT_ROOT, '../go-backend/web/public'),
        emptyOutDir: true,
        rollupOptions: {
            input: path.join(PROJECT_ROOT, 'index.html'),
        },
    },
});
