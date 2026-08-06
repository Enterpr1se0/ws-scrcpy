import path from 'path';
import fs from 'fs';
import { defineConfig, type Plugin } from 'vite';
import { viteSvgStringPlugin } from './scripts/vite-plugin-svg-string';

const PROJECT_ROOT = path.resolve(__dirname);
const OUT_DIR = path.join(PROJECT_ROOT, '../go-backend/web/public');

// Keep a `.gitkeep` placeholder after the build so the tracked directory
// survives in git while the generated assets stay untracked.
function keepGitkeep(): Plugin {
    return {
        name: 'ws-scrcpy-keep-gitkeep',
        closeBundle() {
            fs.writeFileSync(path.join(OUT_DIR, '.gitkeep'), '');
        },
    };
}

export default defineConfig({
    root: PROJECT_ROOT,
    publicDir: path.join(PROJECT_ROOT, 'src/public'),
    plugins: [viteSvgStringPlugin(), keepGitkeep()],
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
        outDir: OUT_DIR,
        emptyOutDir: true,
        rollupOptions: {
            input: path.join(PROJECT_ROOT, 'index.html'),
        },
    },
});
