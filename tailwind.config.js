module.exports = {
    content: ['./src/**/*.{ts,html,css}', './src/public/index.html'],
    corePlugins: {
        preflight: false,
    },
    theme: {
        extend: {
            colors: {
                console: {
                    canvas: 'rgb(var(--console-canvas) / <alpha-value>)',
                    panel: 'rgb(var(--console-panel) / <alpha-value>)',
                    muted: 'rgb(var(--console-muted) / <alpha-value>)',
                    border: 'rgb(var(--console-border) / <alpha-value>)',
                    text: 'rgb(var(--console-text) / <alpha-value>)',
                    subdued: 'rgb(var(--console-subdued) / <alpha-value>)',
                    accent: 'rgb(var(--console-accent) / <alpha-value>)',
                    ok: 'rgb(var(--console-ok) / <alpha-value>)',
                    warn: 'rgb(var(--console-warn) / <alpha-value>)',
                    danger: 'rgb(var(--console-danger) / <alpha-value>)',
                    focus: 'rgb(var(--console-focus) / <alpha-value>)',
                },
            },
            fontFamily: {
                console: ['"IBM Plex Mono"', '"Cascadia Mono"', 'Consolas', 'monospace'],
                control: ['"Aptos"', '"Segoe UI"', 'sans-serif'],
            },
            boxShadow: {
                console: '0 18px 60px rgb(0 0 0 / 0.34)',
                insetConsole: 'inset 0 1px 0 rgb(255 255 255 / 0.06)',
            },
            borderRadius: {
                console: '0.625rem',
            },
        },
    },
};
