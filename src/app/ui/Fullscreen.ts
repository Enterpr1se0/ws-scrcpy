const TAG = '[Fullscreen]';

export default class Fullscreen {
    public static isSupported(element: HTMLElement): boolean {
        return (
            document.fullscreenEnabled &&
            typeof document.exitFullscreen === 'function' &&
            typeof element.requestFullscreen === 'function'
        );
    }

    public static toggle(element: HTMLElement): void {
        if (!Fullscreen.isSupported(element)) {
            return;
        }
        const promise =
            document.fullscreenElement === element ? document.exitFullscreen() : element.requestFullscreen();
        promise.catch((error: any) => {
            console.error(TAG, error);
        });
    }
}
