import ReactDOM from "react-dom/client";
import App from "./App.tsx";
import "./index.css";

function isResizeObserverNoise(msg: string) {
  return msg.includes("ResizeObserver loop");
}

function showFatal(err: unknown) {
  const msg = err instanceof Error ? (err.stack || err.message) : String(err);
  if (isResizeObserverNoise(msg)) return;

  document.body.innerHTML =
    `<pre style="white-space:pre-wrap;padding:16px;font-family:ui-monospace,Menlo,Consolas,monospace">` +
    `ОШИБКА ПРИЛОЖЕНИЯ:\n\n` + msg +
    `</pre>`;
}

window.addEventListener("error", (e) => {
  const anyE: any = e as any;
  const msg = (anyE?.error?.message || anyE?.message || "").toString();
  if (isResizeObserverNoise(msg)) return;
  showFatal(anyE.error || anyE.message);
});

window.addEventListener("unhandledrejection", (e) => {
  const reason: any = (e as PromiseRejectionEvent).reason;
  const msg = (reason?.message || String(reason));
  if (isResizeObserverNoise(msg)) return;
  showFatal(reason);
});

try {
  ReactDOM.createRoot(document.getElementById("root")!).render(<App />);
} catch (e) {
  showFatal(e);
}
