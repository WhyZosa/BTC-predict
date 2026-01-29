import { useEffect, useState } from "react";
import Segmented from "./components/Segmented";
import ChartCard from "./components/ChartCard";
import { applyTgTheme, getTg } from "./lib/telegram";

export default function App() {
  const [tf, setTf] = useState<"5m" | "1h" | "4h" | "1d">("5m");

  useEffect(() => {
    const tg = getTg();
    if (tg) {
      tg.ready();
      tg.expand();
      applyTgTheme();

      const onTheme = () => applyTgTheme();
      tg.onEvent?.("themeChanged", onTheme);
      return () => tg.offEvent?.("themeChanged", onTheme);
    }
  }, []);

  return (
    <div className="min-h-screen px-4 pt-4 pb-6">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-xl font-semibold">BTC/USDT</div>
          <div className="text-sm mt-1" style={{ color: "var(--tg-hint)" }}>
            Mini App терминал • график (заглушка)
          </div>
        </div>

        <button
          className="px-3 py-2 rounded-xl text-sm font-semibold"
          style={{ background: "var(--tg-card)" }}
          onClick={() => {
            getTg()?.HapticFeedback?.impactOccurred("light");
            applyTgTheme();
          }}
        >
          🎛 Тема
        </button>
      </div>

      <div className="mt-3">
        <Segmented value={tf} onChange={setTf} items={["5m", "1h", "4h", "1d"] as const} />
      </div>

      <div className="mt-4">
        <ChartCard timeframe={tf} />
      </div>
    </div>
  );
}


