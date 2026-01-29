import { useEffect, useMemo, useRef, useState } from "react";
import { createChart } from "lightweight-charts";
import type { CandlestickData, UTCTimestamp } from "lightweight-charts";

type Interval = "1m" | "5m" | "15m" | "1h" | "4h" | "1d";

export default function App() {
  const API = import.meta.env.VITE_API_BASE as string;
  const [interval, setIntervalState] = useState<Interval>("1h");
  const [price, setPrice] = useState<number | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const symbol = "BTCUSDT";
  const chartRef = useRef<HTMLDivElement | null>(null);

  const tg = useMemo(() => (window as any).Telegram?.WebApp, []);

  async function loadPrice() {
    try {
      const r = await fetch(`${API}/api/price?symbol=${symbol}`);
      const j = await r.json();
      setPrice(j.price);
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  async function loadCandles() {
    try {
      setErr(null);
      const r = await fetch(`${API}/api/ohlcv?symbol=${symbol}&interval=${interval}&limit=300`);
      const j = await r.json();

      const candles = (j.candles as any[]).map((c) => ({
        time: c.time as UTCTimestamp,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
      })) as CandlestickData[];

      if (!chartRef.current) return;

      // безопасно пересоздаём график (без chart.remove(), чтобы не словить runtime ошибку)
      chartRef.current.innerHTML = "";

      const chart = createChart(chartRef.current, {
        autoSize: true,
        layout: {
          background: { color: tg?.themeParams?.bg_color ?? "#0b1220" },
          textColor: tg?.themeParams?.text_color ?? "#e6edf7",
        },
        grid: { vertLines: { visible: false }, horzLines: { visible: false } },
        rightPriceScale: { borderVisible: false },
        timeScale: { borderVisible: false },
      });

      const series = chart.addCandlestickSeries();
      series.setData(candles);
      chart.timeScale().fitContent();
    } catch (e: any) {
      setErr(String(e?.message ?? e));
    }
  }

  useEffect(() => {
    tg?.ready?.();
    tg?.expand?.();
  }, [tg]);

  useEffect(() => {
    loadCandles();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [interval]);

  useEffect(() => {
    loadPrice();
    const t = setInterval(loadPrice, 2500);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div style={{ minHeight: "100vh", padding: 16, fontFamily: "Inter, system-ui, Arial" }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
        <div>
          <div style={{ opacity: 0.7, fontSize: 12 }}>BTC / USDT</div>
          <div style={{ fontSize: 28, fontWeight: 700 }}>
            {price === null ? "—" : price.toLocaleString(undefined, { maximumFractionDigits: 2 })}
          </div>
          {err && <div style={{ marginTop: 6, fontSize: 12, color: "#ff7b7b" }}>Ошибка: {err}</div>}
        </div>

        <select
          value={interval}
          onChange={(e) => setIntervalState(e.target.value as Interval)}
          style={{
            padding: "10px 12px",
            borderRadius: 12,
            border: "1px solid rgba(255,255,255,0.15)",
            background: "transparent",
            color: "inherit",
          }}
        >
          <option value="1m">1m</option>
          <option value="5m">5m</option>
          <option value="15m">15m</option>
          <option value="1h">1h</option>
          <option value="4h">4h</option>
          <option value="1d">1d</option>
        </select>
      </div>

      <div
        style={{
          height: 420,
          marginTop: 14,
          borderRadius: 16,
          overflow: "hidden",
          border: "1px solid rgba(255,255,255,0.12)",
          background: "#0b1220",
        }}
      >
        <div ref={chartRef} style={{ height: "100%", width: "100%" }} />
      </div>

      <div style={{ opacity: 0.7, fontSize: 12, marginTop: 10 }}>
        Авто-обновление цены: 2.5 сек • Источник: Binance market data
      </div>
    </div>
  );
}

