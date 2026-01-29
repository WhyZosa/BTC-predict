import { useEffect, useMemo, useRef, useState } from "react";
import { createChart, CandlestickSeries, CrosshairMode, ColorType } from "lightweight-charts";
import type { IChartApi, ISeriesApi, CandlestickData, UTCTimestamp } from "lightweight-charts";

type Timeframe = "5m" | "1h" | "4h" | "1d";

type ApiCandle = {
  time?: unknown; t?: unknown; timestamp?: unknown; ts?: unknown; date?: unknown;
  open?: unknown; o?: unknown;
  high?: unknown; h?: unknown;
  low?: unknown; l?: unknown;
  close?: unknown; c?: unknown;
};

type ApiResponse =
  | ApiCandle[]
  | {
      candles?: ApiCandle[];
      data?: ApiCandle[];
      items?: ApiCandle[];
      source?: string;
      updated_at?: string;
      meta?: { source?: string; updated_at?: string };
    };

function toUtcSeconds(x: unknown): number | null {
  if (typeof x === "number" && Number.isFinite(x)) {
    // milliseconds -> seconds
    return x > 2e10 ? Math.floor(x / 1000) : Math.floor(x);
  }

  if (typeof x === "string") {
    const s0 = x.trim();
    if (!s0) return null;

    // "1700000000" or "1700000000000"
    const n = Number(s0);
    if (Number.isFinite(n)) return n > 2e10 ? Math.floor(n / 1000) : Math.floor(n);

    // fix "2026-01-13 21:00:00+00:00" -> "2026-01-13T21:00:00+00:00"
    const s = (s0.includes(" ") && !s0.includes("T")) ? s0.replace(" ", "T") : s0;

    const ms = Date.parse(s);
    if (Number.isFinite(ms)) return Math.floor(ms / 1000);
  }

  return null;
}

function fmtLocal(d: Date | null): string {
  if (!d) return "—";
  const dd = d.toLocaleDateString("ru-RU");
  const tt = d.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  return `${dd}, ${tt}`;
}

function badgeClasses(ok: boolean) {
  return ok
    ? "text-xs px-2 py-1 rounded-full bg-green-500/15 text-green-200 border border-green-500/25"
    : "text-xs px-2 py-1 rounded-full bg-red-500/15 text-red-200 border border-red-500/25";
}

export default function ChartCard({ timeframe }: { timeframe: Timeframe }) {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);

  const [statusOk, setStatusOk] = useState<boolean>(true);
  const [err, setErr] = useState<string>("");
  const [source, setSource] = useState<string>("—");
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);
  const [lastCandleAt, setLastCandleAt] = useState<Date | null>(null);

  const refreshSec = 60;

  const limit = useMemo(() => {
    if (timeframe === "5m") return 800;
    if (timeframe === "1h") return 800;
    if (timeframe === "4h") return 500;
    return 400; // 1d
  }, [timeframe]);

  // init chart once
  useEffect(() => {
    const el = hostRef.current;
    if (!el) return;

    const css = getComputedStyle(document.documentElement);
    const textColor = (css.getPropertyValue("--tg-text") || "#e5e7eb").trim();
    const hintColor = (css.getPropertyValue("--tg-hint") || "#94a3b8").trim();

    const chart = createChart(el, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor,
      },
      rightPriceScale: { borderVisible: false },
      timeScale: { borderVisible: false },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.06)" },
        horzLines: { color: "rgba(255,255,255,0.06)" },
      },
      crosshair: { mode: CrosshairMode.Normal },
      localization: {
        locale: "ru-RU",
      },
    });

    const series = chart.addSeries(CandlestickSeries, {
      upColor: "#22c55e",
      downColor: "#ef4444",
      borderUpColor: "#22c55e",
      borderDownColor: "#ef4444",
      wickUpColor: "#22c55e",
      wickDownColor: "#ef4444",
    });

    chartRef.current = chart;
    seriesRef.current = series;

    // ResizeObserver without infinite loop
    let raf = 0;
    const ro = new ResizeObserver(() => {
      if (!chartRef.current || !hostRef.current) return;
      cancelAnimationFrame(raf);
      raf = requestAnimationFrame(() => {
        const r = hostRef.current!.getBoundingClientRect();
        if (r.width > 0 && r.height > 0) chartRef.current!.resize(Math.floor(r.width), Math.floor(r.height));
      });
    });
    ro.observe(el);

    return () => {
      cancelAnimationFrame(raf);
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, []);

  // load data on timeframe & refresh
  useEffect(() => {
    let alive = true;
    const ac = new AbortController();

    async function load() {
      try {
        setErr("");
        setStatusOk(true);

        const url = `/api/ohlcv?tf=${encodeURIComponent(timeframe)}&limit=${encodeURIComponent(String(limit))}`;
        const res = await fetch(url, { signal: ac.signal });

        if (!res.ok) {
          const txt = await res.text().catch(() => "");
          throw new Error(`API ${url} вернул ${res.status}: ${txt}`);
        }

        const json = (await res.json()) as ApiResponse;

        const list: ApiCandle[] = Array.isArray(json)
          ? json
          : (json.candles ?? json.data ?? json.items ?? []);

        // meta
        const src =
          (Array.isArray(json) ? null : (json.source ?? json.meta?.source)) ??
          "local api";
        const upd =
          (Array.isArray(json) ? null : (json.updated_at ?? json.meta?.updated_at)) ??
          null;

        const now = new Date();
        setSource(String(src));
        setUpdatedAt(upd ? new Date(upd) : now);

        // normalize candles
        const mapped = list
          .map((c) => {
            const t = toUtcSeconds(c.time ?? c.t ?? c.timestamp ?? c.ts ?? c.date);
            if (!t) return null;

            const o = Number(c.open ?? c.o);
            const h = Number(c.high ?? c.h);
            const l = Number(c.low ?? c.l);
            const cl = Number(c.close ?? c.c);

            if (![o, h, l, cl].every(Number.isFinite)) return null;

            return { time: t as UTCTimestamp, open: o, high: h, low: l, close: cl };
          })
          .filter(Boolean) as CandlestickData<UTCTimestamp>[];

        // sort + dedup by time (IMPORTANT for lightweight-charts)
        mapped.sort((a, b) => a.time - b.time);

        const dedup: CandlestickData<UTCTimestamp>[] = [];
        for (const c of mapped) {
          const last = dedup[dedup.length - 1];
          if (!last) dedup.push(c);
          else if (last.time !== c.time) dedup.push(c);
          else dedup[dedup.length - 1] = c; // replace duplicate timestamp with latest
        }

        if (!dedup.length) {
          throw new Error("осле нормализации свечей не осталось (проверь time/данные)");
        }

        if (!alive) return;

        seriesRef.current?.setData(dedup);
        chartRef.current?.timeScale().fitContent();

        const last = dedup[dedup.length - 1];
        setLastCandleAt(new Date(Number(last.time) * 1000));
        setStatusOk(true);
      } catch (e) {
        if (!alive) return;
        const msg = e instanceof Error ? (e.message || String(e)) : String(e);
        setStatusOk(false);
        setErr(msg);
      }
    }

    load();
    const timer = setInterval(load, refreshSec * 1000);

    return () => {
      alive = false;
      ac.abort();
      clearInterval(timer);
    };
  }, [timeframe, limit]);

  return (
    <section className="rounded-2xl p-4" style={{ background: "var(--tg-card)" }}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Свечи • {timeframe}</div>
          <div className="mt-1 text-xs" style={{ color: "var(--tg-hint)" }}>
            источник: {source} • обновлено: {fmtLocal(updatedAt)}
          </div>
          <div className="mt-1 text-xs" style={{ color: "var(--tg-hint)" }}>
            последняя свеча: {fmtLocal(lastCandleAt)}
          </div>
        </div>

        <div className="flex items-center gap-2">
          <span className="text-xs px-2 py-1 rounded-full bg-white/5 border border-white/10" style={{ color: "var(--tg-hint)" }}>
            автообновление: {refreshSec} сек.
          </span>
          <span className={badgeClasses(statusOk)}>
            {statusOk ? "ok" : "ошибка"}
          </span>
        </div>
      </div>

      {!statusOk && err && (
        <div className="mt-3 rounded-xl p-3 text-sm bg-red-500/10 border border-red-500/20 text-red-100">
          {err}
        </div>
      )}

      <div className="mt-3 rounded-2xl overflow-hidden border border-white/10">
        <div ref={hostRef} style={{ height: 360 }} />
      </div>

      <div className="mt-2 text-xs" style={{ color: "var(--tg-hint)" }}>
        анные берём из локального API. сли видишь ECONNREFUSED — запусти uvicorn на 127.0.0.1:8000.
      </div>
    </section>
  );
}
