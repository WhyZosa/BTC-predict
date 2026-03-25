const tg = window.Telegram?.WebApp ?? null;
const root = document.documentElement;

if (tg) {
  tg.ready();
  tg.expand();
  tg.MainButton?.hide();
}

const state = {
  chart: null,
  chartPoints: [],
  candles: [],
  marketSnapshot: null,
  selectedPrediction: null,
  isPredicting: false,
  predictionCooldownUntil: 0,
  activePredictionHorizon: null,
  refreshMs: 15000,
  timer: null,
  userContext: buildUserContext(),
};

const refs = {
  currentPrice: document.getElementById("currentPrice"),
  change24h: document.getElementById("change24h"),
  sourceName: document.getElementById("sourceName"),
  refreshRate: document.getElementById("refreshRate"),
  refreshButton: document.getElementById("refreshButton"),
  chartFocus: document.getElementById("chartFocus"),
  chartFocusTime: document.getElementById("chartFocusTime"),
  chartFocusPrice: document.getElementById("chartFocusPrice"),
  predictionCard: document.getElementById("predictionCard"),
  predictionHorizon: document.getElementById("predictionHorizon"),
  predictionTarget: document.getElementById("predictionTarget"),
  predictionPrice: document.getElementById("predictionPrice"),
  predictionCurrent: document.getElementById("predictionCurrent"),
  predictionDelta: document.getElementById("predictionDelta"),
  predictionDeltaText: document.getElementById("predictionDeltaText"),
  predictionInterval: document.getElementById("predictionInterval"),
  predictionTime: document.getElementById("predictionTime"),
  alertKind: document.getElementById("alertKind"),
  alertValue: document.getElementById("alertValue"),
  alertPreview: document.getElementById("alertPreview"),
  createAlertButton: document.getElementById("createAlertButton"),
  alertsList: document.getElementById("alertsList"),
  historyList: document.getElementById("historyList"),
  exportHistoryButton: document.getElementById("exportHistoryButton"),
  qualityOverall: document.getElementById("qualityOverall"),
  qualityHorizons: document.getElementById("qualityHorizons"),
};

const horizonLabels = {
  "6h": "6 часов",
  "1d": "1 день",
  "1w": "1 неделя",
};

function buildUserContext() {
  const rawInitData = tg?.initData ?? "";
  const telegramUser = tg?.initDataUnsafe?.user;
  const telegramChat = tg?.initDataUnsafe?.chat;
  let parsedInitUser = null;
  let parsedInitChat = null;

  if (rawInitData) {
    try {
      const params = new URLSearchParams(rawInitData);
      const userJson = params.get("user");
      const chatJson = params.get("chat");
      parsedInitUser = userJson ? JSON.parse(userJson) : null;
      parsedInitChat = chatJson ? JSON.parse(chatJson) : null;
    } catch (error) {
      console.debug("Telegram initData parsing failed", error);
    }
  }

  const fallbackTelegramId =
    (telegramChat?.type === "private" || parsedInitChat?.type === "private") &&
    Number.isFinite(Number(telegramChat?.id ?? parsedInitChat?.id))
      ? Number(telegramChat?.id ?? parsedInitChat?.id)
      : null;
  const telegramId = telegramUser?.id ?? parsedInitUser?.id ?? fallbackTelegramId;

  if (telegramId) {
    const resolvedContext = {
      userKey: `tg:${telegramId}`,
      telegramUserId: telegramId,
      chatId: telegramId,
      username: telegramUser?.username ?? parsedInitUser?.username ?? "",
      firstName: telegramUser?.first_name ?? parsedInitUser?.first_name ?? telegramChat?.title ?? parsedInitChat?.title ?? "",
      isTelegramUser: true,
    };

    localStorage.setItem("btc_forecast_telegram_user_key", resolvedContext.userKey);
    localStorage.setItem("btc_forecast_telegram_user_id", String(resolvedContext.telegramUserId));
    localStorage.setItem("btc_forecast_telegram_chat_id", String(resolvedContext.chatId));
    localStorage.setItem("btc_forecast_telegram_username", resolvedContext.username);
    localStorage.setItem("btc_forecast_telegram_first_name", resolvedContext.firstName);
    return resolvedContext;
  }

  const storedTelegramUserKey = localStorage.getItem("btc_forecast_telegram_user_key");
  const storedTelegramUserId = Number(localStorage.getItem("btc_forecast_telegram_user_id"));
  const storedTelegramChatId = Number(localStorage.getItem("btc_forecast_telegram_chat_id"));
  if (tg && storedTelegramUserKey && Number.isFinite(storedTelegramUserId) && storedTelegramUserId > 0) {
    return {
      userKey: storedTelegramUserKey,
      telegramUserId: storedTelegramUserId,
      chatId: Number.isFinite(storedTelegramChatId) && storedTelegramChatId > 0 ? storedTelegramChatId : storedTelegramUserId,
      username: localStorage.getItem("btc_forecast_telegram_username") ?? "",
      firstName: localStorage.getItem("btc_forecast_telegram_first_name") ?? "",
      isTelegramUser: true,
    };
  }

  let localKey = localStorage.getItem("btc_forecast_user_key");
  if (!localKey) {
    const randomPart =
      window.crypto?.randomUUID?.() ??
      `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    localKey = `local:${randomPart}`;
    localStorage.setItem("btc_forecast_user_key", localKey);
  }

  return {
    userKey: localKey,
    telegramUserId: null,
    chatId: null,
    username: "",
    firstName: "",
    isTelegramUser: false,
  };
}

function setCssVar(name, value) {
  if (value) {
    root.style.setProperty(name, value);
  }
}

function hexToRgba(color, alpha) {
  if (!color) {
    return "";
  }
  if (color.startsWith("rgba") || color.startsWith("rgb")) {
    return color;
  }
  const normalized = color.replace("#", "");
  const chunks =
    normalized.length === 3
      ? normalized.split("").map((chunk) => chunk + chunk)
      : [normalized.slice(0, 2), normalized.slice(2, 4), normalized.slice(4, 6)];
  if (chunks.some((chunk) => chunk.length !== 2)) {
    return color;
  }
  const [r, g, b] = chunks.map((chunk) => Number.parseInt(chunk, 16));
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function applyTelegramTheme() {
  if (!tg) {
    return;
  }

  const params = tg.themeParams ?? {};
  const lightMode = tg.colorScheme === "light";

  setCssVar("--text", params.text_color || (lightMode ? "#0f172a" : "#f7fbff"));
  setCssVar("--muted", params.hint_color || (lightMode ? "#64748b" : "#9bb0c7"));
  setCssVar("--primary", params.button_color || params.link_color || "#7fffd4");
  setCssVar("--primary-strong", params.link_color || params.button_color || "#37d4a8");
  setCssVar("--primary-text", params.button_text_color || (lightMode ? "#ffffff" : "#04101d"));
  setCssVar("--danger", params.destructive_text_color || "#ff6b7a");
  setCssVar("--panel-border", hexToRgba(params.hint_color || "#9bb0c7", lightMode ? 0.22 : 0.14));
  setCssVar("--panel", hexToRgba(params.secondary_bg_color || params.bg_color || "#0b1426", lightMode ? 0.92 : 0.82));
  setCssVar("--bg-start", params.bg_color || (lightMode ? "#ecf3ff" : "#050914"));
  setCssVar("--bg-mid", params.secondary_bg_color || (lightMode ? "#dce8f8" : "#081523"));
  setCssVar("--bg-end", params.bg_color || (lightMode ? "#f8fbff" : "#0f1d33"));
  setCssVar("--hero-start", lightMode ? "rgba(255, 255, 255, 0.88)" : "rgba(8, 19, 34, 0.92)");
  setCssVar("--hero-end", lightMode ? "rgba(233, 243, 255, 0.94)" : "rgba(16, 32, 58, 0.78)");
  setCssVar("--accent-surface-start", lightMode ? hexToRgba(params.button_color || "#37d4a8", 0.2) : "rgba(55, 212, 168, 0.16)");
  setCssVar("--accent-surface-end", lightMode ? "rgba(255, 255, 255, 0.82)" : "rgba(10, 20, 38, 0.84)");
  setCssVar("--button-surface-start", lightMode ? "rgba(255, 255, 255, 0.92)" : "rgba(14, 29, 51, 0.95)");
  setCssVar("--button-surface-end", lightMode ? "rgba(232, 241, 255, 0.92)" : "rgba(25, 53, 95, 0.92)");
  setCssVar("--prediction-surface-start", lightMode ? "rgba(255, 255, 255, 0.92)" : "rgba(13, 27, 48, 0.96)");
  setCssVar("--prediction-surface-end", lightMode ? "rgba(239, 245, 255, 0.92)" : "rgba(9, 19, 35, 0.88)");
  setCssVar("--glow-a", hexToRgba(params.button_color || "#37d4a8", lightMode ? 0.22 : 0.38));
  setCssVar("--glow-b", hexToRgba(params.link_color || "#4888ff", lightMode ? 0.18 : 0.28));
  setCssVar("--glow-a-soft", hexToRgba(params.button_color || "#37d4a8", lightMode ? 0.12 : 0.18));
  setCssVar("--glow-b-soft", hexToRgba(params.link_color || "#4888ff", lightMode ? 0.1 : 0.18));
  setCssVar("--shadow", lightMode ? "0 22px 56px rgba(15, 23, 42, 0.12)" : "0 24px 80px rgba(0, 0, 0, 0.35)");

  try {
    if (params.bg_color) {
      tg.setBackgroundColor(params.bg_color);
    }
    if (params.secondary_bg_color || params.bg_color) {
      tg.setHeaderColor(params.secondary_bg_color || params.bg_color);
    }
  } catch (error) {
    console.debug("Telegram theme API is unavailable", error);
  }
}

applyTelegramTheme();
tg?.onEvent?.("themeChanged", applyTelegramTheme);

const chartTicksLimit = () => {
  if (window.innerWidth <= 420) {
    return 4;
  }
  if (window.innerWidth <= 768) {
    return 6;
  }
  return 8;
};

const formatUsd = (value) =>
  new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(value);

const formatPct = (value) => `${value >= 0 ? "+" : ""}${Number(value).toFixed(2)}%`;
const formatMetricPct = (value) => (value == null ? "--" : `${Number(value).toFixed(2)}%`);
const formatMetricUsd = (value) => (value == null ? "--" : formatUsd(value));

const formatDateTime = (iso) =>
  new Intl.DateTimeFormat("ru-RU", {
    hour: "2-digit",
    minute: "2-digit",
    day: "2-digit",
    month: "short",
    year: "numeric",
  }).format(new Date(iso));

const formatRelativeShort = (iso) =>
  new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(iso));

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function outcomePresentation(item) {
  if (item.outcome_status === "hit") {
    return { text: "Сбылся", className: "success" };
  }
  if (item.outcome_status === "miss") {
    return { text: "Не сбылся", className: "danger" };
  }
  return { text: "В ожидании", className: "warning" };
}

async function apiRequest(url, { method = "GET", payload = null } = {}) {
  const options = { method, headers: {} };
  if (payload) {
    options.headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(payload);
  }

  const response = await fetch(url, options);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.detail || `HTTP ${response.status}`);
  }
  return data;
}

function renderStats(snapshot) {
  refs.currentPrice.textContent = formatUsd(snapshot.latest.price);
  refs.sourceName.textContent = snapshot.latest.source;
  refs.change24h.textContent = snapshot.change_24h_pct == null ? "--" : formatPct(snapshot.change_24h_pct);
  refs.change24h.style.color = snapshot.change_24h_pct >= 0 ? "var(--primary)" : "var(--danger)";
}

function buildChartModel(snapshot, prediction = null) {
  const labels = snapshot.candles.map((candle) => formatRelativeShort(candle.timestamp));
  const fullLabels = snapshot.candles.map((candle) => formatDateTime(candle.timestamp));
  const mainData = snapshot.candles.map((candle) => candle.close);
  const chartPoints = snapshot.candles.map((candle) => ({
    kind: "history",
    timestamp: candle.timestamp,
    price: candle.close,
    label: formatDateTime(candle.timestamp),
  }));

  const datasets = [
    {
      label: "BTC/USD",
      data: [...mainData],
      borderColor: "#7fffd4",
      borderWidth: 3,
      pointRadius: 0,
      pointHoverRadius: 6,
      pointHitRadius: 24,
      tension: 0.28,
      fill: true,
      backgroundColor(context) {
        const chart = context.chart;
        const { ctx, chartArea } = chart;
        if (!chartArea) {
          return "rgba(127, 255, 212, 0.12)";
        }
        const gradient = ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
        gradient.addColorStop(0, "rgba(127, 255, 212, 0.28)");
        gradient.addColorStop(1, "rgba(127, 255, 212, 0.02)");
        return gradient;
      },
    },
    {
      label: "Текущая цена",
      data: Array(mainData.length).fill(null).map((value, index) => (index === mainData.length - 1 ? snapshot.latest.price : value)),
      borderWidth: 0,
      pointRadius: 5,
      pointHoverRadius: 7,
      pointHitRadius: 26,
      pointBackgroundColor: "#ffffff",
      pointBorderColor: "#7fffd4",
      pointBorderWidth: 3,
      showLine: false,
    },
  ];

  if (prediction) {
    labels.push(formatRelativeShort(prediction.target_at));
    fullLabels.push(formatDateTime(prediction.target_at));
    chartPoints.push({
      kind: "forecast",
      timestamp: prediction.target_at,
      price: prediction.predicted_price,
      label: formatDateTime(prediction.target_at),
    });

    datasets[0].data = [...mainData, null];
    datasets[1].data = [...datasets[1].data, null];
    datasets.push({
      label: "Прогноз",
      data: [...Array(mainData.length - 1).fill(null), snapshot.latest.price, prediction.predicted_price],
      borderColor: "#ffd166",
      borderDash: [8, 6],
      borderWidth: 2,
      tension: 0,
      pointRadius: 0,
      pointHitRadius: 0,
      fill: false,
    });
    datasets.push({
      label: "Прогнозная цена",
      data: [...Array(mainData.length).fill(null), prediction.predicted_price],
      borderWidth: 0,
      pointRadius: 6,
      pointHoverRadius: 8,
      pointHitRadius: 28,
      pointBackgroundColor: "#ffd166",
      pointBorderColor: "#102033",
      pointBorderWidth: 3,
      showLine: false,
    });
  }

  return { labels, fullLabels, datasets, chartPoints };
}

function renderChart(snapshot) {
  const ctx = document.getElementById("priceChart");
  const chartModel = buildChartModel(snapshot, state.selectedPrediction);
  state.chartPoints = chartModel.chartPoints;
  state.candles = snapshot.candles;

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    events: ["mousemove", "mouseout", "click", "touchstart", "touchmove"],
    interaction: {
      mode: "nearest",
      intersect: false,
    },
    plugins: {
      legend: { display: false },
      tooltip: {
        enabled: true,
        displayColors: false,
        backgroundColor: "rgba(7, 14, 25, 0.92)",
        borderColor: "rgba(127, 255, 212, 0.28)",
        borderWidth: 1,
        padding: 12,
        callbacks: {
          title(items) {
            const point = state.chartPoints[items[0]?.dataIndex ?? 0];
            return point?.label ?? "";
          },
          label(context) {
            const point = state.chartPoints[context.dataIndex];
            const prefix = point?.kind === "forecast" ? "Прогноз" : "Цена";
            return `${prefix}: ${formatUsd(context.parsed.y)}`;
          },
        },
      },
    },
    scales: {
      x: {
        ticks: { color: "#9bb0c7", maxTicksLimit: chartTicksLimit(), maxRotation: 0, minRotation: 0 },
        grid: { color: "rgba(255,255,255,0.05)" },
      },
      y: {
        ticks: { color: "#9bb0c7" },
        grid: { color: "rgba(255,255,255,0.05)" },
      },
    },
  };

  if (state.chart) {
    state.chart.data.labels = chartModel.labels;
    state.chart.data.datasets = chartModel.datasets;
    state.chart.options = options;
    state.chart.update();
    return;
  }

  state.chart = new Chart(ctx, {
    type: "line",
    data: {
      labels: chartModel.labels,
      datasets: chartModel.datasets,
    },
    options,
  });
}

function getEventPosition(event) {
  const rect = event.target?.getBoundingClientRect?.();
  if (!rect) {
    return { x: event.offsetX ?? 0, y: event.offsetY ?? 0 };
  }

  const touch = event.touches?.[0] ?? event.changedTouches?.[0];
  if (touch) {
    return {
      x: touch.clientX - rect.left,
      y: touch.clientY - rect.top,
    };
  }

  return { x: event.offsetX ?? 0, y: event.offsetY ?? 0 };
}

function selectChartPoint(event) {
  if (!state.chart || !state.chartPoints.length) {
    return;
  }

  const points = state.chart.getElementsAtEventForMode(event, "nearest", { intersect: false }, true);
  if (!points.length) {
    return;
  }

  const pointIndex = points[0].index;
  const point = state.chartPoints[pointIndex];
  if (!point) {
    return;
  }

  const activeElements = [{ datasetIndex: points[0].datasetIndex, index: pointIndex }];
  state.chart.setActiveElements(activeElements);
  state.chart.tooltip.setActiveElements(activeElements, getEventPosition(event));
  state.chart.update();

  refs.chartFocus.classList.remove("hidden");
  refs.chartFocusTime.textContent = point.kind === "forecast" ? `Прогноз на ${point.label}` : point.label;
  refs.chartFocusPrice.textContent = formatUsd(point.price);

  tg?.HapticFeedback?.selectionChanged();
}

function renderPrediction(prediction) {
  refs.predictionCard.classList.remove("hidden");
  refs.predictionHorizon.textContent = `Прогноз через ${horizonLabels[prediction.horizon]}`;
  refs.predictionTarget.textContent = `Ориентир на ${formatDateTime(prediction.target_at)}`;
  refs.predictionPrice.textContent = formatUsd(prediction.predicted_price);
  refs.predictionCurrent.textContent = formatUsd(prediction.current_price);
  refs.predictionDelta.textContent = formatPct(prediction.delta_pct);
  refs.predictionDeltaText.textContent = `${formatUsd(prediction.delta_abs)} (${formatPct(prediction.delta_pct)})`;
  refs.predictionDelta.classList.toggle("up", prediction.delta_pct >= 0);
  refs.predictionDelta.classList.toggle("down", prediction.delta_pct < 0);
  refs.predictionInterval.textContent = `${formatUsd(prediction.confidence_low)} - ${formatUsd(prediction.confidence_high)}`;
  refs.predictionTime.textContent = formatDateTime(prediction.target_at);
}

function renderHistory(items) {
  if (!items.length) {
    refs.historyList.innerHTML = `
      <div class="empty-state">
        История появится после первого прогноза. Здесь будет видно, какой прогноз был сделан и что произошло потом.
      </div>
    `;
    return;
  }

  refs.historyList.innerHTML = items
    .map((item) => {
      const outcome = outcomePresentation(item);
      const actualLine =
        item.actual_price == null
          ? `<div class="history-metric"><span>Результат</span><strong>Ждем ${escapeHtml(formatDateTime(item.target_at))}</strong></div>`
          : `
            <div class="history-metric"><span>Факт</span><strong>${escapeHtml(formatUsd(item.actual_price))}</strong></div>
            <div class="history-metric"><span>Ошибка</span><strong>${escapeHtml(formatUsd(item.abs_error ?? 0))}</strong></div>
            <div class="history-metric"><span>Направление</span><strong>${item.direction_hit ? "Угадано" : "Не совпало"}</strong></div>
            <div class="history-metric"><span>Допуск</span><strong>${escapeHtml(`±${Number(item.success_tolerance_pct ?? 0).toFixed(2)}%`)}</strong></div>
            <div class="history-metric"><span>Критерий</span><strong>${item.tolerance_hit ? "Сбылся по допуску" : "Не сбылся по допуску"}</strong></div>
          `;

      return `
        <article class="history-item">
          <div class="history-item-head">
            <div>
              <p class="history-item-title">${escapeHtml(horizonLabels[item.horizon])}</p>
              <p class="history-item-subtitle">Запрос: ${escapeHtml(formatDateTime(item.generated_at))}</p>
            </div>
            <span class="pill ${outcome.className}">${escapeHtml(outcome.text)}</span>
          </div>
          <div class="history-item-grid">
            <div class="history-metric"><span>Прогноз</span><strong>${escapeHtml(formatUsd(item.predicted_price))}</strong></div>
            <div class="history-metric"><span>Текущая цена</span><strong>${escapeHtml(formatUsd(item.current_price))}</strong></div>
            <div class="history-metric"><span>Цель</span><strong>${escapeHtml(formatDateTime(item.target_at))}</strong></div>
            ${actualLine}
          </div>
        </article>
      `;
    })
    .join("");
}

function renderQuality(quality) {
  const live = quality.live || {};
  const overall = live.overall || {};
  const overallCards = [
    { title: "Live MAE", value: formatMetricUsd(overall.mae), subtitle: "Средняя абсолютная ошибка" },
    { title: "Live SMAPE", value: formatMetricPct(overall.smape), subtitle: "Симметричная ошибка" },
    { title: "Direction", value: formatMetricPct(overall.directional_accuracy), subtitle: "Точность направления" },
    { title: "Interval Hit", value: formatMetricPct(overall.interval_hit_rate), subtitle: "Попадание в диапазон" },
  ];

  refs.qualityOverall.innerHTML = overallCards
    .map(
      (card) => `
        <article class="quality-card">
          <h3>${escapeHtml(card.title)}</h3>
          <p>${escapeHtml(card.subtitle)}</p>
          <strong class="quality-value">${escapeHtml(card.value)}</strong>
        </article>
      `
    )
    .join("");

  refs.qualityHorizons.innerHTML = ["6h", "1d", "1w"]
    .map((horizon) => {
      const backtest = quality.backtest?.[horizon] ?? {};
      const liveHorizon = quality.live?.by_horizon?.[horizon] ?? {};
      const modelName = backtest.available ? backtest.model_name : "нет данных";
      const backtestMetrics = backtest.metrics ?? {};

      return `
        <article class="quality-card">
          <h3>${escapeHtml(horizonLabels[horizon])}</h3>
          <p>Модель: ${escapeHtml(modelName)}</p>
          <div class="quality-meta">
            <div class="quality-meta-row"><span>Бэктест MAE</span><strong>${escapeHtml(formatMetricUsd(backtestMetrics.mae))}</strong></div>
            <div class="quality-meta-row"><span>Бэктест RMSE</span><strong>${escapeHtml(formatMetricUsd(backtestMetrics.rmse))}</strong></div>
            <div class="quality-meta-row"><span>Бэктест MAPE</span><strong>${escapeHtml(formatMetricPct(backtestMetrics.mape ?? backtestMetrics.smape))}</strong></div>
            <div class="quality-meta-row"><span>Бэктест direction</span><strong>${escapeHtml(formatMetricPct(backtestMetrics.directional_accuracy))}</strong></div>
            <div class="quality-meta-row"><span>Live count</span><strong>${escapeHtml(String(liveHorizon.count ?? 0))}</strong></div>
            <div class="quality-meta-row"><span>Live MAE</span><strong>${escapeHtml(formatMetricUsd(liveHorizon.mae))}</strong></div>
            <div class="quality-meta-row"><span>Live interval hit</span><strong>${escapeHtml(formatMetricPct(liveHorizon.interval_hit_rate))}</strong></div>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderAlerts(items) {
  if (!state.userContext.isTelegramUser) {
    refs.alertsList.innerHTML = `
      <div class="empty-state">
        Алерты доступны только внутри Telegram, потому что уведомления отправляются через бота.
      </div>
    `;
    return;
  }

  if (!items.length) {
    refs.alertsList.innerHTML = `
      <div class="empty-state">
        Активных алертов пока нет. Создай один выше и бот пришлет сообщение, когда условие выполнится.
      </div>
    `;
    return;
  }

  refs.alertsList.innerHTML = items
    .map((item) => {
      const title =
        item.kind === "above_price"
          ? `Сообщить, если BTC будет выше ${formatUsd(item.threshold_value)}`
          : item.kind === "below_price"
            ? `Сообщить, если BTC будет ниже ${formatUsd(item.threshold_value)}`
            : `Сообщить, если BTC упадет на ${item.threshold_value.toFixed(2)}%`;
      const subtitle =
        item.kind === "above_price"
          ? `Порог: ${formatUsd(item.target_price ?? item.threshold_value)}`
          : item.kind === "below_price"
            ? `Порог: ${formatUsd(item.target_price ?? item.threshold_value)}`
            : `Триггер сработает около ${formatUsd(item.target_price ?? 0)} от цены ${formatUsd(item.baseline_price ?? item.created_price)}`;
      const statusClass = item.is_active ? "" : "danger";
      const statusText = item.is_active ? "Активен" : "Сработал";

      return `
        <article class="alert-item">
          <div class="alert-item-head">
            <div>
              <p class="alert-item-title">${escapeHtml(title)}</p>
              <p class="alert-item-subtitle">${escapeHtml(subtitle)}</p>
            </div>
            <span class="pill ${statusClass}">${escapeHtml(statusText)}</span>
          </div>
          <div class="history-item-grid">
            <div class="history-metric"><span>Создан</span><strong>${escapeHtml(formatDateTime(item.created_at))}</strong></div>
            <div class="history-metric"><span>Цена при создании</span><strong>${escapeHtml(formatUsd(item.created_price))}</strong></div>
            <div class="history-metric"><span>Состояние</span><strong>${item.triggered_at ? escapeHtml(`Сработал ${formatDateTime(item.triggered_at)}`) : "Ждет сигнала"}</strong></div>
            <div class="history-metric"><span>Действие</span><strong><button class="link-button" data-alert-delete="${item.id}">Удалить</button></strong></div>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderAlertPreview() {
  if (!state.marketSnapshot) {
    refs.alertPreview.textContent = "Цена загрузится через пару секунд, после этого здесь появится подсказка по алерту.";
    return;
  }

  const rawValue = Number(refs.alertValue.value);
  const currentPrice = state.marketSnapshot.latest.price;
  const kind = refs.alertKind.value;

  if (!Number.isFinite(rawValue) || rawValue <= 0) {
    refs.alertPreview.textContent =
      kind === "above_price"
        ? `Текущая цена ${formatUsd(currentPrice)}. Введи порог, выше которого нужно прислать уведомление.`
        : `Текущая цена ${formatUsd(currentPrice)}. Введи порог, ниже которого бот пришлет сообщение.`;
    return;
  }

  if (kind === "above_price") {
    refs.alertPreview.textContent = `Бот напишет, когда BTC достигнет примерно ${formatUsd(rawValue)} или выше.`;
    return;
  }

  refs.alertPreview.textContent = `Бот напишет, если BTC опустится примерно до ${formatUsd(rawValue)} или ниже.`;
}

function setPredictionButtonsState({ pending = false, activeHorizon = null } = {}) {
  document.querySelectorAll(".horizon-button").forEach((button) => {
    const isActive = button.dataset.horizon === activeHorizon;
    button.classList.toggle("active", isActive);
    button.classList.toggle("loading", pending && isActive);
    button.disabled = pending;
    button.setAttribute("aria-busy", pending && isActive ? "true" : "false");
  });
}

async function loadMarket() {
  try {
    const snapshot = await apiRequest("/api/market?limit=168");
    state.marketSnapshot = snapshot;
    renderStats(snapshot);
    renderChart(snapshot);
    renderAlertPreview();
  } catch (error) {
    console.error("Market load failed", error);
  }
}

async function requestPrediction(horizon) {
  const now = Date.now();
  if (state.isPredicting || now < state.predictionCooldownUntil) {
    return;
  }

  state.isPredicting = true;
  state.activePredictionHorizon = horizon;
  setPredictionButtonsState({ pending: true, activeHorizon: horizon });

  try {
    const prediction = await apiRequest("/api/predict", {
      method: "POST",
      payload: {
        horizon,
        user_key: state.userContext.userKey,
        telegram_user_id: state.userContext.telegramUserId,
        chat_id: state.userContext.chatId,
        username: state.userContext.username,
        first_name: state.userContext.firstName,
      },
    });

    state.selectedPrediction = prediction;
    renderPrediction(prediction);
    if (state.marketSnapshot) {
      renderChart(state.marketSnapshot);
    }
    await loadHistory();
    await loadModelQuality();
    tg?.HapticFeedback?.notificationOccurred("success");
  } catch (error) {
    console.error("Prediction failed", error);
    tg?.HapticFeedback?.notificationOccurred("error");
  } finally {
    state.isPredicting = false;
    state.predictionCooldownUntil = Date.now() + 1200;
    setPredictionButtonsState({ pending: false, activeHorizon: horizon });
    state.activePredictionHorizon = null;
  }
}

async function loadHistory() {
  try {
    const data = await apiRequest(`/api/history?user_key=${encodeURIComponent(state.userContext.userKey)}`);
    renderHistory(data.items || []);
  } catch (error) {
    console.error("History load failed", error);
  }
}

async function loadModelQuality() {
  try {
    const data = await apiRequest(`/api/model-quality?user_key=${encodeURIComponent(state.userContext.userKey)}`);
    renderQuality(data);
  } catch (error) {
    console.error("Quality load failed", error);
  }
}

async function loadAlerts() {
  if (!state.userContext.isTelegramUser) {
    renderAlerts([]);
    return;
  }

  try {
    const data = await apiRequest(`/api/alerts?user_key=${encodeURIComponent(state.userContext.userKey)}`);
    renderAlerts(data.items || []);
  } catch (error) {
    console.error("Alerts load failed", error);
  }
}

async function createAlert() {
  if (!state.userContext.isTelegramUser) {
    refs.alertPreview.textContent = "Создавать алерты можно только внутри Telegram Mini App.";
    return;
  }

  const thresholdValue = Number(refs.alertValue.value);
  if (!Number.isFinite(thresholdValue) || thresholdValue <= 0) {
    refs.alertPreview.textContent = "Введи корректное число для алерта.";
    return;
  }

  try {
    await apiRequest("/api/alerts", {
      method: "POST",
      payload: {
        user_key: state.userContext.userKey,
        telegram_user_id: state.userContext.telegramUserId,
        chat_id: state.userContext.chatId,
        username: state.userContext.username,
        first_name: state.userContext.firstName,
        kind: refs.alertKind.value,
        threshold_value: thresholdValue,
      },
    });

    refs.alertValue.value = "";
    renderAlertPreview();
    await loadAlerts();
    tg?.HapticFeedback?.notificationOccurred("success");
  } catch (error) {
    refs.alertPreview.textContent = error.message;
    tg?.HapticFeedback?.notificationOccurred("error");
  }
}

async function deleteAlert(alertId) {
  try {
    await apiRequest(`/api/alerts/${alertId}?user_key=${encodeURIComponent(state.userContext.userKey)}`, { method: "DELETE" });
    await loadAlerts();
    await loadModelQuality();
  } catch (error) {
    console.error("Alert delete failed", error);
  }
}

function exportHistoryCsv() {
  const url = `/api/history/export?user_key=${encodeURIComponent(state.userContext.userKey)}`;
  window.open(url, "_blank", "noopener,noreferrer");
}

function startRefresh() {
  refs.refreshRate.textContent = `каждые ${Math.round(state.refreshMs / 1000)} сек`;
  if (state.timer) {
    clearInterval(state.timer);
  }
  state.timer = setInterval(loadMarket, state.refreshMs);
}

window.addEventListener("resize", () => {
  if (!state.chart) {
    return;
  }
  state.chart.options.scales.x.ticks.maxTicksLimit = chartTicksLimit();
  state.chart.resize();
});

document.querySelectorAll(".horizon-button").forEach((button) => {
  button.addEventListener("click", () => requestPrediction(button.dataset.horizon));
});

refs.refreshButton.addEventListener("click", loadMarket);
refs.alertKind.addEventListener("change", renderAlertPreview);
refs.alertValue.addEventListener("input", renderAlertPreview);
refs.createAlertButton.addEventListener("click", createAlert);
refs.exportHistoryButton.addEventListener("click", exportHistoryCsv);
refs.alertsList.addEventListener("click", (event) => {
  const target = event.target.closest("[data-alert-delete]");
  if (!target) {
    return;
  }
  deleteAlert(target.dataset.alertDelete);
});

document.getElementById("priceChart").addEventListener("click", selectChartPoint);
document.getElementById("priceChart").addEventListener("touchstart", selectChartPoint, { passive: true });

loadMarket();
loadHistory();
loadModelQuality();
loadAlerts();
startRefresh();
