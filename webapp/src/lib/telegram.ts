export type TgWebApp = {
  ready: () => void;
  expand: () => void;
  initData: string;
  initDataUnsafe: any;
  themeParams: Record<string, string>;
  colorScheme?: "light" | "dark";
  setHeaderColor?: (color: string) => void;
  setBackgroundColor?: (color: string) => void;
  onEvent?: (event: string, cb: () => void) => void;
  offEvent?: (event: string, cb: () => void) => void;
  HapticFeedback?: { impactOccurred: (style: "light" | "medium" | "heavy") => void };
};

export function getTg(): TgWebApp | null {
  // @ts-ignore
  return window?.Telegram?.WebApp ?? null;
}

export function applyTgTheme() {
  const tg = getTg();
  if (!tg) return;

  const p = tg.themeParams || {};
  const bg = p.bg_color || "#0b1220";
  const text = p.text_color || "#e5e7eb";
  const hint = p.hint_color || "#9ca3af";
  const link = p.link_color || "#60a5fa";
  const accent = p.button_color || "#22c55e";
  const card = "rgba(255,255,255,0.06)";

  const r = document.documentElement;
  r.style.setProperty("--tg-bg", bg);
  r.style.setProperty("--tg-text", text);
  r.style.setProperty("--tg-hint", hint);
  r.style.setProperty("--tg-link", link);
  r.style.setProperty("--tg-accent", accent);
  r.style.setProperty("--tg-card", card);

  tg.setBackgroundColor?.(bg);
  tg.setHeaderColor?.(bg);
}
