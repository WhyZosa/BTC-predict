type Props<T extends string> = {
  items: readonly T[];
  value: T;
  onChange: (v: T) => void;
};

export default function Segmented<T extends string>({ items, value, onChange }: Props<T>) {
  return (
    <div className="w-full grid grid-cols-4 gap-2 p-2 rounded-2xl" style={{ background: "var(--tg-card)" }}>
      {items.map((it) => {
        const active = it === value;
        return (
          <button
            key={it}
            className="py-2 rounded-xl text-sm font-semibold"
            style={{
              background: active ? "var(--tg-accent)" : "transparent",
              color: active ? "#0b1220" : "var(--tg-text)",
            }}
            onClick={() => onChange(it)}
          >
            {it}
          </button>
        );
      })}
    </div>
  );
}
