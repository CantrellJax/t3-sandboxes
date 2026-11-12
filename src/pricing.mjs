// Optional provider quotes. Only the billing models listed here are accepted;
// anything else is rejected rather than reinterpreted.
export const CURRENCIES = ["USD"];
export const UNITS = { second: 1, minute: 60, hour: 3600 };
export const ROUNDING = ["none", "up"];
export const DEFAULT_VALIDITY_MS = 15 * 60000;
export const MAX_VALIDITY_MS = 24 * 3600000;
const ISO_UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$/;
const MAX_RATE = 1000;
const isNumber = (value) => typeof value === "number" && Number.isFinite(value);
export function readQuote(raw, choice, now) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error("Quote missing");
  if (!CURRENCIES.includes(raw.currency)) throw new Error("Unsupported quote currency");
  if (!Object.hasOwn(UNITS, raw.unit)) throw new Error("Unsupported quote billing unit");
  if (!ROUNDING.includes(raw.rounding)) throw new Error("Unsupported quote rounding");
  let validUntil = now + DEFAULT_VALIDITY_MS;
  if (raw.validUntil !== undefined) {
    const text = typeof raw.validUntil === "string" ? raw.validUntil : "";
    validUntil = ISO_UTC.test(text) ? Date.parse(text) : NaN;
    if (
      !Number.isFinite(validUntil) ||
      new Date(validUntil).toISOString().slice(0, 19) !== text.slice(0, 19)
    )
      throw new Error("Quote validUntil is not an ISO 8601 UTC timestamp");
  }
  if (!(validUntil > now)) throw new Error("Quote expired");
  if (validUntil > now + MAX_VALIDITY_MS)
    throw new Error("Quote validUntil is more than 24 hours ahead");
  if (!Array.isArray(raw.choices) || !raw.choices.length || raw.choices.length > 50)
    throw new Error("Quote has no resource choices");
  const choices = raw.choices.map((item) => {
    if (!item || typeof item !== "object" || typeof item.id !== "string")
      throw new Error("Quote choice is not usable");
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(item.id))
      throw new Error("Quote choice id is not usable");
    if (!isNumber(item.rate) || item.rate < 0 || item.rate > MAX_RATE)
      throw new Error("Quote rate is not usable");
    if (item.label !== undefined && typeof item.label !== "string")
      throw new Error("Quote choice label is not usable");
    return { id: item.id, label: (item.label ?? item.id).slice(0, 80), rate: item.rate };
  });
  if (new Set(choices.map((item) => item.id)).size !== choices.length)
    throw new Error("Quote choice ids repeat");
  const selected = choice ? choices.find((item) => item.id === choice) : choices[0];
  if (!selected) throw new Error(`Quote has no resource choice ${choice}`);
  return {
    source: "provider",
    currency: raw.currency,
    unit: raw.unit,
    rounding: raw.rounding,
    choice: selected.id,
    label: selected.label,
    rate: selected.rate,
    quotedAt: now,
    validUntil,
    choices,
  };
}
// The part of an accepted quote that is stored on a sandbox row and never changes.
export function snapshot(quote) {
  const { choices: _choices, ...price } = quote;
  return price;
}
export function chargeFor(price, seconds) {
  if (!(seconds > 0)) return 0;
  const units = seconds / UNITS[price.unit];
  return (price.rounding === "up" ? Math.ceil(units) : units) * price.rate;
}
