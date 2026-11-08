// refresh-map-data: pulls real map layers into public.map_layers
//  flood   : Bangkok road & tunnel flood sensors of the BMA Drainage and Sewerage Department,
//            via POPNIX Flood open API (flood.pop.in.th, free with attribution, refreshed every 5–10 min at source)
//  transit : every Bangkok rail line and station from OpenStreetMap (Overpass API)
// ?only=flood|transit   ?backfill=N  (flood: also load N past days of 10-minute history, max 15 days per call)
import { createClient } from "npm:@supabase/supabase-js@2";

const sb = createClient(Deno.env.get("SUPABASE_URL")!, Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!, { auth: { persistSession: false } });
const POP = "https://flood.pop.in.th";
const UA = { "User-Agent": "ROOMER-beta-map/1.0 (+https://roomer-th.github.io/ROOMER/)" };
const WET = 5; // cm; POPNIX: >10 flood, 5–10 slight, <=5 dry

async function getJSON(url: string, init: RequestInit = {}, ms = 30000) {
  const ac = new AbortController(); const t = setTimeout(() => ac.abort(), ms);
  try {
    const r = await fetch(url, { ...init, headers: { ...UA, ...(init.headers || {}) }, signal: ac.signal });
    if (!r.ok) throw new Error(url + " -> HTTP " + r.status);
    return await r.json();
  } finally { clearTimeout(t); }
}
const r5 = (n: number) => Math.round(n * 1e5) / 1e5;
const bkk = (s: string | null) => (s ? new Date(s.replace(" ", "T") + "+07:00").toISOString() : null); // POPNIX times are Thai local
const fmt = (d: Date) => { const t = new Date(d.getTime() + 7 * 3600e3); return t.toISOString().slice(0, 16).replace("T", " "); };

async function lastFetched(id: string) {
  const { data } = await sb.from("map_layers").select("fetched_at").eq("id", id).maybeSingle();
  return data ? Date.parse(data.fetched_at) : 0;
}

/* store every wet 10-minute frame (depth > 5 cm) so we can summarise the last 90 days */
let firstSeen: number | null = null; // earliest moment any sensor reported in the frames we read
async function storeFrames(fr: any) {
  if (!fr?.st || !fr.t0) return 0;
  const t0 = Date.parse(fr.t0.replace(" ", "T") + "+07:00"), step = (fr.step || 600) * 1000;
  const rows: any[] = [];
  for (const [code, vals] of Object.entries(fr.st as Record<string, (number | null)[]>))
    vals.forEach((v, i) => { if (v != null) { const t = t0 + i * step; if (firstSeen == null || t < firstSeen) firstSeen = t; } if (v != null && v > WET) rows.push({ sensor_id: code, observed_at: new Date(t0 + i * step).toISOString(), value_cm: v }); });
  for (let i = 0; i < rows.length; i += 1000)
    await sb.from("flood_readings").upsert(rows.slice(i, i + 1000), { onConflict: "sensor_id,observed_at", ignoreDuplicates: true });
  return rows.length;
}

async function refreshFlood(backfill = 0) {
  const cur = await getJSON(`${POP}/api_roads.php`);
  let stored = 0;
  // last 24 h of frames every run, so short floods between runs are not missed
  try { stored += await storeFrames(await getJSON(`${POP}/api_roads.php?frames=1`)); } catch (_) { /* keep going */ }
  if (backfill > 0) {
    const { data: done } = await sb.from("map_layers").select("data->backfilledTo").eq("id", "flood").maybeSingle();
    let end = done?.backfilledTo ? new Date(done.backfilledTo as string) : new Date(Date.now() - 864e5);
    const days = Math.min(backfill, 15);
    for (let d = 0; d < days; d += 5) {
      const batch = [];
      for (let k = d; k < Math.min(d + 5, days); k++) batch.push(new Date(end.getTime() - k * 864e5));
      const res = await Promise.allSettled(batch.map((t) => getJSON(`${POP}/api_roads.php?frames=1&at=${encodeURIComponent(fmt(t))}`, {}, 40000)));
      for (const r of res) { if (r.status !== "fulfilled") throw new Error("history request failed: " + String(r.reason)); stored += await storeFrames(r.value); }
    }
    end = new Date(end.getTime() - days * 864e5);
    (cur as any)._backfilledTo = end.toISOString();
  }
  await sb.from("flood_readings").delete().lt("observed_at", new Date(Date.now() - 400 * 864e5).toISOString());

  const since = new Date(Date.now() - 90 * 864e5).toISOString();
  const stats: Record<string, { max: number; at: string; days: Set<string>; n: number }> = {};
  for (let from = 0; ; from += 1000) {
    const { data: hist } = await sb.from("flood_readings").select("sensor_id,value_cm,observed_at").gte("observed_at", since).range(from, from + 999);
    if (!hist?.length) break;
    for (const h of hist) {
      const s = stats[h.sensor_id] || (stats[h.sensor_id] = { max: 0, at: h.observed_at, days: new Set(), n: 0 });
      s.n++; s.days.add(fmt(new Date(h.observed_at)).slice(0, 10));
      if (Number(h.value_cm) > s.max) { s.max = Number(h.value_cm); s.at = h.observed_at; }
    }
    if (hist.length < 1000) break;
  }
  const { data: oldest } = await sb.from("flood_readings").select("observed_at").order("observed_at").limit(1);

  const sensors = (cur.roads || []).filter((r: any) => r.lat && r.lng).map((r: any) => {
    const s = stats[r.code];
    return {
      code: r.code, kind: r.kind, step5: r.grp === 2, name: r.name, road: r.road, district: r.district,
      lat: r5(r.lat), lng: r5(r.lng), depth: r.depth, level: r.level, at: bkk(r.measured_at), since: bkk(r.since),
      max90: s ? s.max : 0, max90At: s ? s.at : null, days90: s ? s.days.size : 0,
    };
  });
  const prev = (await sb.from("map_layers").select("data").eq("id", "flood").maybeSingle()).data?.data as any;
  const data = {
    latest: bkk(cur.summary?.latest), stale: !!cur.summary?.stale, summary: cur.summary,
    sensors, historyDays: 90,
    historyFrom: oldest?.[0]?.observed_at ?? null,
    backfilledTo: (cur as any)._backfilledTo ?? prev?.backfilledTo ?? null,
    // history coverage starts where the source has data (POPNIX keeps sensor history from late Sep 2026)
    coverFrom: [prev?.coverFrom ? Date.parse(prev.coverFrom) : null, firstSeen].filter((x) => x != null).length
      ? new Date(Math.min(...[prev?.coverFrom ? Date.parse(prev.coverFrom) : Infinity, firstSeen ?? Infinity])).toISOString() : null,
    wetCm: WET,
  };
  await sb.from("map_layers").upsert({
    id: "flood", data, fetched_at: new Date().toISOString(), source_updated_at: data.latest,
    source: "ข้อมูล: สำนักการระบายน้ำ กรุงเทพมหานคร ผ่าน POPNIX Flood (flood.pop.in.th)",
  });
  return { sensors: sensors.length, stored, latest: data.latest, backfilledTo: data.backfilledTo };
}

/* ---------------- transit ---------------- */
const OVERPASS = ["https://overpass-api.de/api/interpreter", "https://overpass.private.coffee/api/interpreter", "https://overpass.kumi.systems/api/interpreter"];
const Q = `[out:json][timeout:90];
(
 relation["type"="route"]["route"~"^(subway|light_rail|monorail)$"](13.40,100.20,14.25,101.00);
 relation["type"="route"]["route"="train"]["name"~"แดง|Red Line|Airport|แอร์พอร์ต",i](13.40,100.20,14.25,101.00);
);
out body geom;`;
function dist(a: number[], b: number[]) { return Math.hypot((a[0] - b[0]) * 108000, (a[1] - b[1]) * 111000); }
function thin(c: number[][]) { const o = [c[0]]; for (let i = 1; i < c.length - 1; i++) if (dist(o[o.length - 1], c[i]) > 20) o.push(c[i]); o.push(c[c.length - 1]); return o.map((p) => [r5(p[0]), r5(p[1])]); }
async function overpass(q: string, ms: number) {
  let err = "";
  for (const u of OVERPASS) {
    try { return await getJSON(u, { method: "POST", body: "data=" + encodeURIComponent(q), headers: { "Content-Type": "application/x-www-form-urlencoded" } }, ms); }
    catch (e) { err += u + ": " + String(e) + "; "; }
  }
  throw new Error(err);
}
export function buildTransit(j: any, names: Record<string, any>) {
  const lines: Record<string, any> = {}; const stops: any[] = [];
  for (const rel of j.elements || []) {
    if (rel.type !== "relation") continue;
    const t = rel.tags || {}; const colour = (t.colour || t.color || "#888888").toUpperCase();
    const key = (t.ref || "") + "|" + colour;
    const L = lines[key] || (lines[key] = { ref: t.ref || null, colour, route: t.route, network: t.network || t.operator || null, name: t["name:th"] || t.name, name_en: t["name:en"] || null, ways: {} });
    for (const m of rel.members || []) {
      if (m.type === "way" && m.geometry && !/platform/.test(m.role || "")) L.ways[m.ref] = m.geometry.map((g: any) => [g.lon, g.lat]);
      else if (m.type === "node" && /^(stop|stop_entry_only|stop_exit_only|station)?$/.test(m.role || "") && m.lat) stops.push({ lng: m.lon, lat: m.lat, node: m.ref, colour });
    }
  }
  const st: any[] = [];
  for (const s of stops) {
    const tg = names[s.node] || {};
    const th = (tg["name:th"] || tg.name || "").replace(/^(สถานี)\s*/, "").trim();
    const en = (tg["name:en"] || "").replace(/\s*station$/i, "").trim();
    if (!th && !en) continue;
    const near = st.find((o) => (o.th === th || (en && o.en === en)) && dist([o.lng, o.lat], [s.lng, s.lat]) < 450);
    if (near) { if (!near.colours.includes(s.colour)) near.colours.push(s.colour); continue; }
    st.push({ th, en: en || null, lng: r5(s.lng), lat: r5(s.lat), colours: [s.colour] });
  }
  const out = Object.values(lines).map((L: any) => ({ ref: L.ref, colour: L.colour, route: L.route, network: L.network, name: L.name, name_en: L.name_en,
    coords: Object.values(L.ways as Record<string, number[][]>).filter((c) => c.length > 1).map(thin) })).filter((L) => L.coords.length);
  return { lines: out, stations: st, osmBase: j.osm3s?.timestamp_osm_base || null };
}
async function refreshTransit() {
  const j = await overpass(Q, 100000);
  const ids = [...new Set((j.elements || []).flatMap((r: any) => (r.members || []).filter((m: any) => m.type === "node").map((m: any) => m.ref)))];
  const names: Record<string, any> = {};
  for (let i = 0; i < ids.length; i += 500) {
    const r = await overpass(`[out:json][timeout:60];node(id:${ids.slice(i, i + 500).join(",")});out tags;`, 40000);
    for (const e of r.elements || []) names[e.id] = e.tags || {};
  }
  const data = buildTransit(j, names);
  await sb.from("map_layers").upsert({ id: "transit", data, fetched_at: new Date().toISOString(), source_updated_at: data.osmBase, source: "© OpenStreetMap contributors (ODbL)" });
  return { lines: data.lines.length, stations: data.stations.length, osmBase: data.osmBase };
}

Deno.serve(async (req) => {
  firstSeen = null;
  const url = new URL(req.url);
  const only = url.searchParams.get("only"), backfill = Number(url.searchParams.get("backfill") || 0);
  const res: Record<string, unknown> = {};
  if (!only || only === "flood") {
    if (!backfill && Date.now() - await lastFetched("flood") < 5 * 60e3) res.flood = "skipped (fresh)";
    else try { res.flood = await refreshFlood(backfill); } catch (e) { res.flood = { error: String(e) }; }
  }
  if (only === "transit") {
    if (Date.now() - await lastFetched("transit") < 6 * 3600e3) res.transit = "skipped (fresh)";
    else try { res.transit = await refreshTransit(); } catch (e) { res.transit = { error: String(e) }; }
  }
  return new Response(JSON.stringify(res), { headers: { "Content-Type": "application/json" } });
});
