// The Floor — live price relay
// One process holds ONE Finnhub connection and serves the same prices to
// every student, so the leaderboard is fair. Your API key stays here on the
// server and is never exposed to the browser.

import express from "express";
import WebSocket from "ws";

const KEY = process.env.FINNHUB_KEY;
const PORT = process.env.PORT || 3001;

if (!KEY) {
  console.error("Missing FINNHUB_KEY. Set it in your environment / .env file.");
  process.exit(1);
}

// Must match the UNIVERSE list in the frontend app.
const SYMBOLS = [
  "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AMD", "NFLX",
  "DIS", "JPM", "V", "WMT", "KO", "PEP", "XOM", "BA", "NKE", "SBUX", "UBER",
  "PYPL", "INTC", "GME", "AMC", "SPY", "QQQ", "VOO", "ARKK", "DIA",
];

const prices = {}; // symbol -> latest price
const opens = {};  // symbol -> today's open (for the "Today %" column)

let newsCache = { items: [], ts: 0 };
let newsInflight = null;

const quoteCache = {};   // symbol -> { data, ts }
const compNewsCache = {}; // symbol -> { items, ts }

// --- seed current price + open once via REST, staggered to respect 60/min ---
async function seed() {
  for (const s of SYMBOLS) {
    try {
      const r = await fetch(`https://finnhub.io/api/v1/quote?symbol=${s}&token=${KEY}`);
      const q = await r.json();
      if (q && q.c) {
        prices[s] = q.c;
        opens[s] = q.o || q.pc || q.c; // open, else prev close, else current
      }
    } catch (e) {
      console.error("seed", s, e.message);
    }
    await new Promise((r) => setTimeout(r, 1100)); // ~54 calls/min, under the limit
  }
  console.log("Seeded", Object.keys(prices).length, "symbols");
}

// --- keep one WebSocket open for live trade ticks ---
function connectWS() {
  const ws = new WebSocket(`wss://ws.finnhub.io?token=${KEY}`);

  ws.on("open", () => {
    SYMBOLS.forEach((s) => ws.send(JSON.stringify({ type: "subscribe", symbol: s })));
    console.log("WebSocket open, subscribed to", SYMBOLS.length, "symbols");
  });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.type === "trade" && Array.isArray(msg.data)) {
      for (const t of msg.data) {
        prices[t.s] = t.p;
        if (opens[t.s] === undefined) opens[t.s] = t.p;
      }
    }
  });

  ws.on("close", () => {
    console.log("WebSocket closed, reconnecting in 3s");
    setTimeout(connectWS, 3000);
  });

  ws.on("error", (e) => {
    console.error("WebSocket error:", e.message);
    ws.close();
  });
}

// --- general market news, cached 60s so client count doesn't burn quota ---
async function getNews() {
  const now = Date.now();
  if (newsCache.items.length && now - newsCache.ts < 60000) return newsCache.items;
  if (newsInflight) return newsInflight;
  newsInflight = (async () => {
    try {
      const r = await fetch(`https://finnhub.io/api/v1/news?category=general&token=${KEY}`);
      const data = await r.json();
      if (Array.isArray(data)) {
        newsCache = {
          items: data.slice(0, 30).map((n) => ({
            id: n.id, headline: n.headline, summary: n.summary,
            source: n.source, url: n.url, image: n.image, datetime: n.datetime,
          })),
          ts: Date.now(),
        };
      }
    } catch (e) {
      console.error("news", e.message);
    } finally {
      newsInflight = null;
    }
    return newsCache.items;
  })();
  return newsInflight;
}

const ymd = (d) => d.toISOString().slice(0, 10);

// --- HTTP: serve a snapshot from memory (zero Finnhub calls per request) ---
const app = express();

// allow the browser app (on any host) to read this relay
app.use((req, res, next) => {
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET");
  next();
});

app.get("/api/prices", (req, res) => {
  res.json({ prices, opens, ts: Date.now() });
});

app.get("/api/news", async (req, res) => {
  const items = await getNews();
  res.json({ items, ts: Date.now() });
});

// single-ticker live quote (cached 8s per symbol to protect the rate limit)
app.get("/api/quote", async (req, res) => {
  const symbol = (req.query.symbol || "").toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: "no symbol" });
  const c = quoteCache[symbol];
  if (c && Date.now() - c.ts < 8000) return res.json(c.data);
  try {
    const r = await fetch(`https://finnhub.io/api/v1/quote?symbol=${symbol}&token=${KEY}`);
    const q = await r.json();
    if (!q || !q.c) return res.status(404).json({ error: "not found" });
    const data = { symbol, price: q.c, open: q.o || q.pc || q.c };
    quoteCache[symbol] = { data, ts: Date.now() };
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: "lookup failed" });
  }
});

// recent news for one company (cached 5 min per symbol)
app.get("/api/company-news", async (req, res) => {
  const symbol = (req.query.symbol || "").toUpperCase().trim();
  if (!symbol) return res.status(400).json({ error: "no symbol" });
  const c = compNewsCache[symbol];
  if (c && Date.now() - c.ts < 300000) return res.json({ items: c.items, ts: c.ts });
  try {
    const to = new Date();
    const from = new Date(Date.now() - 21 * 24 * 60 * 60 * 1000);
    const url = `https://finnhub.io/api/v1/company-news?symbol=${symbol}&from=${ymd(from)}&to=${ymd(to)}&token=${KEY}`;
    const r = await fetch(url);
    const data = await r.json();
    const items = Array.isArray(data)
      ? data.slice(0, 20).map((n) => ({
          id: n.id, headline: n.headline, summary: n.summary,
          source: n.source, url: n.url, image: n.image, datetime: n.datetime,
        }))
      : [];
    compNewsCache[symbol] = { items, ts: Date.now() };
    res.json({ items, ts: Date.now() });
  } catch (e) {
    res.status(500).json({ error: "lookup failed" });
  }
});

app.get("/", (req, res) => res.send("The Floor price relay is running."));

app.listen(PORT, () => console.log("Relay listening on port", PORT));

seed();
connectWS();
getNews();
