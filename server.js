const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const NCARDS = 96, NBONUS = 24, NCENTER = 4;
const PAGE_FILE = [path.join(__dirname, "index.html"), path.join(__dirname, "public", "index.html")].find(f => fs.existsSync(f));
if (!PAGE_FILE) { console.error("Не найден файл index.html рядом с server.js. Загрузите его в репозиторий."); process.exit(1); }
const PAGE = fs.readFileSync(PAGE_FILE);

const server = http.createServer((req, res) => {
  if (req.url === "/healthz") { res.writeHead(200); return res.end("ok"); }
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
  res.end(PAGE);
});
const wss = new WebSocketServer({ server, maxPayload: 2048 });
const sessions = new Map();

const token = () => crypto.randomBytes(12).toString("hex");
const shuf = (a) => { a = a.slice(); for (let i = a.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [a[i], a[j]] = [a[j], a[i]]; } return a; };
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const fail = (ws, msg, fatal) => send(ws, { t: "error", msg, fatal: !!fatal });

function broadcast(s) {
  s.touched = Date.now();
  const msg = JSON.stringify({ t: "state", state: {
    status: s.status,
    bonusOn: s.bonus,
    deckN: s.deck.length,
    top: s.deck.length ? s.deck[s.deck.length - 1] : null,
    center: s.center,
    players: [...s.players.values()].map(p => ({ id: p.id, nick: p.nick, cards: p.cards, placed: p.placed, bonus: p.bonus })),
  } });
  for (const w of s.conns) if (w.readyState === 1) w.send(msg);
}
function detach(ws) {
  const o = ws.ctx && sessions.get(ws.ctx.code);
  if (o) o.conns.delete(ws);
  ws.ctx = null;
}
function attach(ws, s, role, id, tok, nick) {
  detach(ws);
  ws.ctx = { code: s.code, role, id };
  s.conns.add(ws);
  send(ws, { t: "joined", code: s.code, role, id, tok, nick });
}

function handle(ws, m) {
  const c = ws.ctx, s = c && sessions.get(c.code);
  switch (m.t) {
    case "ping": return;
    case "create": {
      if (sessions.size >= 300) return fail(ws, "Сервер перегружен, попробуйте позже.");
      let code = null;
      for (let i = 0; i < 50 && !code; i++) { const x = String(1000 + crypto.randomInt(9000)); if (!sessions.has(x)) code = x; }
      if (!code) return fail(ws, "Нет свободных кодов, попробуйте позже.");
      const ns = { code, bonus: m.bonus !== false, hostTok: token(), status: "lobby", deck: [], center: [], players: new Map(), conns: new Set(), seq: 0, touched: Date.now() };
      sessions.set(code, ns);
      attach(ws, ns, "host", "host", ns.hostTok);
      return broadcast(ns);
    }
    case "join": {
      const ss = sessions.get(String(m.code));
      if (!ss) return fail(ws, "Игра с таким кодом не найдена.");
      const nick = String(m.nick || "").trim().slice(0, 16);
      if (!nick) return fail(ws, "Введите никнейм.");
      if (ss.status !== "lobby") return fail(ws, "Игра уже началась.");
      if (ss.players.size >= 10) return fail(ws, "В игре уже 10 игроков.");
      const id = "p" + (++ss.seq);
      const p = { id, tok: token(), nick, cards: [], placed: { 0: [], 1: [], 2: [], 3: [] }, bonus: null };
      ss.players.set(id, p);
      attach(ws, ss, "player", id, p.tok, nick);
      return broadcast(ss);
    }
    case "resume": {
      const ss = sessions.get(String(m.code)), t = String(m.tok || "");
      if (!ss) return fail(ws, "Игра закончилась или сервер был перезапущен.", true);
      if (t === ss.hostTok) { attach(ws, ss, "host", "host", t); return broadcast(ss); }
      for (const p of ss.players.values()) if (p.tok === t) { attach(ws, ss, "player", p.id, t, p.nick); return broadcast(ss); }
      return fail(ws, "Игрок не найден.", true);
    }
    case "start": {
      if (!s || c.role !== "host" || s.status !== "lobby") return;
      const n = s.players.size;
      if (n < 1 || n > 10) return;
      s.deck = shuf(Array.from({ length: NCARDS }, (_, i) => i));
      for (const p of s.players.values()) { p.cards = [s.deck.pop(), s.deck.pop(), s.deck.pop()]; p.bonus = s.bonus ? crypto.randomInt(NBONUS) : null; }
      s.center = Array.from({ length: NCENTER }, () => s.deck.pop());
      s.status = "playing";
      return broadcast(s);
    }
    case "give": {
      if (!s || c.role !== "host" || s.status !== "playing") return;
      const p = s.players.get(String(m.to));
      if (!p) return;
      let card;
      if (m.sel === "deck") { if (!s.deck.length) return; card = s.deck.pop(); }
      else if (Number.isInteger(m.sel) && m.sel >= 0) {
        if (m.sel >= s.center.length) return;
        card = s.center[m.sel];
        if (s.deck.length) s.center[m.sel] = s.deck.pop(); else s.center.splice(m.sel, 1);
      } else return;
      p.cards.push(card);
      return broadcast(s);
    }
    case "place": {
      if (!s || c.role !== "player" || s.status !== "playing") return;
      const p = s.players.get(c.id), id = m.id, z = m.zone;
      if (!p || !Number.isInteger(id) || !p.cards.includes(id) || ![0, 1, 2, 3].includes(z)) return;
      for (const k of [0, 1, 2, 3]) p.placed[k] = p.placed[k].filter(x => x !== id);
      p.placed[z].push(id);
      return broadcast(s);
    }
    case "leave": {
      if (!s) return detach(ws);
      if (c.role === "host") {
        for (const w of s.conns) if (w !== ws) { fail(w, "Ведущий закрыл игру.", true); w.ctx = null; }
        sessions.delete(s.code); ws.ctx = null; return;
      }
      if (s.status === "lobby") { s.players.delete(c.id); detach(ws); return broadcast(s); }
      return detach(ws);
    }
  }
}

wss.on("connection", (ws) => {
  ws.isAlive = true; ws.ctx = null;
  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("message", (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.t !== "string") return;
    try { handle(ws, m); } catch (e) { console.error(e); }
  });
  ws.on("close", () => detach(ws));
});
setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; ws.ping(); } }, 30000);
setInterval(() => { const now = Date.now(); for (const [k, s] of sessions) if (now - s.touched > 6 * 3600e3) sessions.delete(k); }, 600000);
server.listen(PORT, () => console.log("Игра запущена на порту " + PORT));
