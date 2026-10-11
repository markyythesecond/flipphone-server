// FlipPhone relay server
// Phones connect over WebSocket, see who else is online (same group key),
// ring each other, and once a call is answered their audio frames are relayed 1:1.
//
//   PORT       port to listen on (hosts like Render set this for you)  default 8080
//   MAX_PHONES safety limit                                               default 500
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.env.PORT || '8080', 10);
const MAX_PHONES = parseInt(process.env.MAX_PHONES || '500', 10);
const MAX_AUDIO_BYTES = 4000;          // one audio frame is ~320 bytes; drop anything silly
const RING_TIMEOUT_MS = 35000;

/** id -> phone */
const phones = new Map();

function now() { return Date.now(); }
function clean(s, max) { return String(s || '').replace(/[^\w \-\.\[\]\(\)!?#@~*+]/g, '').trim().slice(0, max); }
function send(p, obj) { if (p && p.ws.readyState === 1) p.ws.send(JSON.stringify(obj)); }

// a short keitai-style phone number derived from the id, e.g. 090-1234-5678
function numberFor(id) {
  let h = 2166136261;
  for (const c of id) { h ^= c.charCodeAt(0); h = Math.imul(h, 16777619) >>> 0; }
  const d = String(h % 100000000).padStart(8, '0');
  return '090-' + d.slice(0, 4) + '-' + d.slice(4);
}

let rosterTimer = null;
function scheduleRoster() {
  if (rosterTimer) return;
  rosterTimer = setTimeout(() => { rosterTimer = null; broadcastRoster(); }, 150);
}
function broadcastRoster() {
  const byKey = new Map();
  for (const p of phones.values()) {
    if (!p.ready) continue;
    if (!byKey.has(p.key)) byKey.set(p.key, []);
    byKey.get(p.key).push(p);
  }
  for (const group of byKey.values()) {
    for (const me of group) {
      const list = group.filter(o => o !== me).map(o => ({ id: o.id, name: o.name, number: o.number, busy: !!(o.peer || o.ringing || o.incoming) }));
      send(me, { type: 'roster', list });
    }
  }
}

function endCall(p, notifyType) {
  const other = p.peer ? phones.get(p.peer) : null;
  if (other && other.peer === p.id) { other.peer = null; send(other, { type: notifyType || 'hangup', from: p.id }); }
  p.peer = null;
}

function clearRing(p) {
  if (!p.ringing) return;
  const target = phones.get(p.ringing.to);
  if (target && target.incoming === p.id) target.incoming = null;
  clearTimeout(p.ringing.timer);
  p.ringing = null;
}

function handle(p, msg) {
  switch (msg.type) {
    case 'hello': {
      p.name = clean(msg.name, 20) || 'MONKE';
      p.key = clean(msg.key, 40).toLowerCase() || 'public';
      p.ready = true;
      send(p, { type: 'welcome', id: p.id, number: p.number });
      scheduleRoster();
      break;
    }
    case 'name': p.name = clean(msg.name, 20) || p.name; scheduleRoster(); break;
    case 'call': {
      const t = phones.get(msg.to);
      if (!t || !t.ready || t.key !== p.key) { send(p, { type: 'offline', from: msg.to }); break; }
      if (p.peer || p.ringing) break;
      if (t.peer || t.incoming || t.ringing) { send(p, { type: 'busy', from: t.id }); break; }
      t.incoming = p.id;
      p.ringing = { to: t.id, timer: setTimeout(() => {   // nobody answered
        if (p.ringing && p.ringing.to === t.id) { clearRing(p); send(p, { type: 'noanswer', from: t.id }); send(t, { type: 'hangup', from: p.id }); scheduleRoster(); }
      }, RING_TIMEOUT_MS) };
      send(t, { type: 'call', from: p.id, name: p.name, number: p.number });
      scheduleRoster();
      break;
    }
    case 'answer': {
      const c = phones.get(msg.to);
      if (!c || p.incoming !== c.id || !c.ringing || c.ringing.to !== p.id) break;
      clearRing(c);
      p.incoming = null;
      p.peer = c.id; c.peer = p.id;
      send(c, { type: 'answer', from: p.id });
      scheduleRoster();
      break;
    }
    case 'decline': {
      const c = phones.get(msg.to);
      if (c && c.ringing && c.ringing.to === p.id) { clearRing(c); send(c, { type: 'decline', from: p.id }); }
      p.incoming = null;
      scheduleRoster();
      break;
    }
    case 'hangup': {
      if (p.ringing) { const t = phones.get(p.ringing.to); clearRing(p); if (t) send(t, { type: 'hangup', from: p.id }); }
      endCall(p, 'hangup');
      scheduleRoster();
      break;
    }
    case 'ping': send(p, { type: 'pong', t: msg.t }); break;
  }
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain' });
  res.end('FlipPhone server is running. Phones online: ' + phones.size + '\n');
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });

wss.on('connection', (ws, req) => {
  if (phones.size >= MAX_PHONES) { ws.close(1013, 'full'); return; }
  const url = new URL(req.url, 'http://x');
  const id = clean(url.searchParams.get('id'), 64) || Math.random().toString(36).slice(2);
  const old = phones.get(id);
  if (old) { try { old.ws.close(4000, 'replaced'); } catch (e) {} cleanup(old); }

  const p = { id, ws, name: 'MONKE', key: 'public', number: numberFor(id), ready: false, peer: null, ringing: null, incoming: null, alive: true };
  phones.set(id, p);

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      // audio: forward only to the person you're in a call with
      if (!p.peer || data.length > MAX_AUDIO_BYTES) return;
      const other = phones.get(p.peer);
      if (other && other.peer === p.id && other.ws.readyState === 1) other.ws.send(data, { binary: true });
      return;
    }
    let msg; try { msg = JSON.parse(data.toString()); } catch (e) { return; }
    if (msg && typeof msg.type === 'string') handle(p, msg);
  });
  ws.on('pong', () => { p.alive = true; });
  ws.on('close', () => cleanup(p));
  ws.on('error', () => {});
});

function cleanup(p) {
  if (phones.get(p.id) !== p) return;
  if (p.ringing) { const t = phones.get(p.ringing.to); clearRing(p); if (t) send(t, { type: 'hangup', from: p.id }); }
  if (p.incoming) { const c = phones.get(p.incoming); if (c && c.ringing && c.ringing.to === p.id) { clearRing(c); send(c, { type: 'offline', from: p.id }); } }
  endCall(p, 'offline');
  phones.delete(p.id);
  scheduleRoster();
}

// drop dead connections
setInterval(() => {
  for (const p of phones.values()) {
    if (!p.alive) { try { p.ws.terminate(); } catch (e) {} cleanup(p); continue; }
    p.alive = false;
    try { p.ws.ping(); } catch (e) {}
  }
}, 20000);

server.listen(PORT, () => console.log('FlipPhone server listening on port ' + PORT));
