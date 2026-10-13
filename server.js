const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.env.PORT || '8080', 10);
const MAX_PHONES = parseInt(process.env.MAX_PHONES || '500', 10);
const MAX_AUDIO_BYTES = 4000;          // one audio frame is ~320 bytes; drop anything silly
const RING_TIMEOUT_MS = 35000;

// ---- admin ----
const ADMIN_CODE = String(process.env.ADMIN_CODE || '').replace(/\D/g, '');
const ADMIN_HASH = ADMIN_CODE.length >= 6 ? crypto.createHash('sha256').update(ADMIN_CODE).digest() : null;
const bannedIds = new Set(String(process.env.BANNED_IDS || '').split(',').map(s => s.trim()).filter(Boolean));
const bannedIps = new Set();
const loginFails = new Map();   // ip -> { n, until }
const MAX_FAILS = 5, LOCK_MS = 15 * 60 * 1000;
function codeMatches(code) {
  if (!ADMIN_HASH) return false;
  const h = crypto.createHash('sha256').update(String(code || '').replace(/\D/g, '')).digest();
  return crypto.timingSafeEqual(h, ADMIN_HASH);
}
function cleanText(s, max) { return String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, max); }
function phoneInfo(o) {
  return { id: o.id, name: o.name, number: o.number, key: o.key, busy: !!(o.peer || o.ringing || o.incoming), dnd: !!o.dnd, admin: !!o.admin };
}

/** id -> phone */
const phones = new Map();

// ---- ECHO TEST: a fake contact that answers and plays your voice back, so you can test calls alone ----
const ECHO_ID = 'echo-test';
const ECHO_ENTRY = { id: ECHO_ID, name: 'ECHO TEST', number: '117', busy: false };
const ECHO_DELAY_MS = 1000, ECHO_MAX_MS = 3 * 60 * 1000;

function muLaw(sample) {            // float -1..1 -> 8-bit mu-law
  let s = Math.max(-1, Math.min(1, sample)) * 32767 | 0;
  const sign = (s >> 8) & 0x80; if (sign) s = -s;
  if (s > 32635) s = 32635; s += 0x84;
  let exp = 7; for (let mask = 0x4000; (s & mask) === 0 && exp > 0; mask >>= 1) exp--;
  const mant = (s >> (exp + 3)) & 0x0F;
  return (~(sign | (exp << 4) | mant)) & 0xFF;
}
// "pi-pi-pip": three short beeps so you know when to start talking (20 ms frames of 320 samples @ 16 kHz)
const BEEP_FRAMES = (() => {
  const frames = [], rate = 16000, pattern = [[0.12, 1000], [0.08, 0], [0.12, 1000], [0.08, 0], [0.2, 1300], [0.1, 0]];
  const samples = []; for (const [len, f] of pattern) for (let i = 0; i < len * rate; i++) samples.push(f ? 0.35 * Math.sin(2 * Math.PI * f * i / rate) : 0);
  for (let i = 0; i + 320 <= samples.length; i += 320) frames.push(Buffer.from(samples.slice(i, i + 320).map(muLaw)));
  return frames;
})();

function startEcho(p) {
  p.peer = ECHO_ID; p.echoStart = now();
  send(p, { type: 'answer', from: ECHO_ID });
  BEEP_FRAMES.forEach((f, i) => setTimeout(() => { if (p.peer === ECHO_ID && p.ws.readyState === 1) p.ws.send(f, { binary: true }); }, 300 + i * 20));
}

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
      const list = [ECHO_ENTRY].concat(group.filter(o => o !== me).map(o => ({ id: o.id, name: o.name, number: o.number, busy: !!(o.peer || o.ringing || o.incoming), dnd: !!o.dnd })));
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
    case 'status': p.dnd = !!msg.dnd; scheduleRoster(); break;   // do-not-disturb (manner mode)
    case 'call': {
      if (msg.to === ECHO_ID) {
        if (p.peer || p.ringing) break;
        p.ringing = { to: ECHO_ID, timer: setTimeout(() => { if (p.ringing && p.ringing.to === ECHO_ID) { p.ringing = null; startEcho(p); } }, 2500) };
        break;
      }
      const t = phones.get(msg.to);
      if (!t || !t.ready || (t.key !== p.key && !p.admin)) { send(p, { type: 'offline', from: msg.to }); break; }
      if (p.peer || p.ringing) break;
      if (t.dnd && !p.admin) { send(p, { type: 'dnd', from: t.id }); break; }
      if (t.peer || t.incoming || t.ringing) { send(p, { type: 'busy', from: t.id }); break; }
      t.incoming = p.id;
      p.ringing = { to: t.id, timer: setTimeout(() => {   // nobody answered
        if (p.ringing && p.ringing.to === t.id) { clearRing(p); send(p, { type: 'noanswer', from: t.id }); send(t, { type: 'hangup', from: p.id }); scheduleRoster(); }
      }, RING_TIMEOUT_MS) };
      send(t, { type: 'call', from: p.id, name: p.name, number: p.number, admin: !!p.admin });
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
      if (p.peer === ECHO_ID) { p.peer = null; break; }
      if (p.ringing && p.ringing.to === ECHO_ID) { clearTimeout(p.ringing.timer); p.ringing = null; break; }
      if (p.ringing) { const t = phones.get(p.ringing.to); clearRing(p); if (t) send(t, { type: 'hangup', from: p.id }); }
      endCall(p, 'hangup');
      scheduleRoster();
      break;
    }
    case 'ping': send(p, { type: 'pong', t: msg.t }); break;

    // ---------- admin ----------
    case 'admin_login': {
      const f = loginFails.get(p.ip) || { n: 0, until: 0 };
      if (f.until > now()) { send(p, { type: 'admin_fail', wait: Math.ceil((f.until - now()) / 1000) }); break; }
      if (codeMatches(msg.code)) {
        loginFails.delete(p.ip);
        p.admin = true;
        console.log('admin login:', p.id, p.name, p.ip);
        send(p, { type: 'admin_ok' });
      } else {
        f.n++; if (f.n >= MAX_FAILS) { f.n = 0; f.until = now() + LOCK_MS; }
        loginFails.set(p.ip, f);
        send(p, { type: 'admin_fail' });
      }
      break;
    }
    case 'admin_logout': p.admin = false; break;
    default:
      if (String(msg.type).startsWith('admin_')) { if (p.admin) handleAdmin(p, msg); else send(p, { type: 'admin_fail' }); }
  }
}

function handleAdmin(p, msg) {
  switch (msg.type) {
    case 'admin_list': {
      const list = [];
      for (const o of phones.values()) if (o.ready && o !== p) list.push(phoneInfo(o));
      list.sort((a, b) => a.name.localeCompare(b.name));
      send(p, { type: 'admin_list', list: list.slice(0, 200), total: list.length, banned: bannedIds.size });
      break;
    }
    case 'admin_broadcast': {
      const text = cleanText(msg.text, 140); if (!text) break;
      let n = 0;
      for (const o of phones.values()) if (o.ready && o !== p) { send(o, { type: 'notice', text, from: 'ADMIN' }); n++; }
      console.log('admin broadcast by', p.name + ':', text);
      send(p, { type: 'admin_done', what: 'broadcast', n });
      break;
    }
    case 'admin_notice': {   // message to one phone
      const t = phones.get(msg.to), text = cleanText(msg.text, 140);
      if (t && text) send(t, { type: 'notice', text, from: 'ADMIN' });
      send(p, { type: 'admin_done', what: 'notice', n: t ? 1 : 0 });
      break;
    }
    case 'admin_kick':
    case 'admin_ban': {
      const t = phones.get(msg.to);
      if (!t || t === p) { send(p, { type: 'admin_done', what: msg.type.slice(6), n: 0 }); break; }
      const ban = msg.type === 'admin_ban';
      if (ban) { bannedIds.add(t.id); if (t.ip && t.ip !== p.ip) bannedIps.add(t.ip); }   // never IP-ban your own network
      console.log('admin', ban ? 'ban' : 'kick', t.id, t.name, 'by', p.name);
      send(t, { type: ban ? 'banned' : 'kicked' });
      setTimeout(() => { try { t.ws.close(ban ? 4003 : 4001, ban ? 'banned' : 'kicked'); } catch (e) {} }, 200);
      send(p, { type: 'admin_done', what: ban ? 'ban' : 'kick', n: 1 });
      break;
    }
    case 'admin_unban_all': {
      const n = bannedIds.size + bannedIps.size; bannedIds.clear(); bannedIps.clear();
      send(p, { type: 'admin_done', what: 'unban', n });
      break;
    }
    case 'admin_hangup_all': {   // end every call on the server
      let n = 0;
      for (const o of phones.values()) {
        if (o.ringing) { const to = o.ringing.to, t = phones.get(to); if (to === ECHO_ID) { clearTimeout(o.ringing.timer); o.ringing = null; } else clearRing(o); if (t) send(t, { type: 'hangup', from: o.id }); send(o, { type: 'noanswer', from: to }); n++; }
        if (o.peer === ECHO_ID) { o.peer = null; send(o, { type: 'hangup', from: ECHO_ID }); n++; }
        else if (o.peer) { const other = phones.get(o.peer); send(o, { type: 'hangup', from: o.peer }); if (other) { send(other, { type: 'hangup', from: o.id }); other.peer = null; } o.peer = null; n++; }
      }
      scheduleRoster();
      send(p, { type: 'admin_done', what: 'hangup', n });
      break;
    }
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
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  if (bannedIds.has(id) || bannedIps.has(ip)) { try { ws.send(JSON.stringify({ type: 'banned' })); } catch (e) {} ws.close(4003, 'banned'); return; }
  const old = phones.get(id);
  if (old) { try { old.ws.close(4000, 'replaced'); } catch (e) {} cleanup(old); }

  const p = { id, ip, admin: false, ws, name: 'MONKE', key: 'public', number: numberFor(id), ready: false, peer: null, ringing: null, incoming: null, alive: true };
  phones.set(id, p);

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      // audio: forward only to the person you're in a call with
      if (!p.peer || data.length > MAX_AUDIO_BYTES) return;
      if (p.peer === ECHO_ID) {
        if (now() - p.echoStart > ECHO_MAX_MS) { p.peer = null; send(p, { type: 'hangup', from: ECHO_ID }); return; }
        const copy = Buffer.from(data);
        setTimeout(() => { if (p.peer === ECHO_ID && p.ws.readyState === 1) p.ws.send(copy, { binary: true }); }, ECHO_DELAY_MS);
        return;
      }
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
