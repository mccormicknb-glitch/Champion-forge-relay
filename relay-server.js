// Champion Forge multiplayer relay server — Stage 3 (multi-seat).
// Dumb relay: no game logic, no state beyond the room's seat map and
// its decision log. All clients run the deterministic engine with the
// shared seed; identical decision streams in identical order means
// identical games.
//
// Implements the RELAY PROTOCOL SPEC in the client's multiplayer
// bridge:
//   Part A — the classic 2-player protocol. Behavior for rooms created
//     without a table size is IDENTICAL to the previous relay (create /
//     join / start / decision / rejoin / resync / grace), with the one
//     spec-mandated completion that surrender and draw are forwarded.
//     A client that never sends a Part B message observes Part A only.
//   Part B — multi-seat rooms (3–5): lobby seat map, host seat edits,
//     shrink-to-filled starts, per-seat decision routing with
//     controller attribution, FCFS claim broadcasts, and the MANUAL
//     dropout model (Norman, 2026-10-20): a dropped seat STALLS — no
//     bot ever takes it on a timer. The table decides via
//     {t:'dropoutReplace'} / {t:'dropoutSurrender'} (first press wins),
//     a reconnect before any press is a seamless return, after a
//     REPLACE the player reclaims their seat from the bot the moment
//     they reconnect (seatHuman), and after a SURRENDER they return
//     as a spectator only. A dropped host still ends the room after
//     the grace period (hostLeft) — no host migration.
//
// Run: node relay-server-v2.js [port]
// Env: RELAY_GRACE_MS overrides the 10-minute room-hold grace (tests).
import http from 'http';
import { WebSocketServer } from 'ws';
import { randomBytes } from 'crypto';
import { promises as fsp } from 'fs';
import path from 'path';

const PORT = parseInt(process.env.PORT || process.argv[2] || '8080', 10);
const GRACE_MS = parseInt(process.env.RELAY_GRACE_MS || String(10 * 60 * 1000), 10);
const rooms = new Map(); // code -> room

const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
function makeCode() {
  let code;
  do {
    code = Array.from(randomBytes(4)).map(b => CODE_ALPHABET[b % CODE_ALPHABET.length]).join('');
  } while (rooms.has(code));
  return code;
}

function send(ws, msg) {
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify(msg)); } catch { /* socket dying */ }
  }
}

/* ---------------- battle-log ingest (additive, 2026-10-21) ----------------
   POST /log            — a finished game client sends its complete
                          battle-history export (JSON {"text": "..."} or
                          a raw text body). Stored as its own .txt file,
                          byte-identical to the player's manual download.
   GET  /logs           — token-guarded list of stored logs.
   GET  /logs/<name>    — token-guarded fetch of one stored log.
   GET  /log-count      — unguarded count of stored logs (health check).
   The fetch endpoints require the LOG_FETCH_TOKEN env var; without it
   they answer 503. Storage is LOG_DIR (default ./game-logs). If
   LOG_GH_TOKEN and LOG_GH_REPO are also set, every log is additionally
   committed to that GitHub repo under logs/ (durable sink). Nothing
   here touches the WebSocket room/protocol logic below. */

const LOG_DIR = process.env.LOG_DIR || './game-logs';
const LOG_FETCH_TOKEN = process.env.LOG_FETCH_TOKEN || '';
const LOG_GH_TOKEN = process.env.LOG_GH_TOKEN || '';
const LOG_GH_REPO = process.env.LOG_GH_REPO || '';
const LOG_MAX_BYTES = 2 * 1024 * 1024;

async function storeBattleLog(text) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const name = `champion-forge-battle-${stamp}-${randomBytes(3).toString('hex')}.txt`;
  await fsp.mkdir(LOG_DIR, { recursive: true });
  await fsp.writeFile(path.join(LOG_DIR, name), text, 'utf8');
  if (LOG_GH_TOKEN && LOG_GH_REPO) {
    try {
      const r = await fetch(`https://api.github.com/repos/${LOG_GH_REPO}/contents/logs/${name}`, {
        method: 'PUT',
        headers: {
          'Authorization': `Bearer ${LOG_GH_TOKEN}`,
          'Accept': 'application/vnd.github+json',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          message: `Battle log ${name}`,
          content: Buffer.from(text, 'utf8').toString('base64'),
        }),
      });
      if (!r.ok) console.log(`battle log ${name}: GitHub sink answered ${r.status} (filesystem copy kept)`);
    } catch (e) {
      console.log(`battle log ${name}: GitHub sink error (${e.message}); filesystem copy kept`);
    }
  }
  return name;
}

async function listBattleLogs() {
  try {
    const files = await fsp.readdir(LOG_DIR);
    return files.filter((f) => f.startsWith('champion-forge-battle-') && f.endsWith('.txt')).sort();
  } catch { return []; }
}

const server = http.createServer((req, res) => {
  let url;
  try { url = new URL(req.url, 'http://localhost'); } catch { url = { pathname: '/' }; }

  if (req.method === 'OPTIONS' && url.pathname === '/log') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    });
    return res.end();
  }

  if (req.method === 'POST' && url.pathname === '/log') {
    const chunks = [];
    let size = 0;
    let tooBig = false;
    req.on('data', (c) => {
      size += c.length;
      if (size > LOG_MAX_BYTES) { tooBig = true; req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', async () => {
      if (tooBig) return;
      const reply = (code, obj) => {
        res.writeHead(code, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify(obj));
      };
      try {
        const raw = Buffer.concat(chunks).toString('utf8');
        let text = raw;
        try {
          const j = JSON.parse(raw);
          if (j && typeof j.text === 'string') text = j.text;
        } catch { /* raw text body: store as-is */ }
        if (!text || text.trim().length < 20) return reply(400, { ok: false, error: 'empty log' });
        const name = await storeBattleLog(text);
        console.log(`battle log stored: ${name} (${text.length} chars)`);
        return reply(202, { ok: true, id: name });
      } catch (e) {
        console.log('battle log store failed:', e.message);
        return reply(500, { ok: false, error: 'store failed' });
      }
    });
    return;
  }

  if (req.method === 'GET' && url.pathname === '/log-count') {
    listBattleLogs().then((files) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ count: files.length }));
    });
    return;
  }

  if (req.method === 'GET' && (url.pathname === '/logs' || url.pathname.startsWith('/logs/'))) {
    if (!LOG_FETCH_TOKEN) {
      res.writeHead(503, { 'Content-Type': 'text/plain' });
      return res.end('log fetch not configured (LOG_FETCH_TOKEN unset)');
    }
    if (req.headers['x-log-token'] !== LOG_FETCH_TOKEN) {
      res.writeHead(401, { 'Content-Type': 'text/plain' });
      return res.end('unauthorized');
    }
    if (url.pathname === '/logs') {
      listBattleLogs().then((files) => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ logs: files }));
      });
      return;
    }
    const name = path.basename(url.pathname.slice('/logs/'.length));
    if (!name.startsWith('champion-forge-battle-') || !name.endsWith('.txt')) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      return res.end('no such log');
    }
    fsp.readFile(path.join(LOG_DIR, name), 'utf8').then((text) => {
      res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end(text);
    }).catch(() => {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('no such log');
    });
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Champion Forge relay OK');
});
const wss = new WebSocketServer({ server });
server.listen(PORT);
console.log(`Relay server on :${PORT}`);

// Heartbeat: browsers answer pings automatically at the protocol level.
// This keeps idle sockets warm through proxies/carriers and detects dead
// connections quickly so the away/grace flow starts promptly.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch { /* socket already gone */ }
  }
}, 25000);
wss.on('close', () => clearInterval(heartbeat));

/* ---------------- room helpers ---------------- */

function seatList(room) {
  return room.seats.map((s, i) => ({
    seat: i,
    kind: s.kind,
    name: s.name || '',
    difficulty: s.kind === 'bot' ? (s.difficulty || 'medium') : null,
  }));
}

function broadcastLobby(room) {
  const msg = { t: 'lobby', size: room.size, seats: seatList(room) };
  for (const ws of room.players) send(ws, msg);
}

function broadcastAll(room, msg) {
  for (const ws of room.players) send(ws, msg);
}

function broadcastOthers(room, exceptWs, msg) {
  for (const ws of room.players) if (ws && ws !== exceptWs) send(ws, msg);
}

// Seats the table surrendered (spectator-only on rejoin), seat indexes.
function spectatorSeats(room) {
  const out = [];
  room.resolved.forEach((r, seat) => { if (r.type === 'surrender') out.push(seat); });
  return out;
}

function destroyRoom(code, room, notifyWs) {
  if (room.graceTimer) { clearTimeout(room.graceTimer); room.graceTimer = null; }
  if (notifyWs) send(notifyWs, { t: 'opponentLeft' });
  rooms.delete(code);
  console.log(`room ${code} closed`);
}

function newRoom(code, size, multi) {
  return {
    code, size, multi,
    seats: Array.from({ length: size }, () => ({ kind: 'open', name: '', difficulty: null })),
    players: new Array(size).fill(null),     // live connection per seat (humans)
    controllers: new Array(size).fill(null), // connection allowed to decide for the seat
    decisions: [],
    seed: null,
    started: false,
    resolved: new Map(), // seat -> {type:'replace', difficulty} | {type:'surrender'}
    graceTimer: null,
  };
}

/* ---------------- connection handling ---------------- */

wss.on('connection', (ws) => {
  console.log('client connected');
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  let roomCode = null;
  let seat = -1;

  const myRoom = () => (roomCode ? rooms.get(roomCode) : undefined);

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return send(ws, { t: 'error', msg: 'bad json' }); }

    if (m.t === 'create') {
      const size = typeof m.size === 'number' ? Math.max(2, Math.min(5, Math.floor(m.size))) : 2;
      const multi = size > 2;
      roomCode = makeCode();
      seat = 0;
      const room = newRoom(roomCode, size, multi);
      room.seats[0] = { kind: 'human', name: typeof m.name === 'string' ? m.name.slice(0, 24) : '', difficulty: null };
      room.players[0] = ws;
      room.controllers[0] = ws;
      rooms.set(roomCode, room);
      ws.cfRoom = roomCode; ws.cfSeat = 0;
      if (multi) {
        send(ws, { t: 'created', room: roomCode, playerIdx: 0, size, seats: seatList(room) });
        broadcastLobby(room);
      } else {
        // Part A: the classic reply carries no extra fields.
        send(ws, { t: 'created', room: roomCode, playerIdx: 0 });
      }
      console.log(`room ${roomCode} created (size ${size})`);
      return;
    }

    if (m.t === 'join') {
      const room = rooms.get(m.room);
      if (!room) return send(ws, { t: 'error', msg: 'no such room' });
      if (!room.multi) {
        // Part A join, exactly as before.
        if (room.seed !== null) return send(ws, { t: 'error', msg: 'room full' }); // already started; rejoin is the way back
        if (room.players[1]) return send(ws, { t: 'error', msg: 'room full' });
        roomCode = m.room;
        seat = 1;
        room.seats[1] = { kind: 'human', name: typeof m.name === 'string' ? m.name.slice(0, 24) : '', difficulty: null };
        room.players[1] = ws;
        room.controllers[1] = ws;
        ws.cfRoom = roomCode; ws.cfSeat = 1;
        send(ws, { t: 'joined', room: roomCode, playerIdx: 1 });
        room.seed = Math.floor(Math.random() * 1e9);
        room.started = true;
        for (const pws of room.players) send(pws, { t: 'start', seed: room.seed });
        console.log(`room ${roomCode} started (seed ${room.seed})`);
        return;
      }
      // Multi-seat join: lowest OPEN seat; started/full rooms refuse.
      if (room.started) return send(ws, { t: 'error', msg: 'room full' });
      const idx = room.seats.findIndex((s) => s.kind === 'open');
      if (idx < 0) return send(ws, { t: 'error', msg: 'room full' });
      roomCode = m.room;
      seat = idx;
      room.seats[idx] = { kind: 'human', name: typeof m.name === 'string' ? m.name.slice(0, 24) : '', difficulty: null };
      room.players[idx] = ws;
      room.controllers[idx] = ws;
      ws.cfRoom = roomCode; ws.cfSeat = idx;
      send(ws, { t: 'joined', room: roomCode, playerIdx: idx });
      broadcastLobby(room);
      console.log(`room ${roomCode}: seat ${idx} joined`);
      return;
    }

    if (m.t === 'seatBot' || m.t === 'seatOpen') {
      // Lobby seat edits: host only, pre-start, multi-seat rooms.
      const room = myRoom();
      if (!room || !room.multi || room.started) return;
      if (seat !== 0) return send(ws, { t: 'error', msg: 'not host' });
      const s = m.seat;
      if (typeof s !== 'number' || s < 1 || s >= room.size) return;
      if (m.t === 'seatBot' && room.seats[s].kind === 'open') {
        const d = ['easy', 'medium', 'hard'].includes(m.difficulty) ? m.difficulty : 'medium';
        room.seats[s] = { kind: 'bot', name: '', difficulty: d };
        broadcastLobby(room);
      } else if (m.t === 'seatOpen' && room.seats[s].kind === 'bot') {
        room.seats[s] = { kind: 'open', name: '', difficulty: null };
        broadcastLobby(room);
      }
      return;
    }

    if (m.t === 'start') {
      // Host starts a multi-seat room (Part A rooms auto-start on join).
      const room = myRoom();
      if (!room || !room.multi) return;
      if (seat !== 0) return send(ws, { t: 'error', msg: 'not host' });
      if (room.started) return send(ws, { t: 'error', msg: 'already started' });
      const filledIdx = [];
      room.seats.forEach((s, i) => { if (s.kind !== 'open') filledIdx.push(i); });
      let finalIdx = filledIdx;
      if (typeof m.size === 'number') {
        const k = Math.floor(m.size);
        if (k < 2 || k > filledIdx.length) return send(ws, { t: 'error', msg: 'cannot start with that table size' });
        finalIdx = filledIdx.slice(0, k);
      } else if (filledIdx.length !== room.size) {
        return send(ws, { t: 'error', msg: 'empty seats at the table' });
      }
      // Compact the filled seats IN ORDER into seats 0..k-1 (a no-op
      // when nothing was open), reassigning seat indexes.
      const oldPlayers = room.players, oldControllers = room.controllers, oldSeats = room.seats;
      room.size = finalIdx.length;
      room.seats = finalIdx.map((i) => oldSeats[i]);
      room.players = finalIdx.map((i) => oldPlayers[i]);
      room.controllers = finalIdx.map((i) => oldControllers[i]);
      room.players.forEach((pws, i) => { if (pws) { pws.cfSeat = i; if (pws === ws) seat = i; } });
      // Bots are controlled by the host from the first moment.
      room.seats.forEach((s, i) => { if (s.kind === 'bot') room.controllers[i] = room.players[0]; });
      room.seed = Math.floor(Math.random() * 1e9);
      room.started = true;
      const seatsMsg = seatList(room);
      room.players.forEach((pws, i) => {
        if (pws) send(pws, { t: 'start', seed: room.seed, size: room.size, seats: seatsMsg, playerIdx: i });
      });
      console.log(`room ${roomCode} started multi (size ${room.size}, seed ${room.seed})`);
      return;
    }

    if (m.t === 'rejoin') {
      const room = rooms.get(m.room);
      if (!room || !room.started) return send(ws, { t: 'error', msg: 'game ended' });
      const idx = m.playerIdx;
      if (typeof idx !== 'number' || idx < 0 || idx >= room.size) return send(ws, { t: 'error', msg: 'game ended' });
      const occupant = room.players[idx];
      if (occupant && occupant.readyState === 1 && occupant !== ws) return send(ws, { t: 'error', msg: 'slot taken' });
      roomCode = m.room;
      seat = idx;
      room.players[idx] = ws;
      ws.cfRoom = roomCode; ws.cfSeat = idx;
      if (room.graceTimer) { clearTimeout(room.graceTimer); room.graceTimer = null; }
      const res = room.resolved.get(idx);
      if (!res) room.controllers[idx] = ws;         // plain return: they control their seat
      if (res && res.type === 'replace') room.controllers[idx] = ws; // RECLAIM: control returns at once
      // (a 'surrender' resolution leaves the controller with the host pilot)
      send(ws, {
        t: 'rejoined', playerIdx: idx, seed: room.seed,
        ...(room.multi ? { size: room.size, seats: seatList(room), spectatorSeats: spectatorSeats(room) } : {}),
        ...(!room.multi && res && res.type === 'surrender' ? { spectatorSeats: [idx] } : {}),
      });
      if (res && res.type === 'surrender') {
        // Spectator-only return: seatHuman tells the table the seat's
        // player is present again WITHOUT restoring control.
        broadcastOthers(room, ws, { t: 'seatHuman', seat: idx });
      } else if (res && res.type === 'replace') {
        broadcastOthers(room, ws, { t: 'seatHuman', seat: idx });
      } else if (room.multi) {
        broadcastOthers(room, ws, { t: 'seatBack', seat: idx });
        broadcastOthers(room, ws, { t: 'opponentBack', seat: idx });
      } else {
        send(room.players[1 - idx], { t: 'opponentBack' });
        broadcastOthers(room, ws, { t: 'seatBack', seat: idx });
      }
      console.log(`room ${roomCode}: seat ${idx} rejoined${res ? ' (' + res.type + ')' : ''}`);
      return;
    }

    if (m.t === 'decision') {
      const room = myRoom();
      if (!room || seat < 0 || !room.started) return;
      // Attribution: a decision FOR a seat is accepted only from that
      // seat's current controller; a classic decision (no seat field)
      // is attributed to the sender's own seat under the same rule.
      let seatAttr = seat;
      if (typeof m.seat === 'number') seatAttr = m.seat;
      if (seatAttr < 0 || seatAttr >= room.size) return;
      if (room.controllers[seatAttr] !== ws) return; // not the controller: dropped
      const fwd = { t: 'decision', seq: room.decisions.length, hook: m.hook, playerIdx: seatAttr, answer: m.answer };
      room.decisions.push(fwd);
      broadcastOthers(room, ws, fwd);
      return;
    }

    if (m.t === 'claim') {
      // FCFS quick-action claim: an ordering signal, broadcast to ALL
      // (the echo is the sender's race receipt). Never state-bearing.
      const room = myRoom();
      if (!room || seat < 0 || !room.started) return;
      broadcastAll(room, { t: 'claim', seat, uid: m.uid, window: m.window });
      return;
    }

    if (m.t === 'surrender' || m.t === 'draw') {
      const room = myRoom();
      if (!room || seat < 0) return;
      broadcastOthers(room, ws, { t: m.t, playerIdx: seat });
      return;
    }

    if (m.t === 'dropoutReplace' || m.t === 'dropoutSurrender') {
      // MANUAL dropout table decisions (Norman, 2026-10-20). Any
      // remaining connected player may press; the FIRST press for a
      // seat resolves for the whole table and is broadcast to all —
      // later presses are dropped. The host's seat is never decidable
      // (a dropped host ends the room instead). No timer is involved:
      // nothing here ever fires on its own.
      const room = myRoom();
      if (!room || !room.started || seat < 0) return;
      const s = m.seat;
      if (typeof s !== 'number' || s < 1 || s >= room.size) return;
      if (s === seat) return;                          // not your own seat
      if (room.seats[s].kind !== 'human' && room.seats[s].kind !== 'out') return;
      if (room.resolved.has(s)) return;               // first press already won
      if (room.players[s] && room.players[s].readyState === 1) return; // they are not dropped
      if (m.t === 'dropoutReplace') {
        const d = ['easy', 'medium', 'hard'].includes(m.difficulty) ? m.difficulty : 'medium';
        room.resolved.set(s, { type: 'replace', difficulty: d });
        room.seats[s] = { kind: 'bot', name: room.seats[s].name, difficulty: d };
        room.controllers[s] = room.players[0]; // the bot runs on the host
        broadcastAll(room, { t: 'dropoutReplace', seat: s, difficulty: d });
        console.log(`room ${roomCode}: seat ${s} replaced by ${d} bot (press by seat ${seat})`);
      } else {
        room.resolved.set(s, { type: 'surrender' });
        room.seats[s] = { kind: 'out', name: room.seats[s].name, difficulty: null };
        room.controllers[s] = room.players[0]; // the host pilots the seat's remaining asks
        broadcastAll(room, { t: 'dropoutSurrender', seat: s });
        console.log(`room ${roomCode}: seat ${s} surrendered by the table (press by seat ${seat})`);
      }
      return;
    }

    if (m.t === 'resync') {
      const room = myRoom();
      if (!room) return;
      const from = m.fromSeq || 0;
      send(ws, { t: 'resync', decisions: room.decisions.slice(from) });
      return;
    }
  });

  ws.on('close', () => {
    if (!roomCode || !rooms.has(roomCode) || seat < 0) return;
    const room = rooms.get(roomCode);
    // Only vacate if this socket is still the slot's current occupant
    // (a rejoin may already have replaced it).
    if (room.players[seat] !== ws) return;
    room.players[seat] = null;

    if (!room.multi) {
      // ---- Part A close semantics, exactly as before ----
      const otherIdx = 1 - seat;
      const other = room.players[otherIdx];
      if (room.seed === null) {
        // Game never started: old behavior — room dies when empty.
        send(other, { t: 'opponentLeft' });
        if (!room.players[0] && !room.players[1]) {
          rooms.delete(roomCode);
          console.log(`room ${roomCode} closed`);
        }
        return;
      }
      // Started game: hold the room for the grace period. The remaining
      // player is told their friend is away, not that the game is over.
      // (Additive Stage 3 notice: the same event as seatAway, which is
      // what raises the REPLACE / SURRENDER buttons in 2-player rooms.)
      send(other, { t: 'opponentAway' });
      send(other, { t: 'seatAway', seat });
      console.log(`room ${roomCode}: player ${seat} away (grace started)`);
      if (!room.graceTimer) {
        room.graceTimer = setTimeout(() => {
          if (!rooms.has(roomCode)) return;
          const r = rooms.get(roomCode);
          // Manual dropout model: if the table already resolved the
          // dropped seat (bot replacement or surrender), the game is
          // alive on the remaining side — the room outlives the grace
          // while anyone is still connected to it.
          if (r.resolved.has(seat) && (r.players[0] || r.players[1])) return;
          const present = r.players[0] ? 0 : (r.players[1] ? 1 : -1);
          destroyRoom(roomCode, r, present >= 0 ? r.players[present] : null);
        }, GRACE_MS);
      }
      return;
    }

    // ---- Multi-seat close ----
    if (!room.started) {
      if (seat === 0) {
        // Host left the lobby: the room closes for everyone.
        broadcastAll(room, { t: 'hostLeft' });
        rooms.delete(roomCode);
        console.log(`room ${roomCode} closed (host left lobby)`);
      } else {
        room.seats[seat] = { kind: 'open', name: '', difficulty: null };
        room.controllers[seat] = null;
        broadcastLobby(room);
        console.log(`room ${roomCode}: seat ${seat} left lobby`);
      }
      return;
    }
    if (seat === 0) {
      // Host dropped mid-game: the room is held for the host's rejoin
      // grace, then ends cleanly for everyone (hostLeft). No other
      // seat's stall is affected meanwhile.
      broadcastAll(room, { t: 'seatAway', seat: 0 });
      broadcastAll(room, { t: 'opponentAway', seat: 0 });
      console.log(`room ${roomCode}: host away (grace started)`);
      if (!room.graceTimer) {
        room.graceTimer = setTimeout(() => {
          if (!rooms.has(roomCode)) return;
          const r = rooms.get(roomCode);
          broadcastAll(r, { t: 'hostLeft' });
          rooms.delete(roomCode);
          console.log(`room ${roomCode} closed (hostLeft)`);
        }, GRACE_MS);
      }
      return;
    }
    // A non-host seat dropped: the seat STALLS. The table gets the
    // away notice (which raises the REPLACE / SURRENDER buttons) and
    // nothing else happens — no timer, no takeover, ever.
    broadcastAll(room, { t: 'seatAway', seat });
    broadcastAll(room, { t: 'opponentAway', seat });
    console.log(`room ${roomCode}: seat ${seat} away (stalled — table decides)`);
  });
});
