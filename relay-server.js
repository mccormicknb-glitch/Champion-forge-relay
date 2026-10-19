// Champion Forge multiplayer relay server.
// Dumb relay: forwards decisions between two players in a private room.
// Both clients run the deterministic engine; server only relays.
// Started rooms survive a disconnect for a grace period so a player who
// switches apps (or loses signal) can rejoin and resync instead of
// ending the game for both sides.
// Run: node relay-server.js [port]
import http from 'http';
import { WebSocketServer } from 'ws';
import { randomBytes } from 'crypto';

const PORT = parseInt(process.env.PORT || process.argv[2] || '8080', 10);
const GRACE_MS = 10 * 60 * 1000; // hold a started room 10 min after a drop
const rooms = new Map(); // code -> { players: [ws, ws], decisions: [], seed, graceTimer }

const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
function makeCode() {
  let code;
  do {
    code = Array.from(randomBytes(4)).map(b => CODE_ALPHABET[b % CODE_ALPHABET.length]).join('');
  } while (rooms.has(code));
  return code;
}

function send(ws, msg) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
}

const server = http.createServer((req, res) => {
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

function destroyRoom(code, room, notifyIdx) {
  if (room.graceTimer) { clearTimeout(room.graceTimer); room.graceTimer = null; }
  if (notifyIdx === 0 || notifyIdx === 1) send(room.players[notifyIdx], { t: 'opponentLeft' });
  rooms.delete(code);
  console.log(`room ${code} closed`);
}

wss.on('connection', (ws) => {
  console.log('client connected');
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  let roomCode = null;
  let playerIdx = -1;

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return send(ws, { t: 'error', msg: 'bad json' }); }

    if (m.t === 'create') {
      roomCode = makeCode();
      playerIdx = 0;
      rooms.set(roomCode, { players: [ws, null], decisions: [], seed: null, graceTimer: null });
      send(ws, { t: 'created', room: roomCode, playerIdx: 0 });
      console.log(`room ${roomCode} created`);
    } else if (m.t === 'join') {
      const room = rooms.get(m.room);
      if (!room) return send(ws, { t: 'error', msg: 'no such room' });
      if (room.seed !== null) return send(ws, { t: 'error', msg: 'room full' }); // already started; rejoin is the way back
      if (room.players[1]) return send(ws, { t: 'error', msg: 'room full' });
      roomCode = m.room;
      playerIdx = 1;
      room.players[1] = ws;
      send(ws, { t: 'joined', room: roomCode, playerIdx: 1 });
      // Both present: start the game with a shared seed.
      room.seed = Math.floor(Math.random() * 1e9);
      for (const pws of room.players) send(pws, { t: 'start', seed: room.seed });
      console.log(`room ${roomCode} started (seed ${room.seed})`);
    } else if (m.t === 'rejoin') {
      const room = rooms.get(m.room);
      if (!room || room.seed === null) return send(ws, { t: 'error', msg: 'game ended' });
      const idx = m.playerIdx;
      if (idx !== 0 && idx !== 1) return send(ws, { t: 'error', msg: 'game ended' });
      const occupant = room.players[idx];
      if (occupant && occupant.readyState === 1) return send(ws, { t: 'error', msg: 'slot taken' });
      roomCode = m.room;
      playerIdx = idx;
      room.players[idx] = ws;
      if (room.graceTimer) { clearTimeout(room.graceTimer); room.graceTimer = null; }
      send(ws, { t: 'rejoined', playerIdx: idx, seed: room.seed });
      send(room.players[1 - idx], { t: 'opponentBack' });
      console.log(`room ${roomCode}: player ${idx} rejoined`);
    } else if (m.t === 'decision') {
      const room = rooms.get(roomCode);
      if (!room || playerIdx < 0) return;
      // Tag with sequence number for resync.
      const seq = room.decisions.length;
      const fwd = { t: 'decision', seq, hook: m.hook, playerIdx, answer: m.answer };
      room.decisions.push(fwd);
      const other = room.players[1 - playerIdx];
      send(other, fwd);
    } else if (m.t === 'resync') {
      const room = rooms.get(roomCode);
      if (!room) return;
      const from = m.fromSeq || 0;
      send(ws, { t: 'resync', decisions: room.decisions.slice(from) });
    }
  });

  ws.on('close', () => {
    if (!roomCode || !rooms.has(roomCode) || playerIdx < 0) return;
    const room = rooms.get(roomCode);
    // Only vacate if this socket is still the slot's current occupant
    // (a rejoin may already have replaced it).
    if (room.players[playerIdx] !== ws) return;
    room.players[playerIdx] = null;
    const otherIdx = 1 - playerIdx;
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
    send(other, { t: 'opponentAway' });
    console.log(`room ${roomCode}: player ${playerIdx} away (grace started)`);
    if (!room.graceTimer) {
      room.graceTimer = setTimeout(() => {
        if (!rooms.has(roomCode)) return;
        const r = rooms.get(roomCode);
        const present = r.players[0] ? 0 : (r.players[1] ? 1 : -1);
        destroyRoom(roomCode, r, present);
      }, GRACE_MS);
    }
  });
});
