// Champion Forge multiplayer relay server (prototype).
// Dumb relay: forwards decisions between two players in a private room.
// Both clients run the deterministic engine; server only relays.
// Run: node relay-server.js [port]
import { WebSocketServer } from 'ws';
import { randomBytes } from 'crypto';

const PORT = parseInt(process.argv[2] || '8080', 10);
const rooms = new Map(); // code -> { players: [ws, ws], decisions: [], seed }

const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
function makeCode() {
  let code;
  do {
    code = Array.from(randomBytes(4)).map(b => CODE_ALPHABET[b % 32]).join('');
  } while (rooms.has(code));
  return code;
}

function send(ws, msg) {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
}

const wss = new WebSocketServer({ port: PORT });
console.log(`Relay server on :${PORT}`);

wss.on('connection', (ws) => {
  let roomCode = null;
  let playerIdx = -1;

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return send(ws, { t: 'error', msg: 'bad json' }); }

    if (m.t === 'create') {
      roomCode = makeCode();
      playerIdx = 0;
      rooms.set(roomCode, { players: [ws, null], decisions: [], seed: null });
      send(ws, { t: 'created', room: roomCode, playerIdx: 0 });
      console.log(`room ${roomCode} created`);
    } else if (m.t === 'join') {
      const room = rooms.get(m.room);
      if (!room) return send(ws, { t: 'error', msg: 'no such room' });
      if (room.players[1]) return send(ws, { t: 'error', msg: 'room full' });
      roomCode = m.room;
      playerIdx = 1;
      room.players[1] = ws;
      send(ws, { t: 'joined', room: roomCode, playerIdx: 1 });
      // Both present: start the game with a shared seed.
      room.seed = Math.floor(Math.random() * 1e9);
      for (const pws of room.players) send(pws, { t: 'start', seed: room.seed });
      console.log(`room ${roomCode} started (seed ${room.seed})`);
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
    if (roomCode && rooms.has(roomCode)) {
      const room = rooms.get(roomCode);
      room.players[playerIdx] = null;
      const other = room.players[1 - playerIdx];
      send(other, { t: 'opponentLeft' });
      if (!room.players[0] && !room.players[1]) {
        rooms.delete(roomCode);
        console.log(`room ${roomCode} closed`);
      }
    }
  });
});
