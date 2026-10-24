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
// v3 (2026-10-23, MP-1..MP-4): heartbeat tolerates two missed pongs;
// started-room away notices are park-debounced (RELAY_PARK_MS); rejoin
// replaces zombie occupants; dropout presses require a seat to have
// been away past the park window; the relay mints one gameId per match
// and carries it on 'start'; partial envelopes record the sender seat;
// duplicate/conflicting finishes for one gameId are deduped / kept as
// .conflict copies and /log-count counts matches. All changes are
// additive or delayed-notice only — v2 clients observe no breakage.
//
// v4 (2026-10-23, Save & Quit): additive only — a room may be created
// from a saved game ({t:'create', resume:{seed, gameId, size, mySeat,
// seats, decisions}}): the relay preloads the seed + decision log and
// seats the creator at their ORIGINAL seat; joins into the pending
// room take the remaining human seats ({t:'join', seat} asks for one
// by number); 'start' then carries resumed:true and keeps the saved
// seed + gameId instead of minting new ones, so both phones rebuild
// by replaying the preloaded log through the normal resync path and
// fresh decisions append to the same log. Also forwards
// {t:'saveQuit'} to the rest of the table. Clients that never send a
// resume create observe v3 exactly.
//
// Leaderboard (2026-10-23, additive): finished-report envelopes may
// carry a sanitized `stats` block + `playedAt`; accepted stats persist
// as <stem>.stats.json beside the report (same storage + GitHub sink),
// fold into monthly (America/New_York) top-10 boards in memory, and
// rebuild on boot by scanning the stored stats files. The public
// read-only GET /leaderboard serves the boards. Developed/verified in
// this file (relay-server-v4-lb.js = the save-quit v4 candidate plus
// this delta); it becomes the single relay-server-v4.js deploy
// candidate once the save-quit merge check is done.
//
// Run: node relay-server-v4.js [port]
// Env: RELAY_GRACE_MS overrides the 10-minute room-hold grace (tests).
//      RELAY_HEARTBEAT_MS overrides the 25s heartbeat tick (tests).
//      RELAY_PARK_MS overrides the 60s park debounce (tests).
import http from 'http';
import { WebSocketServer } from 'ws';
import { randomBytes } from 'crypto';
import { promises as fsp } from 'fs';
import path from 'path';

const PORT = parseInt(process.env.PORT || process.argv[2] || '8080', 10);
const GRACE_MS = parseInt(process.env.RELAY_GRACE_MS || String(10 * 60 * 1000), 10);
const PARK_MS = parseInt(process.env.RELAY_PARK_MS || String(60 * 1000), 10);
const HEARTBEAT_MS = parseInt(process.env.RELAY_HEARTBEAT_MS || String(25 * 1000), 10);
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
                          battle-history export (JSON {"text": "..."}
                          or a raw text body). Stored as its own .txt
                          file, byte-identical to the player's manual
                          download. The envelope may also carry "gameId"
                          and "builds" (one player-card record per
                          seat); builds are stored beside the history
                          as <same-stem>.builds.json.
   GET  /logs           — token-guarded list of stored logs.
   GET  /logs/<name>    — token-guarded fetch of one stored log.
   GET  /log-count      — unguarded count of stored logs (health check).
   The fetch endpoints require the LOG_FETCH_TOKEN env var; without it
   they answer 503. Storage is LOG_DIR (default ./game-logs). If
   LOG_GH_TOKEN and LOG_GH_REPO are also set, every log is additionally
   committed to that GitHub repo under logs/ (durable sink). Nothing
   here touches the WebSocket room/protocol logic below.

   Quit tracking (additive, 2026-10-22): an envelope whose "status"
   is present and not "finished" is a PARTIAL report for a game that
   never finished (statuses: checkpoint, quit, abandoned,
   disconnected, host-ended). Partials are stored per gameId as
   partial-<gameId>.txt / partial-<gameId>.builds.json (the builds
   file carries status + progress), overwritten by newer partials —
   except a mere "checkpoint" never overwrites a final partial
   (quit/abandoned/disconnected/host-ended). When the finished
   report for a gameId arrives, its partials are deleted
   best-effort: the finished report supersedes them, and review
   keys on gameId + status either way. */

const LOG_DIR = process.env.LOG_DIR || './game-logs';
const LOG_FETCH_TOKEN = process.env.LOG_FETCH_TOKEN || '';
const LOG_GH_TOKEN = process.env.LOG_GH_TOKEN || '';
const LOG_GH_REPO = process.env.LOG_GH_REPO || '';
const LOG_MAX_BYTES = 2 * 1024 * 1024;

async function ghPutLogFile(name, content) {
  const headers = {
    'Authorization': `Bearer ${LOG_GH_TOKEN}`,
    'Accept': 'application/vnd.github+json',
    'Content-Type': 'application/json',
  };
  // Overwrite support (partials are re-put under the same name as the
  // game progresses): the contents API refuses an update without the
  // current sha, so fetch it first when the file already exists.
  let sha = null;
  try {
    const g = await fetch(`https://api.github.com/repos/${LOG_GH_REPO}/contents/logs/${name}`, { headers });
    if (g.ok) { const j = await g.json(); if (j && j.sha) sha = j.sha; }
  } catch { /* create path below still applies */ }
  const r = await fetch(`https://api.github.com/repos/${LOG_GH_REPO}/contents/logs/${name}`, {
    method: 'PUT',
    headers,
    body: JSON.stringify({
      message: `Battle log ${name}`,
      content: Buffer.from(content, 'utf8').toString('base64'),
      ...(sha ? { sha } : {}),
    }),
  });
  if (!r.ok) console.log(`battle log ${name}: GitHub sink answered ${r.status} (filesystem copy kept)`);
}

async function ghDeleteLogFile(name) {
  const headers = {
    'Authorization': `Bearer ${LOG_GH_TOKEN}`,
    'Accept': 'application/vnd.github+json',
    'Content-Type': 'application/json',
  };
  try {
    const g = await fetch(`https://api.github.com/repos/${LOG_GH_REPO}/contents/logs/${name}`, { headers });
    if (!g.ok) return; // nothing stored under that name
    const j = await g.json();
    if (!j || !j.sha) return;
    const r = await fetch(`https://api.github.com/repos/${LOG_GH_REPO}/contents/logs/${name}`, {
      method: 'DELETE',
      headers,
      body: JSON.stringify({ message: `Battle log ${name} superseded by the finished report`, sha: j.sha }),
    });
    if (!r.ok) console.log(`battle log ${name}: GitHub delete answered ${r.status} (partial copy may remain)`);
  } catch (e) {
    console.log(`battle log ${name}: GitHub delete error (${e.message})`);
  }
}

async function storeBattleLog(text, builds, gameId, statsRec) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const stem = `champion-forge-battle-${stamp}-${randomBytes(3).toString('hex')}`;
  const name = `${stem}.txt`;
  await fsp.mkdir(LOG_DIR, { recursive: true });
  await fsp.writeFile(path.join(LOG_DIR, name), text, 'utf8');
  const files = [name];
  let buildsName = null;
  if (Array.isArray(builds) && builds.length > 0) {
    buildsName = `${stem}.builds.json`;
    const payload = JSON.stringify({ gameId: gameId || stem, storedAt: new Date().toISOString(), builds }, null, 1);
    await fsp.writeFile(path.join(LOG_DIR, buildsName), payload, 'utf8');
    files.push(buildsName);
  }
  // Leaderboard: the accepted stats block persists beside the report
  // (same sink below) and folds into the in-memory monthly boards.
  // This is the ONLY ingest path that contributes stats: duplicates
  // dedupe before reaching here and conflicts never carry stats, so a
  // gameId's boards are fed exactly once, by its primary finish.
  let statsName = null;
  if (statsRec) {
    statsName = `${stem}.stats.json`;
    const payload = JSON.stringify({ gameId: gameId || stem, storedAt: new Date().toISOString(), playedAt: statsRec.playedAt, month: statsRec.month, seats: statsRec.seats }, null, 1);
    await fsp.writeFile(path.join(LOG_DIR, statsName), payload, 'utf8');
    files.push(statsName);
    lbFoldStats(statsRec, gameId || stem);
  }
  if (LOG_GH_TOKEN && LOG_GH_REPO) {
    for (const f of files) {
      try {
        const content = await fsp.readFile(path.join(LOG_DIR, f), 'utf8');
        await ghPutLogFile(f, content);
      } catch (e) {
        console.log(`battle log ${f}: GitHub sink error (${e.message}); filesystem copy kept`);
      }
    }
  }
  return { name, buildsName, statsName };
}

/* -------- finish integrity (MP-4): one match per gameId --------
   A finished envelope carrying a gameId is checked against the finish
   builds files already stored for that gameId. An identical re-post
   is a duplicate (no second copy); a different history for the same
   gameId is divergence evidence and is kept as a .conflict copy of
   the original stem instead of a second "game". */
async function findFinishesByGameId(gameId) {
  let files;
  try { files = await fsp.readdir(LOG_DIR); } catch { return []; }
  const buildsFiles = files
    .filter((f) => f.startsWith('champion-forge-battle-') && f.endsWith('.builds.json'))
    .sort();
  const matches = [];
  for (const buildsName of buildsFiles) {
    try {
      const payload = JSON.parse(await fsp.readFile(path.join(LOG_DIR, buildsName), 'utf8'));
      if (!payload || payload.gameId !== gameId) continue;
      const name = buildsName.slice(0, -'.builds.json'.length) + '.txt';
      const text = await fsp.readFile(path.join(LOG_DIR, name), 'utf8');
      matches.push({ name, buildsName, text, conflict: buildsName.includes('.conflict.') });
    } catch { /* unreadable / pairless builds file: not a finish we can match */ }
  }
  // The primary (non-conflict) finish, when one exists, is the match.
  matches.sort((a, b) => Number(a.conflict) - Number(b.conflict));
  return matches;
}

async function storeConflictLog(text, builds, gameId, existingStem) {
  const name = `${existingStem}.conflict.txt`;
  const buildsName = `${existingStem}.conflict.builds.json`;
  await fsp.mkdir(LOG_DIR, { recursive: true });
  await fsp.writeFile(path.join(LOG_DIR, name), text, 'utf8');
  const payload = JSON.stringify({ gameId: gameId || existingStem, storedAt: new Date().toISOString(), builds: Array.isArray(builds) ? builds : [], conflict: true }, null, 1);
  await fsp.writeFile(path.join(LOG_DIR, buildsName), payload, 'utf8');
  if (LOG_GH_TOKEN && LOG_GH_REPO) {
    for (const f of [name, buildsName]) {
      try {
        const content = await fsp.readFile(path.join(LOG_DIR, f), 'utf8');
        await ghPutLogFile(f, content);
      } catch (e) {
        console.log(`battle log ${f}: GitHub sink error (${e.message}); filesystem copy kept`);
      }
    }
  }
  return { name, buildsName };
}

/* ---------------- partial (unfinished game) reports ---------------- */
const PARTIAL_FINAL_STATUSES = new Set(['quit', 'abandoned', 'disconnected', 'host-ended']);

function partialStem(gameId) {
  const safe = String(gameId || '').replace(/[^a-zA-Z0-9._-]/g, '').slice(0, 80);
  return safe ? `partial-${safe}` : null;
}

async function storePartialLog(text, builds, gameId, status, progress, seat) {
  const stem = partialStem(gameId);
  if (!stem) return null;
  const name = `${stem}.txt`;
  const buildsName = `${stem}.builds.json`;
  await fsp.mkdir(LOG_DIR, { recursive: true });
  // Rank guard: a late-arriving checkpoint (e.g. flushed from a
  // client's offline queue after the player already quit) must never
  // downgrade a final partial back to a mere progress note.
  if (!PARTIAL_FINAL_STATUSES.has(status)) {
    try {
      const prev = JSON.parse(await fsp.readFile(path.join(LOG_DIR, buildsName), 'utf8'));
      if (prev && PARTIAL_FINAL_STATUSES.has(prev.status)) return { name, buildsName, kept: true };
    } catch { /* no prior partial — store this one */ }
  }
  await fsp.writeFile(path.join(LOG_DIR, name), text, 'utf8');
  const payload = JSON.stringify({ gameId, status, progress: progress || null, seat: typeof seat === 'number' ? seat : null, storedAt: new Date().toISOString(), builds: Array.isArray(builds) ? builds : [] }, null, 1);
  await fsp.writeFile(path.join(LOG_DIR, buildsName), payload, 'utf8');
  if (LOG_GH_TOKEN && LOG_GH_REPO) {
    for (const f of [name, buildsName]) {
      try {
        const content = await fsp.readFile(path.join(LOG_DIR, f), 'utf8');
        await ghPutLogFile(f, content);
      } catch (e) {
        console.log(`partial log ${f}: GitHub sink error (${e.message}); filesystem copy kept`);
      }
    }
  }
  return { name, buildsName };
}

async function deletePartialLog(gameId) {
  const stem = partialStem(gameId);
  if (!stem) return;
  for (const f of [`${stem}.txt`, `${stem}.builds.json`]) {
    try { await fsp.unlink(path.join(LOG_DIR, f)); } catch { /* not present */ }
    if (LOG_GH_TOKEN && LOG_GH_REPO) { try { await ghDeleteLogFile(f); } catch { /* best effort */ } }
  }
}

/* ---------------- leaderboard (additive, 2026-10-23) ----------------
   Monthly top-10 boards (America/New_York calendar months) fed by the
   `stats` block on FINISHED POST /log envelopes. Stats are computed by
   the game engine on the client from its own event stream; the relay
   only sanitizes, persists, folds, and serves. Sanitized stats persist
   as <stem>.stats.json beside the report (storeBattleLog above, same
   filesystem + GitHub sink); boards live in memory and rebuild on boot
   by scanning those files (rebuildLeaderboards below), the same scan
   pattern findFinishesByGameId uses for finishes. Entry fields are
   player-supplied strings — API consumers MUST render them as text
   only. The endpoint exposes champion names/races/values/dates only. */

const LB_METRICS = [
  { key: 'largest_single_attack', dir: -1 },
  { key: 'largest_turn_damage', dir: -1 },
  { key: 'most_damage_game', dir: -1 },
  { key: 'biggest_heal', dir: -1 },
  { key: 'most_summons_game', dir: -1 },
  { key: 'fastest_win', dir: 1 }, // fewest rounds to win — ascending
];
const LB_BOARDS = new Map(); // month 'YYYY-MM' -> { metricKey: [entry, ...] }
const lbMonthFmt = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit' });
const lbLabelFmt = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', month: 'long', year: 'numeric' });

function lbMonthKey(d) {
  let y = '', m = '';
  for (const p of lbMonthFmt.formatToParts(d)) {
    if (p.type === 'year') y = p.value;
    if (p.type === 'month') m = p.value;
  }
  return `${y}-${m}`;
}

function lbMonthLabel(monthKey) {
  const parts = String(monthKey).split('-').map(Number);
  if (!parts[0] || !parts[1]) return String(monthKey);
  return lbLabelFmt.format(new Date(Date.UTC(parts[0], parts[1] - 1, 15, 12, 0, 0)));
}

function lbCleanStr(v, cap) {
  if (typeof v !== 'string') return null;
  const s = v.replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, cap);
  return s.length ? s : null;
}

function lbMetricValue(metrics, key) {
  if (!metrics || typeof metrics !== 'object') return null;
  const v = metrics[key];
  if (typeof v !== 'number' || !Number.isFinite(v)) return null;
  const n = Math.round(v);
  if (key === 'fastest_win') return (n >= 1 && n <= 10000) ? n : null;
  return (n >= 0 && n <= 1e9) ? n : null;
}

// Sanitizes a raw envelope `stats` block. Returns null when nothing
// usable remains (the report then stores exactly as a no-stats one).
function lbSanitizeStats(raw) {
  if (!raw || typeof raw !== 'object' || !Array.isArray(raw.seats)) return null;
  const seats = [];
  for (const s of raw.seats.slice(0, 5)) {
    if (!s || typeof s !== 'object') continue;
    const name = lbCleanStr(s.name, 24) || 'Unknown';
    const races = [];
    if (Array.isArray(s.races)) {
      for (const r of s.races.slice(0, 2)) {
        const c = lbCleanStr(r, 24);
        if (c) races.push(c);
      }
    }
    if (!s.opponent || typeof s.opponent !== 'object'
      || (s.opponent.kind !== 'human' && s.opponent.kind !== 'ai')) continue;
    const difficulty = ['easy', 'medium', 'hard'].includes(s.opponent.difficulty) ? s.opponent.difficulty : null;
    const opponent = { kind: s.opponent.kind, difficulty: s.opponent.kind === 'ai' ? difficulty : null };
    const metrics = {};
    for (const { key } of LB_METRICS) {
      const v = lbMetricValue(s.metrics, key);
      if (v !== null) metrics[key] = v;
    }
    if (Object.keys(metrics).length === 0) continue;
    seats.push({ name, races, opponent, metrics });
  }
  return seats.length ? { seats } : null;
}

// The envelope's playedAt is the game's finish time and decides the
// board month (a finish flushed late from a client's offline queue
// still belongs to its game's month). Accepted when it parses and is
// within a plausibility window of ±400 days — the relay's trust model
// already accepts client-reported finishes; the window only rejects
// absurd clock garbage. Anything else falls back to receive time.
function lbPlayedAt(rawPlayedAt) {
  const now = Date.now();
  if (typeof rawPlayedAt === 'string') {
    const t = Date.parse(rawPlayedAt);
    if (Number.isFinite(t) && Math.abs(t - now) <= 400 * 24 * 3600 * 1000) return new Date(t);
  }
  return new Date(now);
}

function lbBoardFor(month) {
  let b = LB_BOARDS.get(month);
  if (!b) {
    b = {};
    for (const { key } of LB_METRICS) b[key] = [];
    LB_BOARDS.set(month, b);
  }
  return b;
}

function lbFold(month, seats, gameId, dateISO) {
  const board = lbBoardFor(month);
  for (const seat of seats) {
    if (!seat || typeof seat !== 'object' || !seat.metrics) continue;
    for (const { key, dir } of LB_METRICS) {
      const v = seat.metrics[key];
      if (typeof v !== 'number') continue;
      const list = board[key];
      list.push({ name: seat.name, races: Array.isArray(seat.races) ? seat.races : [], value: v, opponent: seat.opponent || null, date: dateISO, gameId: gameId || null });
      // Value order per metric direction; earliest date wins ties.
      list.sort((a, b) => (dir * (a.value - b.value)) || (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
      if (list.length > 10) list.length = 10;
    }
  }
}

function lbFoldStats(rec, gameId) {
  lbFold(rec.month, rec.seats, gameId, rec.playedAt);
}

function lbSnapshot(month) {
  const board = LB_BOARDS.get(month);
  const metrics = {};
  for (const { key } of LB_METRICS) metrics[key] = board ? board[key] : [];
  return { month, monthLabel: lbMonthLabel(month), metrics };
}

// Boot rebuild: fold every persisted stats file. Stored playedAt was
// validated at ingest, so it is trusted here; month is recomputed
// from it rather than trusting the stored month field.
async function rebuildLeaderboards() {
  let files;
  try { files = await fsp.readdir(LOG_DIR); } catch { return; }
  const statsFiles = files
    .filter((f) => f.startsWith('champion-forge-battle-') && f.endsWith('.stats.json') && !f.includes('.conflict.'))
    .sort();
  let folded = 0;
  for (const f of statsFiles) {
    try {
      const rec = JSON.parse(await fsp.readFile(path.join(LOG_DIR, f), 'utf8'));
      if (!rec || !Array.isArray(rec.seats)) continue;
      const t = Date.parse(rec.playedAt);
      const when = Number.isFinite(t) ? new Date(t) : new Date();
      lbFold(lbMonthKey(when), rec.seats, typeof rec.gameId === 'string' ? rec.gameId : null, when.toISOString());
      folded++;
    } catch { /* unreadable stats file: skipped, boards stay honest */ }
  }
  if (folded) console.log(`leaderboard: rebuilt boards from ${folded} stored stats file(s)`);
}
rebuildLeaderboards();

async function listBattleLogs() {
  try {
    const files = await fsp.readdir(LOG_DIR);
    return files.filter((f) => f.startsWith('champion-forge-battle-') && f.endsWith('.txt')).sort();
  } catch { return []; }
}

async function listPartialLogs() {
  try {
    const files = await fsp.readdir(LOG_DIR);
    return files.filter((f) => f.startsWith('partial-') && f.endsWith('.txt')).sort();
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
        let builds = null;
        let gameId = null;
        let status = null;
        let progress = null;
        let seat = null;
        let statsRaw = null;
        let playedAtRaw = null;
        try {
          const j = JSON.parse(raw);
          if (j && typeof j.text === 'string') text = j.text;
          if (j && Array.isArray(j.builds)) builds = j.builds;
          if (j && typeof j.gameId === 'string') gameId = j.gameId.slice(0, 80);
          if (j && typeof j.status === 'string') status = j.status.slice(0, 24);
          if (j && j.progress && typeof j.progress === 'object') progress = j.progress;
          if (j && typeof j.seat === 'number' && Number.isFinite(j.seat)) seat = j.seat;
          if (j && j.stats && typeof j.stats === 'object') statsRaw = j.stats;
          if (j && typeof j.playedAt === 'string') playedAtRaw = j.playedAt;
        } catch { /* raw text body: store as-is */ }
        if (!text || text.trim().length < 20) return reply(400, { ok: false, error: 'empty log' });
        if (status && status !== 'finished' && gameId) {
          const stored = await storePartialLog(text, builds, gameId, status, progress, seat);
          if (stored) {
            console.log(`partial log stored: ${stored.name} (status ${status}, ${text.length} chars${stored.kept ? ', kept existing final partial' : ''})`);
            return reply(202, { ok: true, partial: true, status, id: stored.name, buildsId: stored.buildsName });
          }
        }
        // Finish integrity (MP-4): a gameId that already has a finish
        // never gains a second "game" — identical text dedupes onto
        // the existing file, different text becomes its .conflict copy.
        if (gameId) {
          const prior = await findFinishesByGameId(gameId);
          if (prior.length > 0) {
            try { await deletePartialLog(gameId); } catch { /* supersede is best-effort */ }
            const identical = prior.find((p) => p.text === text);
            if (identical) {
              console.log(`battle log duplicate for gameId ${gameId}: identical to ${identical.name}, not stored again`);
              return reply(202, { ok: true, id: identical.name, buildsId: identical.buildsName, duplicate: true });
            }
            const primary = prior[0];
            const existingStem = primary.name.slice(0, -'.txt'.length).replace(/\.conflict$/, '');
            const stored = await storeConflictLog(text, builds, gameId, existingStem);
            console.log(`battle log conflict for gameId ${gameId}: stored as ${stored.name} (original ${primary.name} kept)`);
            return reply(202, { ok: true, id: stored.name, buildsId: stored.buildsName, conflict: true });
          }
        }
        // Leaderboard: stats count only on this primary-finish path —
        // the partial branch above returned already, and duplicates /
        // conflicts returned in the integrity block, so a match feeds
        // the boards exactly once. Invalid stats sanitize to null and
        // the report stores exactly as a no-stats report.
        const statsClean = lbSanitizeStats(statsRaw);
        let statsRec = null;
        if (statsClean) {
          const when = lbPlayedAt(playedAtRaw);
          statsRec = { seats: statsClean.seats, playedAt: when.toISOString(), month: lbMonthKey(when) };
        }
        const stored = await storeBattleLog(text, builds, gameId, statsRec);
        if (gameId) { try { await deletePartialLog(gameId); } catch { /* supersede is best-effort */ } }
        console.log(`battle log stored: ${stored.name} (${text.length} chars${stored.buildsName ? `, builds: ${stored.buildsName}` : ''}${stored.statsName ? `, stats: ${stored.statsName}` : ''})`);
        return reply(202, { ok: true, id: stored.name, buildsId: stored.buildsName, statsAccepted: !!statsRec });
      } catch (e) {
        console.log('battle log store failed:', e.message);
        return reply(500, { ok: false, error: 'store failed' });
      }
    });
    return;
  }

  if (req.method === 'GET' && url.pathname === '/log-count') {
    Promise.all([listBattleLogs(), listPartialLogs()]).then(([files, partials]) => {
      // MP-4: count MATCHES, not posts — a .conflict.txt is a second
      // report of a match already counted, never a second match. The
      // /logs listing below still lists every file, conflicts included.
      const matches = files.filter((f) => !f.endsWith('.conflict.txt'));
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ count: matches.length, partials: partials.length }));
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
      Promise.all([listBattleLogs(), listPartialLogs()]).then(([files, partials]) => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ logs: files, partials }));
      });
      return;
    }
    const name = path.basename(url.pathname.slice('/logs/'.length));
    // MP-4 conflict copies (<stem>.conflict.txt / <stem>.conflict.builds.json)
    // are ordinary stored logs for fetch purposes and must validate here.
    const isHistory = (name.startsWith('champion-forge-battle-') || name.startsWith('partial-')) && (name.endsWith('.txt') || name.endsWith('.conflict.txt'));
    const isBuilds = (name.startsWith('champion-forge-battle-') || name.startsWith('partial-')) && (name.endsWith('.builds.json') || name.endsWith('.conflict.builds.json'));
    if (!isHistory && !isBuilds) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      return res.end('no such log');
    }
    fsp.readFile(path.join(LOG_DIR, name), 'utf8').then((text) => {
      res.writeHead(200, { 'Content-Type': isBuilds ? 'application/json; charset=utf-8' : 'text/plain; charset=utf-8' });
      res.end(text);
    }).catch(() => {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('no such log');
    });
    return;
  }

  // Leaderboard (2026-10-23): public, read-only monthly top-10s.
  // ?month=YYYY-MM serves any month the boot rebuild found; the
  // default is the current America/New_York month.
  if (req.method === 'GET' && url.pathname === '/leaderboard') {
    const q = url.searchParams ? url.searchParams.get('month') : null;
    const month = (q && /^\d{4}-(0[1-9]|1[0-2])$/.test(q)) ? q : lbMonthKey(new Date());
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify(lbSnapshot(month)));
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
// connections so the away/grace flow starts. MP-1 tolerance: a socket is
// terminated only after TWO consecutive missed pongs (~75s at the 25s
// default tick) — a tab that merely froze/slept through one cycle keeps
// its socket. ws.missedPongs is the per-socket consecutive-miss counter
// (reset on every pong); the rejoin handler reads it to tell a zombie
// occupant from a genuinely live one.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.missedPongs = (ws.missedPongs || 0) + 1;
      if (ws.missedPongs >= 2) { ws.terminate(); continue; }
    } else {
      ws.missedPongs = 0;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch { /* socket already gone */ }
  }
}, HEARTBEAT_MS);
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
  clearAllParks(room);
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
    gameId: null,        // MP-4: minted once, when the game starts
    started: false,
    resumePending: false, // v4: created from a saved game; seed + log preloaded, waiting for the table to refill
    creatorWs: null,      // v4: the re-hosting connection (may start a multi resume room from any seat)
    resolved: new Map(), // seat -> {type:'replace', difficulty} | {type:'surrender'}
    graceTimer: null,
    awaySince: {},       // MP-1/MP-3: seat -> Date.now() of its socket close (started rooms)
    parkTimers: {},      // MP-1: seat -> pending away-broadcast timer
  };
}

/* ---------------- park debounce (MP-1 / MP-2) ----------------
   A socket that closes in a STARTED room does not raise the away
   notices at once: the seat is parked for PARK_MS. A rejoin inside
   the window cancels the timer silently — no away broadcast ever
   happens, so a brief phone backgrounding is invisible at the table.
   If the timer fires with the seat still gone, it broadcasts exactly
   the notices v2 broadcast at close. awaySince survives the firing:
   MP-3's dropout window is measured from it, and only a rejoin (or
   the room's end) clears it. */
function clearPark(room, seatIdx) {
  if (room.parkTimers[seatIdx]) { clearTimeout(room.parkTimers[seatIdx]); delete room.parkTimers[seatIdx]; }
  delete room.awaySince[seatIdx];
}

function schedulePark(room, seatIdx, fire) {
  if (room.parkTimers[seatIdx]) clearTimeout(room.parkTimers[seatIdx]);
  room.awaySince[seatIdx] = Date.now();
  room.parkTimers[seatIdx] = setTimeout(() => {
    delete room.parkTimers[seatIdx];
    if (!rooms.has(room.code)) return;
    const r = rooms.get(room.code);
    if (r.players[seatIdx]) return; // seat reoccupied — nothing to announce
    fire(r);
  }, PARK_MS);
}

function clearAllParks(room) {
  for (const k of Object.keys(room.parkTimers)) clearTimeout(room.parkTimers[k]);
  room.parkTimers = {};
  room.awaySince = {};
}

/* ---------------- connection handling ---------------- */

wss.on('connection', (ws) => {
  console.log('client connected');
  ws.isAlive = true;
  ws.missedPongs = 0;
  ws.on('pong', () => { ws.isAlive = true; ws.missedPongs = 0; });
  let roomCode = null;
  let seat = -1;

  const myRoom = () => (roomCode ? rooms.get(roomCode) : undefined);

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return send(ws, { t: 'error', msg: 'bad json' }); }

    if (m.t === 'create') {
      // v4: re-hosting a saved game. The resume payload is validated
      // strictly; anything malformed falls through to a classic
      // create rather than failing the player's tap.
      const R = (m.resume && typeof m.resume === 'object') ? m.resume : null;
      const rSize = R && typeof R.size === 'number' ? Math.floor(R.size) : 0;
      const rSeat = R && typeof R.mySeat === 'number' ? Math.floor(R.mySeat) : -1;
      const rSeatsOk = R && Array.isArray(R.seats) && R.seats.length === rSize
        && R.seats.every((s) => s && (s.kind === 'human' || s.kind === 'bot' || s.kind === 'out'))
        && R.seats[0] && R.seats[0].kind === 'human';
      const rLogOk = R && Array.isArray(R.decisions) && R.decisions.length <= 20000
        && R.decisions.every((d) => d && typeof d.hook === 'string' && typeof d.playerIdx === 'number' && d.playerIdx >= 0 && d.playerIdx < rSize && 'answer' in d);
      if (R && typeof R.seed === 'number' && rSize >= 2 && rSize <= 5
        && rSeat >= 0 && rSeat < rSize && rSeatsOk && R.seats[rSeat].kind === 'human' && rLogOk) {
        roomCode = makeCode();
        seat = rSeat;
        const room = newRoom(roomCode, rSize, rSize > 2);
        room.seats = R.seats.map((s) => ({
          kind: s.kind,
          name: typeof s.name === 'string' ? s.name.slice(0, 24) : '',
          difficulty: s.kind === 'bot' ? (['easy', 'medium', 'hard'].includes(s.difficulty) ? s.difficulty : 'medium') : null,
        }));
        room.players[rSeat] = ws;
        room.controllers[rSeat] = ws;
        room.seed = R.seed;
        room.gameId = (typeof R.gameId === 'string' && R.gameId) ? R.gameId.slice(0, 64) : ('cf-' + Date.now() + '-' + randomBytes(4).toString('hex'));
        room.decisions = R.decisions.map((d, i) => ({ t: 'decision', seq: i, hook: d.hook, playerIdx: d.playerIdx, answer: d.answer }));
        room.resumePending = true;
        room.creatorWs = ws;
        rooms.set(roomCode, room);
        ws.cfRoom = roomCode; ws.cfSeat = rSeat;
        if (room.multi) {
          send(ws, { t: 'created', room: roomCode, playerIdx: rSeat, size: rSize, seats: seatList(room) });
          broadcastLobby(room);
        } else {
          send(ws, { t: 'created', room: roomCode, playerIdx: rSeat });
        }
        console.log(`room ${roomCode} created as a RESUME (size ${rSize}, creator seat ${rSeat}, ${room.decisions.length} decisions preloaded)`);
        return;
      }
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
      if (room.resumePending) {
        // v4: joining a re-hosted saved game. Seats are identities,
        // not slots: the joiner takes their ORIGINAL seat (asked for
        // by number from their saved entry) or the lowest human seat
        // still empty. Bot / out seats are never joinable.
        let idx = -1;
        if (typeof m.seat === 'number' && m.seat >= 0 && m.seat < room.size
          && room.seats[m.seat].kind === 'human' && !room.players[m.seat]) idx = m.seat;
        if (idx < 0) idx = room.seats.findIndex((s, i) => s.kind === 'human' && !room.players[i]);
        if (idx < 0) return send(ws, { t: 'error', msg: 'room full' });
        roomCode = m.room;
        seat = idx;
        room.players[idx] = ws;
        room.controllers[idx] = ws;
        ws.cfRoom = roomCode; ws.cfSeat = idx;
        send(ws, { t: 'joined', room: roomCode, playerIdx: idx });
        if (!room.multi) {
          // 2-player resume rooms start on join, like classic rooms —
          // but with the SAVED seed + gameId and the preloaded log.
          room.started = true;
          room.resumePending = false;
          const seatsMsg = seatList(room);
          for (const pws of room.players) send(pws, { t: 'start', seed: room.seed, gameId: room.gameId, resumed: true, size: room.size, seats: seatsMsg });
          console.log(`room ${roomCode} resumed (seed ${room.seed}, ${room.decisions.length} decisions preloaded)`);
        } else {
          broadcastLobby(room);
          console.log(`room ${roomCode}: seat ${idx} joined the resume`);
        }
        return;
      }
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
        room.gameId = 'cf-' + Date.now() + '-' + randomBytes(4).toString('hex');
        room.started = true;
        for (const pws of room.players) send(pws, { t: 'start', seed: room.seed, gameId: room.gameId });
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
      if (room.started) return send(ws, { t: 'error', msg: 'already started' });
      if (room.resumePending) {
        // v4: starting a re-hosted saved game. The creator (any seat)
        // starts it once every human seat is refilled. NO compaction
        // and NO new seed: seat indexes and the saved seed + gameId
        // are the game's identity — the preloaded log refers to them.
        if (ws !== room.creatorWs) return send(ws, { t: 'error', msg: 'not host' });
        const missing = room.seats.some((s, i) => s.kind === 'human' && !room.players[i]);
        if (missing) return send(ws, { t: 'error', msg: 'waiting for players' });
        room.seats.forEach((s, i) => { if (s.kind === 'bot' || s.kind === 'out') room.controllers[i] = room.players[0]; });
        room.started = true;
        room.resumePending = false;
        const seatsMsg = seatList(room);
        room.players.forEach((pws, i) => {
          if (pws) send(pws, { t: 'start', seed: room.seed, size: room.size, seats: seatsMsg, playerIdx: i, gameId: room.gameId, resumed: true });
        });
        console.log(`room ${room.code} resumed multi (size ${room.size}, seed ${room.seed}, ${room.decisions.length} decisions preloaded)`);
        return;
      }
      if (seat !== 0) return send(ws, { t: 'error', msg: 'not host' });
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
      room.gameId = 'cf-' + Date.now() + '-' + randomBytes(4).toString('hex');
      room.started = true;
      const seatsMsg = seatList(room);
      room.players.forEach((pws, i) => {
        if (pws) send(pws, { t: 'start', seed: room.seed, size: room.size, seats: seatsMsg, playerIdx: i, gameId: room.gameId });
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
      if (occupant && occupant !== ws) {
        // MP-2 ghost-seat fix: a seat whose occupant socket is a zombie
        // — not OPEN, or it missed its most recent heartbeat ping — is
        // released to the rejoiner instead of refusing them with the
        // 'slot taken' their own corpse would have caused. 'slot taken'
        // remains only for a genuinely live occupant (OPEN and
        // pong-current). The occupant's close event, when it lands,
        // finds the seat already reoccupied and vacates nothing.
        const zombie = occupant.readyState !== 1 || (occupant.missedPongs || 0) > 0;
        if (!zombie) return send(ws, { t: 'error', msg: 'slot taken' });
        try { occupant.terminate(); } catch { try { occupant.close(); } catch { /* already gone */ } }
        console.log(`room ${m.room}: seat ${idx} occupant was a zombie — released for rejoin`);
      }
      roomCode = m.room;
      seat = idx;
      room.players[idx] = ws;
      clearPark(room, idx); // MP-1: a return inside the window cancels the away notices silently
      ws.cfRoom = roomCode; ws.cfSeat = idx;
      if (room.graceTimer) { clearTimeout(room.graceTimer); room.graceTimer = null; }
      const res = room.resolved.get(idx);
      if (!res) room.controllers[idx] = ws;         // plain return: they control their seat
      if (res && res.type === 'replace') room.controllers[idx] = ws; // RECLAIM: control returns at once
      // (a 'surrender' resolution leaves the controller with the host pilot)
      send(ws, {
        t: 'rejoined', playerIdx: idx, seed: room.seed,
        ...(room.gameId ? { gameId: room.gameId } : {}),
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

    if (m.t === 'championName') {
      // Additive pass-through (2026-10-21): at game start a player
      // names their champion; the other clients apply the name to
      // their seat maps. Forwarded to the rest of the room, stamped
      // with the sender's own seat (the seat being named) so the
      // attribution cannot be spoofed. No room state changes.
      const room = myRoom();
      if (!room || seat < 0) return;
      const fwd = { ...m, t: 'championName', seat, playerIdx: seat };
      if (typeof fwd.name === 'string') fwd.name = fwd.name.slice(0, 40);
      broadcastOthers(room, ws, fwd);
      return;
    }

    if (m.t === 'stateHash') {
      // Additive pass-through (MP-2, 2026-10-23): a client's phase-
      // boundary state hash. Peers compare it against their own hash
      // for the same phase/round to detect divergence; the relay just
      // forwards it, stamped with the sender's seat. No room state.
      const room = myRoom();
      if (!room || seat < 0) return;
      broadcastOthers(room, ws, {
        t: 'stateHash', seat, playerIdx: seat,
        phase: typeof m.phase === 'string' ? m.phase.slice(0, 40) : '',
        round: typeof m.round === 'number' ? m.round : null,
        hash: typeof m.hash === 'string' ? m.hash.slice(0, 16) : '',
      });
      return;
    }

    if (m.t === 'hashPlease') {
      // Additive pass-through (MP-2, 2026-10-23): a (re)joining client
      // asks the room to re-send its current phase hashes so it can
      // verify its rebuilt state. Forwarded to the rest of the room.
      const room = myRoom();
      if (!room || seat < 0) return;
      broadcastOthers(room, ws, { t: 'hashPlease', seat, playerIdx: seat });
      return;
    }

    if (m.t === 'surrender' || m.t === 'draw') {
      const room = myRoom();
      if (!room || seat < 0) return;
      broadcastOthers(room, ws, { t: m.t, playerIdx: seat });
      return;
    }

    if (m.t === 'saveQuit') {
      // v4: a player saved the game to resume later — a courtesy
      // signal so the other phones mirror the save. Never logged,
      // never state-bearing.
      const room = myRoom();
      if (!room || seat < 0 || !room.started) return;
      broadcastOthers(room, ws, { t: 'saveQuit', seat });
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
      // MP-3 layer-2 boundary: a press counts only once the seat has
      // been CONTINUOUSLY away past the park window — "actually left"
      // as a code fact, not a guess. Earlier presses are dropped
      // silently, exactly like any other invalid press; awaySince is
      // set at socket close and cleared only by a rejoin.
      if (typeof room.awaySince[s] !== 'number' || Date.now() - room.awaySince[s] < PARK_MS) return;
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
      // Started game: hold the room for the grace period (the grace
      // timer below starts now, at close, exactly as before). The away
      // notices themselves are park-debounced (MP-1): they reach the
      // remaining player only if the seat is still gone when the park
      // window lapses — a return inside the window is invisible.
      // (Additive Stage 3 notice: the same event as seatAway, which is
      // what raises the REPLACE / SURRENDER buttons in 2-player rooms.)
      {
        const awaySeat = seat;
        schedulePark(room, awaySeat, (r) => {
          const o = r.players[1 - awaySeat];
          send(o, { t: 'opponentAway' });
          send(o, { t: 'seatAway', seat: awaySeat });
        });
      }
      console.log(`room ${roomCode}: player ${seat} away (parked; grace started)`);
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
      if (seat === 0 || (room.resumePending && ws === room.creatorWs)) {
        // Host left the lobby: the room closes for everyone. (v4: in a
        // resume room the host is the CREATOR, at whatever seat.)
        broadcastAll(room, { t: 'hostLeft' });
        rooms.delete(roomCode);
        console.log(`room ${roomCode} closed (host left lobby)`);
      } else if (room.resumePending) {
        // v4: a resume seat is an identity in a saved game — leaving
        // the lobby frees the connection, never the seat itself.
        room.controllers[seat] = null;
        broadcastLobby(room);
        console.log(`room ${roomCode}: seat ${seat} left the resume lobby (seat kept)`);
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
      // seat's stall is affected meanwhile. The away notices are
      // park-debounced (MP-1) like every started-room seat; the grace
      // timer below still starts at close.
      schedulePark(room, 0, (r) => {
        broadcastAll(r, { t: 'seatAway', seat: 0 });
        broadcastAll(r, { t: 'opponentAway', seat: 0 });
      });
      console.log(`room ${roomCode}: host away (parked; grace started)`);
      if (!room.graceTimer) {
        room.graceTimer = setTimeout(() => {
          if (!rooms.has(roomCode)) return;
          const r = rooms.get(roomCode);
          broadcastAll(r, { t: 'hostLeft' });
          clearAllParks(r);
          rooms.delete(roomCode);
          console.log(`room ${roomCode} closed (hostLeft)`);
        }, GRACE_MS);
      }
      return;
    }
    // A non-host seat dropped: the seat STALLS. The table gets the
    // away notice (which raises the REPLACE / SURRENDER buttons) and
    // nothing else happens — no takeover, ever. MP-1 park debounce:
    // the notice itself now waits out the park window (the park timer
    // is the only new timer, and it only delays the notice — a rejoin
    // inside the window cancels it silently).
    {
      const awaySeat = seat;
      schedulePark(room, awaySeat, (r) => {
        broadcastAll(r, { t: 'seatAway', seat: awaySeat });
        broadcastAll(r, { t: 'opponentAway', seat: awaySeat });
      });
    }
    console.log(`room ${roomCode}: seat ${seat} away (parked — table decides after the window)`);
  });
});
