// GameRoom load test — N simulated players in one room, moving at a
// realistic client rate and attacking monsters, against a local
// wrangler dev server.
//
// Usage:  npx wrangler dev --port 8787 --local   (in another terminal)
//         node scripts/load-test.mjs
//         CLIENTS=50 DURATION=30 node scripts/load-test.mjs
//
// Reports per-client downstream bandwidth, message rates, and server
// tick cadence (the `ts` stamps in tick messages reveal whether the
// 22 ms tick loop is overrunning under load).  Requires Node >= 21.
const PORT = process.env.PORT || 8787;
const N = +(process.env.CLIENTS || 40);
const DURATION_MS = +(process.env.DURATION || 20) * 1000;
const ROOM = 'load_' + Date.now();
const URL_WS = `ws://127.0.0.1:${PORT}/ws?room=${ROOM}`;
const ZONES = ['meadow', 'ember', 'mist'];
const MOVE_INTERVAL_MS = 100; // ~10 Hz client input, matches a real player
const ATTACK_INTERVAL_MS = 700;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function makeClient(i) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(URL_WS);
    const zone = ZONES[i % ZONES.length];
    const c = {
      ws, id: 'load_p' + i, zone,
      x: 200 + Math.random() * 400, y: 200 + Math.random() * 400,
      bytesIn: 0, msgsIn: 0, ticks: 0, tickTs: [], lastSeq: null, seqGaps: 0,
      monsterIds: [], timers: [],
    };
    ws.onmessage = (ev) => {
      c.bytesIn += typeof ev.data === 'string' ? ev.data.length : ev.data.byteLength;
      c.msgsIn++;
      let m;
      try { m = JSON.parse(ev.data); } catch { return; }
      if (m.type === 'ping') { ws.send(JSON.stringify({ type: 'pong', ts: m.ts })); return; }
      if (m.type === 'tick') {
        c.ticks++;
        c.tickTs.push({ ts: m.ts, seq: m.seq });
        if (c.lastSeq !== null && m.seq > c.lastSeq + 1) c.seqGaps += m.seq - c.lastSeq - 1;
        c.lastSeq = m.seq;
        if (m.monsters && m.monsters[c.zone]) {
          c.monsterIds = m.monsters[c.zone].filter((x) => x.alive).map((x) => x.id);
        }
      } else if (m.type === 'zone_monsters' && m.zone === c.zone && Array.isArray(m.monsters)) {
        c.monsterIds = m.monsters.filter((x) => x.alive).map((x) => x.id);
      }
    };
    ws.onopen = () => {
      ws.send(JSON.stringify({ type: 'join', id: c.id, name: 'Load' + i, data: { z: 'town' } }));
      // Move into the assigned zone, then random-walk.
      c.timers.push(setInterval(() => {
        c.x = Math.max(140, Math.min(880, c.x + (Math.random() - 0.5) * 60));
        c.y = Math.max(140, Math.min(880, c.y + (Math.random() - 0.5) * 60));
        ws.send(JSON.stringify({ type: 'move', x: c.x, y: c.y, d: 'down', z: c.zone, vx: 0, vy: 0 }));
      }, MOVE_INTERVAL_MS));
      c.timers.push(setInterval(() => {
        if (c.monsterIds.length === 0) return;
        const target = c.monsterIds[(Math.random() * c.monsterIds.length) | 0];
        ws.send(JSON.stringify({ type: 'monster_damage', payload: { monsterId: target, zone: c.zone, dmg: 5 } }));
      }, ATTACK_INTERVAL_MS));
      resolve(c);
    };
    ws.onerror = () => reject(new Error('connect failed for client ' + i));
  });
}

function percentile(sorted, p) {
  if (sorted.length === 0) return 0;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

console.log(`Connecting ${N} clients to ${URL_WS} for ${DURATION_MS / 1000}s ...`);
const clients = [];
for (let i = 0; i < N; i++) {
  clients.push(await makeClient(i));
  await sleep(25); // stagger joins
}
console.log('All connected. Running ...');
const t0 = Date.now();
await sleep(DURATION_MS);
const elapsed = (Date.now() - t0) / 1000;
for (const c of clients) { c.timers.forEach(clearInterval); try { c.ws.close(); } catch {} }

// Aggregate
const totalBytes = clients.reduce((a, c) => a + c.bytesIn, 0);
const totalMsgs = clients.reduce((a, c) => a + c.msgsIn, 0);
const totalGaps = clients.reduce((a, c) => a + c.seqGaps, 0);
// Tick cadence from one observer's ts stamps (all clients see the same
// broadcast).  Only consecutive-seq pairs measure loop health — a seq
// jump means the server intentionally sent nothing that tick (nothing
// dirty, or monster data deferred by MONSTER_BROADCAST_DIVISOR), which
// would otherwise read as a fake 2x interval.
const ts = clients[0].tickTs;
const deltas = [];
for (let i = 1; i < ts.length; i++) {
  if (ts[i].seq === ts[i - 1].seq + 1) deltas.push(ts[i].ts - ts[i - 1].ts);
}
deltas.sort((a, b) => a - b);

console.log('--- Results ---');
console.log(`clients:                 ${N} across zones [${ZONES.join(', ')}]`);
console.log(`duration:                ${elapsed.toFixed(1)}s`);
console.log(`downstream total:        ${(totalBytes / 1024).toFixed(0)} KiB  (${(totalBytes / elapsed / 1024).toFixed(1)} KiB/s room-wide)`);
console.log(`downstream per client:   ${(totalBytes / N / elapsed / 1024).toFixed(2)} KiB/s  (${(totalMsgs / N / elapsed).toFixed(1)} msg/s)`);
console.log(`ticks observed (c0):     ${clients[0].ticks}  quiet/deferred ticks room-wide: ${totalGaps}`);
console.log(`tick interval ms:        p50=${percentile(deltas, 50)}  p95=${percentile(deltas, 95)}  p99=${percentile(deltas, 99)}  max=${deltas[deltas.length - 1] ?? 0}  (consecutive-seq pairs: ${deltas.length})`);
console.log(`(target cadence is 22 ms; sustained p95 >> 22 means the tick loop is overrunning)`);
process.exit(0);
