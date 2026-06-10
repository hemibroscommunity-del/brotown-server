// GameRoom WebSocket smoke test.
//
// Usage:  npx wrangler dev --port 8787 --local   (in another terminal)
//         node scripts/smoke-test.mjs
//
// Exercises the core wire protocol end-to-end: join/state_sync, tick
// deltas with player positions, monster zone spawns, player_state
// shape, duplicate-login eviction, reconnect, and persistence across
// disconnect (write-behind flush).  Requires Node >= 21 (global
// WebSocket).  Exits 0 on all-pass.
const PORT = process.env.PORT || 8787;
const ROOM = 'smoke_' + Date.now(); // fresh room per run; local DO storage persists across runs
const URL_WS = `ws://127.0.0.1:${PORT}/ws?room=${ROOM}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function connect(name) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(URL_WS);
    const client = { ws, name, msgs: [], closed: null };
    ws.onmessage = (ev) => {
      try {
        const m = JSON.parse(ev.data);
        client.msgs.push(m);
        if (m.type === 'ping') ws.send(JSON.stringify({ type: 'pong', ts: m.ts }));
      } catch {}
    };
    ws.onclose = (ev) => { client.closed = { code: ev.code, reason: ev.reason }; };
    ws.onopen = () => resolve(client);
    ws.onerror = () => reject(new Error('ws error for ' + name));
  });
}

function send(c, obj) { c.ws.send(JSON.stringify(obj)); }
function lastOfType(c, t) { for (let i = c.msgs.length - 1; i >= 0; i--) if (c.msgs[i].type === t) return c.msgs[i]; return null; }

let failures = 0;
function check(cond, label) {
  console.log((cond ? 'PASS' : 'FAIL') + ' — ' + label);
  if (!cond) failures++;
}

const a = await connect('alice');
const b = await connect('bob');
send(a, { type: 'join', id: 'p_alice', name: 'Alice', data: { z: 'town', rpgCoins: 123 } });
send(b, { type: 'join', id: 'p_bob', name: 'Bob', data: { z: 'town' } });
await sleep(500);

check(lastOfType(a, 'state_sync') !== null, 'alice got state_sync');
check(lastOfType(b, 'state_sync') !== null, 'bob got state_sync');
const syncB = lastOfType(b, 'state_sync');
check(syncB && syncB.players && syncB.players.p_alice, 'bob state_sync includes alice');
check(lastOfType(a, 'player_count') !== null, 'player_count broadcast received');
const psA = lastOfType(a, 'player_state');
check(psA && psA.payload && typeof psA.payload.coins === 'number' && 'inventory' in psA.payload && 'hp' in psA.payload, 'player_state shape (coins/inventory/hp)');
check(psA && psA.payload.coins === 123, 'bootstrap coins persisted (got ' + (psA && psA.payload.coins) + ')');

// Move into a monster zone and verify tick deltas carry positions.
send(a, { type: 'move', x: 100, y: 100, d: 'down', z: 'meadow', vx: 0, vy: 0 });
await sleep(300);
const tickWithPlayers = b.msgs.find((m) => m.type === 'tick' && m.players && m.players.p_alice);
check(!!tickWithPlayers, 'bob saw tick delta with alice position');
check(tickWithPlayers && tickWithPlayers.players.p_alice.x === 100, 'tick position value correct');
const tickWithMonsters = a.msgs.find((m) => m.type === 'tick' && m.monsters);
check(!!tickWithMonsters, 'tick delta included monster zone data (meadow spawn)');

// Duplicate login: joining the same player id on a NEW socket must
// evict the old socket, and the old socket's close must NOT wipe the
// live player's state or broadcast player_leave.
const a2 = await connect('alice2');
send(a2, { type: 'join', id: 'p_alice', name: 'Alice', data: { z: 'town' } });
await sleep(400);
check(a.closed !== null && a.closed.reason === 'duplicate login', 'old socket evicted on duplicate login (got ' + JSON.stringify(a.closed) + ')');
const leavesBefore = b.msgs.filter((m) => m.type === 'player_leave' && m.id === 'p_alice').length;
a.ws.close(); // no-op if already closed by server
await sleep(400);
const leavesAfter = b.msgs.filter((m) => m.type === 'player_leave' && m.id === 'p_alice').length;
check(leavesAfter === leavesBefore, 'no bogus player_leave when stale socket closes');
const before = a2.msgs.length;
send(a2, { type: 'move', x: 50, y: 50, d: 'down', z: 'town', vx: 0, vy: 0 });
await sleep(300);
const gotTick = a2.msgs.slice(before).some((m) => m.type === 'tick' && m.players && m.players.p_alice);
check(gotTick, 'after stale-socket close, live socket still receives its player ticks');

// Persistence across full disconnect: stored coins must survive and win
// over a spoofed join payload (write-behind flushed on close).
a2.ws.close();
await sleep(400);
const a3 = await connect('alice3');
send(a3, { type: 'join', id: 'p_alice', name: 'Alice', data: { z: 'town', rpgCoins: 99999 } });
await sleep(500);
const psA3 = lastOfType(a3, 'player_state');
check(psA3 && psA3.payload.coins === 123, 'stored coins win over spoofed join payload after reconnect (got ' + (psA3 && psA3.payload.coins) + ')');

b.ws.close();
a3.ws.close();
await sleep(200);
console.log(failures === 0 ? 'ALL PASS' : failures + ' FAILURES');
process.exit(failures === 0 ? 0 : 1);
