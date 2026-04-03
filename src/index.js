/**
 * Hemi Bros ARPG — Cloudflare Durable Objects Multiplayer Server
 * 
 * Architecture:
 * - Worker routes /ws?room=ROOM_NAME to GameRoom DO per room
 * - Marketplace DO handles global cross-room buy/sell order book
 * - Leaderboard DO persists player rankings across sessions
 * - 30Hz server tick batches position broadcasts
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const corsHeaders = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: { ...corsHeaders, 'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS', 'Access-Control-Allow-Headers': '*' } });
    }

    if (url.pathname === '/ws') {
      const room = url.searchParams.get('room') || 'brotown';
      return env.GAME_ROOM.get(env.GAME_ROOM.idFromName(room)).fetch(request);
    }

    if (url.pathname.startsWith('/api/market')) {
      return env.MARKETPLACE.get(env.MARKETPLACE.idFromName('global')).fetch(request);
    }

    if (url.pathname.startsWith('/api/leaderboard')) {
      return env.LEADERBOARD.get(env.LEADERBOARD.idFromName('global')).fetch(request);
    }

    if (url.pathname.startsWith('/api/arena')) {
      return env.ARENA.get(env.ARENA.idFromName('global')).fetch(request);
    }

    if (url.pathname.startsWith('/api/feedback')) {
      return env.FEEDBACK.get(env.FEEDBACK.idFromName('global')).fetch(request);
    }

    if (url.pathname === '/health') {
      return new Response(JSON.stringify({ status: 'ok', ts: Date.now() }), { headers: corsHeaders });
    }

    return new Response('Hemi Bros Game Server', { status: 200 });
  },
};


// ═══════════════════════════════════════
//  GAME ROOM — One per room, handles WebSocket multiplayer
// ═══════════════════════════════════════

export class GameRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Map();
    this.playerState = {};
    this.dirtyPlayers = new Set();
    this.eventBuffer = []; // §16.10 — batched events sent with tick
    this.tickInterval = null;
    this.tickSeq = 0;
    this.TICK_RATE = 50; // §16.13 — 20Hz (50ms)
    this.MAX_PLAYERS = 50;
    this.EVENTS_PER_TICK_CAP = 500; // §16.10

    // §16.12 — PvP Lag Compensation
    this.stateHistory = {}; // playerId -> [StateSnapshot, ...] (ring buffer)
    this.LAGCOMP_BUFFER_TICKS = 6; // 300ms of history at 20Hz
    this.LAGCOMP_RTT_CAP = 300; // ms
    this.LAGCOMP_RTT_ALPHA = 0.3; // EMA smoothing
  }

  async fetch(request) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected WebSocket', { status: 426 });
    }
    if (this.sessions.size >= this.MAX_PLAYERS) {
      return new Response('Room full', { status: 503 });
    }
    const [client, server] = Object.values(new WebSocketPair());
    this.state.acceptWebSocket(server);
    this.sessions.set(server, { id: null, name: 'Anon', data: {}, rtt: 80, lastPing: 0 });
    if (!this.tickInterval && this.sessions.size === 1) this.startTickLoop();
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    const session = this.sessions.get(ws);
    if (!session) return;
    let msg;
    try { msg = JSON.parse(message); } catch { return; }

    switch (msg.type) {
      case 'join':
        session.id = msg.id;
        session.name = msg.name || 'Anon';
        session.data = msg.data || {};
        this.playerState[msg.id] = {
          x: 0, y: 0, d: 'down', z: 'town', vx: 0, vy: 0,
          dodging: false, blocking: false, dead: false, disconnected: false,
          ...msg.data
        };
        this.stateHistory[msg.id] = [];
        this.broadcastExcept(ws, { type: 'player_join', id: msg.id, name: msg.name, data: msg.data });
        ws.send(JSON.stringify({ type: 'state_sync', players: this.getAllPlayerData(), playerCount: this.getPlayerCount() }));
        this.broadcastAll({ type: 'player_count', count: this.getPlayerCount() });
        this.reportToLeaderboard(session);
        break;

      case 'move':
        if (session.id && this.playerState[session.id]) {
          const ps = this.playerState[session.id];
          ps.x = msg.x; ps.y = msg.y; ps.d = msg.d || ps.d; ps.z = msg.z || ps.z;
          ps.vx = msg.vx || 0; ps.vy = msg.vy || 0;
          // §16.12 — Combat state flags from client
          if (msg.dodging !== undefined) ps.dodging = !!msg.dodging;
          if (msg.blocking !== undefined) ps.blocking = !!msg.blocking;
          if (msg.dead !== undefined) ps.dead = !!msg.dead;
          this.dirtyPlayers.add(session.id);
        }
        break;

      case 'pong':
        // §16.12 — RTT estimation from ping/pong
        if (session.lastPing > 0) {
          const sample = Date.now() - session.lastPing;
          session.rtt = session.rtt * (1 - this.LAGCOMP_RTT_ALPHA) + sample * this.LAGCOMP_RTT_ALPHA;
          session.rtt = Math.min(session.rtt, this.LAGCOMP_RTT_CAP);
        }
        break;

      case 'track':
        if (session.id) {
          session.data = { ...session.data, ...msg.data };
          if (this.playerState[session.id]) Object.assign(this.playerState[session.id], msg.data);
          this.broadcastExcept(ws, { type: 'player_update', id: session.id, data: msg.data });
          this.reportToLeaderboard(session);
        }
        break;

      case 'player_attack':
        // §16.12 — Server-side PvP hit detection with lag compensation
        if (session.id) {
          this._resolvePvPAttack(session, msg.payload || msg);
        }
        break;

      default:
        // §16.10 — Buffer game events for next tick broadcast
        if (session.id) {
          msg.from = session.id;
          this.eventBuffer.push(msg);
        }
        break;
    }
  }

  // §16.12 — Attacker-favored rollback PvP resolution
  _resolvePvPAttack(attackerSession, payload) {
    const attackerId = attackerSession.id;
    const attackerPs = this.playerState[attackerId];
    if (!attackerPs) return;

    // Calculate rewind depth from attacker's RTT
    const halfRtt = attackerSession.rtt / 2;
    const rewindTicks = Math.min(Math.ceil(halfRtt / this.TICK_RATE), this.LAGCOMP_BUFFER_TICKS);

    const range = payload.range || 40;
    const arc = payload.arc || 1.2;
    const angle = payload.angle || 0;
    const dmgBase = payload.dmgBase || 10;
    const critChance = payload.critChance || 0;

    // Check all players in room for hits
    for (const [targetId, targetPs] of Object.entries(this.playerState)) {
      if (targetId === attackerId) continue;
      if (targetPs.z !== attackerPs.z) continue; // different zone
      if (targetPs.dead || targetPs.disconnected) continue;

      // §16.12 — Look up target's historical state
      const history = this.stateHistory[targetId];
      let checkState = targetPs; // fallback: current state
      if (history && history.length > 0) {
        const idx = Math.max(0, history.length - 1 - rewindTicks);
        checkState = history[idx] || targetPs;
      }

      // Range check against historical position
      const dx = checkState.x - attackerPs.x;
      const dy = checkState.y - attackerPs.y;
      const dist = Math.sqrt(dx * dx + dy * dy);
      if (dist > range) continue;

      // Arc check
      const targetAngle = Math.atan2(dy, dx);
      let angleDiff = targetAngle - angle;
      while (angleDiff > Math.PI) angleDiff -= Math.PI * 2;
      while (angleDiff < -Math.PI) angleDiff += Math.PI * 2;
      if (Math.abs(angleDiff) > arc / 2) continue;

      // §16.12 — Resolve dodge/block against historical state
      if (checkState.dodging) continue; // was in i-frames from attacker's perspective

      let blocked = false;
      if (checkState.blocking) blocked = true;

      // Crit roll
      const isCrit = Math.random() * 100 < critChance;

      // Build hit event — defender's client will apply their own defense calc
      // but the HIT/MISS decision is server-authoritative
      const hitEvent = {
        type: 'pvp_hit',
        payload: {
          attacker: attackerId,
          attackerName: attackerSession.name,
          target: targetId,
          dmgBase: dmgBase,
          isCrit: isCrit,
          blocked: blocked,
          ts: Date.now(),
          rewindTicks: rewindTicks,
        }
      };
      this.eventBuffer.push(hitEvent);
    }
  }

  async webSocketClose(ws) {
    const session = this.sessions.get(ws);
    if (session?.id) {
      if (this.playerState[session.id]) this.playerState[session.id].disconnected = true;
      delete this.playerState[session.id];
      delete this.stateHistory[session.id];
      this.dirtyPlayers.delete(session.id);
      this.broadcastAll({ type: 'player_leave', id: session.id });
      this.broadcastAll({ type: 'player_count', count: this.getPlayerCount() - 1 });
    }
    this.sessions.delete(ws);
    if (this.sessions.size === 0 && this.tickInterval) { clearInterval(this.tickInterval); this.tickInterval = null; }
  }

  async webSocketError(ws) { this.webSocketClose(ws); }

  startTickLoop() {
    // §16.12 — Ping counter for RTT measurement
    let pingCounter = 0;

    this.tickInterval = setInterval(() => {
      // §16.12 — Step 1: Snapshot player states to history buffer
      for (const [id, ps] of Object.entries(this.playerState)) {
        if (!this.stateHistory[id]) this.stateHistory[id] = [];
        this.stateHistory[id].push({
          x: ps.x, y: ps.y, d: ps.d, z: ps.z,
          dodging: ps.dodging || false,
          blocking: ps.blocking || false,
          dead: ps.dead || false,
          tick: this.tickSeq,
        });
        // Ring buffer — keep only LAGCOMP_BUFFER_TICKS entries
        if (this.stateHistory[id].length > this.LAGCOMP_BUFFER_TICKS) {
          this.stateHistory[id].shift();
        }
      }

      // §16.12 — Periodic ping for RTT estimation (every 5s = 100 ticks)
      pingCounter++;
      if (pingCounter >= 100) {
        pingCounter = 0;
        const pingMsg = JSON.stringify({ type: 'ping', ts: Date.now() });
        for (const [ws, session] of this.sessions) {
          session.lastPing = Date.now();
          try { ws.send(pingMsg); } catch {}
        }
      }

      const hasDirty = this.dirtyPlayers.size > 0;
      const hasEvents = this.eventBuffer.length > 0;
      if (!hasDirty && !hasEvents) { this.tickSeq++; return; }

      // §16.8 — Build single room-wide tick delta
      const delta = { type: 'tick', seq: this.tickSeq++, ts: Date.now() };

      // §16.9 — Batched player positions (only dirty)
      if (hasDirty) {
        const players = {};
        for (const id of this.dirtyPlayers) {
          const ps = this.playerState[id];
          if (ps) players[id] = { x: ps.x, y: ps.y, d: ps.d, z: ps.z, vx: ps.vx, vy: ps.vy };
        }
        delta.players = players;
        this.dirtyPlayers.clear();
      }

      // §16.10 — Batched game events (capped)
      if (hasEvents) {
        delta.events = this.eventBuffer.length <= this.EVENTS_PER_TICK_CAP
          ? this.eventBuffer
          : this.eventBuffer.slice(0, this.EVENTS_PER_TICK_CAP);
        this.eventBuffer = [];
      }

      // §16.8 — Single broadcast to all clients
      const msg = JSON.stringify(delta);
      for (const [ws] of this.sessions) { try { ws.send(msg); } catch {} }
    }, this.TICK_RATE);
  }

  async reportToLeaderboard(session) {
    try {
      const stub = this.env.LEADERBOARD.get(this.env.LEADERBOARD.idFromName('global'));
      await stub.fetch(new Request('https://internal/api/leaderboard/update', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          playerId: session.id, name: session.name || session.data?.name || 'Anon',
          color: session.data?.color || '#5b52ff', level: session.data?.rpgLv || 1,
          rpgData: session.data?.rpgData || {}, ts: Date.now(),
        }),
      }));
    } catch {}
  }

  broadcastAll(msg) { const s = JSON.stringify(msg); for (const [ws] of this.sessions) { try { ws.send(s); } catch {} } }
  broadcastExcept(ex, msg) { const s = JSON.stringify(msg); for (const [ws] of this.sessions) { if (ws !== ex) { try { ws.send(s); } catch {} } } }
  getAllPlayerData() { const r = {}; for (const [, s] of this.sessions) { if (s.id) r[s.id] = { ...this.playerState[s.id], name: s.name, ...s.data }; } return r; }
  getPlayerCount() { let c = 0; for (const [, s] of this.sessions) { if (s.id) c++; } return c; }
}


// ═══════════════════════════════════════
//  MARKETPLACE — Global persistent order book (§39.4 indexed)
//  Composite index: category:subtype:tierKey:element1:element2
//  Buy orders sorted descending by price, sell orders ascending.
//  Matching is O(1) against bucket head.
// ═══════════════════════════════════════

export class Marketplace {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.index = null; // In-memory index, lazy-loaded
    this.playerOrderCounts = null; // playerId -> count
    this.SWEEP_INTERVAL = 60000;
    this.ORDER_EXPIRY = 3600000;
    this.MAX_ORDERS_PER_PLAYER = 10;
  }

  // §39.4 — Composite index key
  _indexKey(o) {
    return `${o.category}:${o.subtype}:${o.tierKey}:${o.element1 || 'none'}:${o.element2 || 'none'}`;
  }

  // Load full index from storage into memory (once per DO wake)
  async _ensureIndex() {
    if (this.index) return;
    this.index = new Map();
    this.playerOrderCounts = new Map();
    const now = Date.now();
    const entries = await this.state.storage.list({ prefix: 'order:' });
    const expired = [];
    for (const [key, raw] of entries) {
      let o;
      try { o = JSON.parse(raw); } catch { expired.push(key); continue; }
      if (o.expires <= now) { expired.push(key); continue; }
      this._addToIndex(o);
    }
    if (expired.length) await this.state.storage.delete(expired);
  }

  _addToIndex(o) {
    const key = this._indexKey(o);
    if (!this.index.has(key)) this.index.set(key, { buys: [], sells: [] });
    const bucket = this.index.get(key);
    if (o.type === 'buy') {
      bucket.buys.push(o);
      bucket.buys.sort((a, b) => b.price - a.price); // highest bid first
    } else {
      bucket.sells.push(o);
      bucket.sells.sort((a, b) => a.price - b.price); // lowest ask first
    }
    this.playerOrderCounts.set(o.playerId, (this.playerOrderCounts.get(o.playerId) || 0) + 1);
  }

  _removeFromIndex(o) {
    const key = this._indexKey(o);
    const bucket = this.index.get(key);
    if (!bucket) return;
    if (o.type === 'buy') {
      bucket.buys = bucket.buys.filter(x => x.id !== o.id);
    } else {
      bucket.sells = bucket.sells.filter(x => x.id !== o.id);
    }
    if (bucket.buys.length === 0 && bucket.sells.length === 0) this.index.delete(key);
    const count = (this.playerOrderCounts.get(o.playerId) || 1) - 1;
    if (count <= 0) this.playerOrderCounts.delete(o.playerId);
    else this.playerOrderCounts.set(o.playerId, count);
  }

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace('/api/market', '');
    const H = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' };

    try {
      await this._ensureIndex();
      await this._lazySweep();

      // GET /orders?category=weapon&subtype=greatsword&tier=iron
      if (request.method === 'GET' && path.startsWith('/orders')) {
        const category = url.searchParams.get('category');
        const subtype = url.searchParams.get('subtype');
        const tier = url.searchParams.get('tier');
        const orders = this._queryOrders(category, subtype, tier, null, 100);
        return new Response(JSON.stringify({ ok: true, orders }), { headers: H });
      }

      // POST /place — place buy or sell order
      if (request.method === 'POST' && path.startsWith('/place')) {
        const body = await request.json();
        const result = await this.placeOrder(body);
        return new Response(JSON.stringify(result), { headers: H });
      }

      // DELETE /cancel?id=X&playerId=Y
      if (request.method === 'DELETE' && path.startsWith('/cancel')) {
        const orderId = url.searchParams.get('id');
        const playerId = url.searchParams.get('playerId');
        const result = await this.cancelOrder(orderId, playerId);
        return new Response(JSON.stringify(result), { headers: H });
      }

      // GET /my?playerId=X
      if (request.method === 'GET' && path.startsWith('/my')) {
        const playerId = url.searchParams.get('playerId');
        const orders = this._queryOrders(null, null, null, playerId, 100);
        return new Response(JSON.stringify({ ok: true, orders }), { headers: H });
      }

      return new Response(JSON.stringify({ ok: false, error: 'Not found' }), { status: 404, headers: H });
    } catch (err) {
      return new Response(JSON.stringify({ ok: false, error: err.message }), { status: 500, headers: H });
    }
  }

  // §39.4 — Query using index. If category+subtype+tier all specified, direct bucket lookup.
  // Otherwise scan relevant buckets with filtering.
  _queryOrders(category, subtype, tier, playerId, limit) {
    const results = [];
    for (const [, bucket] of this.index) {
      const all = [...bucket.buys, ...bucket.sells];
      for (const o of all) {
        if (category && o.category !== category) continue;
        if (subtype && o.subtype !== subtype) continue;
        if (tier && o.tierKey !== tier) continue;
        if (playerId && o.playerId !== playerId) continue;
        results.push(o);
        if (results.length >= limit) return results;
      }
    }
    return results;
  }

  async placeOrder(body) {
    const { type, category, subtype, tierKey, element1, element2, price, item, tierLabel, playerName, playerId } = body;
    if (!type || !category || !subtype || !tierKey || !price || !playerId) return { ok: false, error: 'Missing fields' };
    if (price < 1 || price > 999999) return { ok: false, error: 'Invalid price' };
    if (type !== 'buy' && type !== 'sell') return { ok: false, error: 'Invalid type' };
    if (type === 'sell' && !item) return { ok: false, error: 'Sell needs item' };

    // Rate limit — O(1) lookup from in-memory count
    const currentCount = this.playerOrderCounts.get(playerId) || 0;
    if (currentCount >= this.MAX_ORDERS_PER_PLAYER) return { ok: false, error: 'Max 10 orders' };

    const order = {
      id: crypto.randomUUID(), type, category, subtype, tierKey,
      element1: element1 || null, element2: element2 || null,
      price: Math.floor(price), item: type === 'sell' ? item : null,
      tierLabel: tierLabel || tierKey, playerName: playerName || 'Unknown', playerId,
      ts: Date.now(), expires: Date.now() + this.ORDER_EXPIRY,
    };

    // §39.4 — O(1) match against bucket head
    const key = this._indexKey(order);
    const bucket = this.index.get(key);
    let best = null;

    if (bucket) {
      const oppList = type === 'buy' ? bucket.sells : bucket.buys;
      // Check head of opposite sorted array
      for (let i = 0; i < oppList.length; i++) {
        const o = oppList[i];
        if (o.playerId === playerId) continue; // can't self-trade
        if (type === 'buy' && o.price <= price) { best = o; break; }
        if (type === 'sell' && o.price >= price) { best = o; break; }
      }
    }

    if (best) {
      // Match found — execute trade
      this._removeFromIndex(best);
      await this.state.storage.delete('order:' + best.id);
      return { ok: true, matched: true, execPrice: best.price, matchedOrder: best, newOrder: order };
    }

    // No match — add to book
    this._addToIndex(order);
    await this.state.storage.put('order:' + order.id, JSON.stringify(order));
    return { ok: true, matched: false, order };
  }

  async cancelOrder(orderId, playerId) {
    if (!orderId || !playerId) return { ok: false, error: 'Missing params' };
    const raw = await this.state.storage.get('order:' + orderId);
    if (!raw) return { ok: false, error: 'Not found' };
    const order = JSON.parse(raw);
    if (order.playerId !== playerId) return { ok: false, error: 'Not yours' };
    this._removeFromIndex(order);
    await this.state.storage.delete('order:' + orderId);
    return { ok: true, cancelled: order };
  }

  // §39.4 — Lazy expiry sweep (once per minute)
  async _lazySweep() {
    const lp = await this.state.storage.get('_lastPurge') || 0;
    if (Date.now() - lp < this.SWEEP_INTERVAL) return;
    const now = Date.now();
    const toDelete = [];
    for (const [, bucket] of this.index) {
      for (const o of [...bucket.buys, ...bucket.sells]) {
        if (o.expires <= now) toDelete.push(o);
      }
    }
    for (const o of toDelete) {
      this._removeFromIndex(o);
      await this.state.storage.delete('order:' + o.id);
    }
    await this.state.storage.put('_lastPurge', Date.now());
  }
}


// ═══════════════════════════════════════
//  LEADERBOARD — Global persistent rankings
// ═══════════════════════════════════════

export class Leaderboard {
  constructor(state, env) { this.state = state; this.env = env; }

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace('/api/leaderboard', '');
    const H = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' };

    try {
      if (request.method === 'POST' && path.startsWith('/update')) {
        const body = await request.json();
        await this.updatePlayer(body);
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }

      if (request.method === 'GET' && path.startsWith('/top')) {
        const category = url.searchParams.get('category') || 'level';
        const limit = Math.min(100, parseInt(url.searchParams.get('limit')) || 50);
        const results = await this.getTop(category, limit);
        return new Response(JSON.stringify({ ok: true, category, results }), { headers: H });
      }

      return new Response(JSON.stringify({ ok: false, error: 'Not found' }), { status: 404, headers: H });
    } catch (err) {
      return new Response(JSON.stringify({ ok: false, error: err.message }), { status: 500, headers: H });
    }
  }

  async updatePlayer(data) {
    const { playerId, name, color, level, rpgData, ts } = data;
    if (!playerId) return;
    await this.state.storage.put('player:' + playerId, JSON.stringify({
      id: playerId, name: name || 'Anon', color: color || '#5b52ff', level: level || 1,
      lifeTotal: rpgData?.lifeTotal || 0, ap: rpgData?.ap || 0, kills: rpgData?.kills || 0,
      dungeons: rpgData?.dungeons || 0, goldEarned: rpgData?.goldEarned || 0, playtime: rpgData?.playtime || 0,
      clanTag: rpgData?.clanTag || null, lastSeen: ts || Date.now(),
    }));
  }

  async getTop(category, limit) {
    const entries = await this.state.storage.list({ prefix: 'player:' });
    const players = []; const now = Date.now(); const STALE = 7 * 86400000;
    for (const [, raw] of entries) { try { const p = JSON.parse(raw); if (now - (p.lastSeen || 0) < STALE) players.push(p); } catch {} }
    const key = { level:'level', lifeskills:'lifeTotal', ap:'ap', kills:'kills', dungeons:'dungeons', gold:'goldEarned', playtime:'playtime' }[category] || 'level';
    players.sort((a, b) => (b[key] || 0) - (a[key] || 0));
    return players.slice(0, limit);
  }
}


// ═══════════════════════════════════════
//  ARENA — Cross-room gladiator tournament
//  10 rounds, single elimination, blind matchup
// ═══════════════════════════════════════

export class Arena {
  constructor(state, env) { this.state = state; this.env = env; }

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace('/api/arena', '');
    const H = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' };

    try {
      // POST /join — enter the queue (costs gold, validated client-side)
      if (request.method === 'POST' && path.startsWith('/join')) {
        const body = await request.json();
        return new Response(JSON.stringify(await this.joinQueue(body)), { headers: H });
      }

      // POST /leave — leave the queue
      if (request.method === 'POST' && path.startsWith('/leave')) {
        const body = await request.json();
        return new Response(JSON.stringify(await this.leaveQueue(body.playerId)), { headers: H });
      }

      // GET /status?playerId=X — check queue/match status
      if (request.method === 'GET' && path.startsWith('/status')) {
        const pid = url.searchParams.get('playerId');
        return new Response(JSON.stringify(await this.getStatus(pid)), { headers: H });
      }

      // POST /result — report a match result (winner reports)
      if (request.method === 'POST' && path.startsWith('/result')) {
        const body = await request.json();
        return new Response(JSON.stringify(await this.reportResult(body)), { headers: H });
      }

      // GET /tournament — get current tournament state (for spectators)
      if (request.method === 'GET' && path.startsWith('/tournament')) {
        return new Response(JSON.stringify(await this.getTournament()), { headers: H });
      }

      // GET /history — past gladiator winners
      if (request.method === 'GET' && path.startsWith('/history')) {
        return new Response(JSON.stringify(await this.getHistory()), { headers: H });
      }

      return new Response(JSON.stringify({ ok: false, error: 'Not found' }), { status: 404, headers: H });
    } catch (err) {
      return new Response(JSON.stringify({ ok: false, error: err.message }), { status: 500, headers: H });
    }
  }

  async joinQueue(data) {
    const { playerId, name, level, color } = data;
    if (!playerId || !name) return { ok: false, error: 'Missing fields' };

    // Check if already in queue or active tournament
    const queue = await this.getQueue();
    if (queue.find(p => p.id === playerId)) return { ok: false, error: 'Already in queue' };

    const tournament = await this.getActiveTournament();
    if (tournament) {
      const inTournament = tournament.players.find(p => p.id === playerId);
      if (inTournament && !inTournament.eliminated) return { ok: false, error: 'Already in tournament' };
    }

    const entry = { id: playerId, name, level: level || 1, color: color || '#5b52ff', joinedAt: Date.now() };
    queue.push(entry);
    await this.state.storage.put('queue', JSON.stringify(queue));

    // Check if we have enough players to start (minimum 8, max 16, or start after 2min with 4+)
    const TOURNAMENT_MIN = 4;
    const TOURNAMENT_IDEAL = 16;
    const QUEUE_TIMEOUT = 120000; // 2 min

    const oldestEntry = queue.reduce((min, p) => Math.min(min, p.joinedAt), Infinity);
    const queueAge = Date.now() - oldestEntry;

    if (queue.length >= TOURNAMENT_IDEAL || (queue.length >= TOURNAMENT_MIN && queueAge >= QUEUE_TIMEOUT)) {
      // Start tournament!
      const players = queue.splice(0, TOURNAMENT_IDEAL).map(p => ({ ...p, eliminated: false, wins: 0, round: 0 }));
      await this.state.storage.put('queue', JSON.stringify(queue));

      const tournament = {
        id: 'arena-' + Date.now(),
        players,
        round: 1,
        maxRounds: 10,
        matches: [],       // {round, p1id, p2id, winnerId, ts}
        currentMatches: [], // active matches this round
        startTime: Date.now(),
        status: 'active',   // 'active' | 'complete'
        champion: null,
        spectators: [],
      };

      // Generate round 1 matchups
      tournament.currentMatches = this.generateMatchups(tournament);
      await this.state.storage.put('tournament', JSON.stringify(tournament));

      return { ok: true, started: true, tournament: this.sanitizeTournament(tournament), position: null };
    }

    return { ok: true, started: false, queuePosition: queue.length, queueSize: queue.length };
  }

  async leaveQueue(playerId) {
    if (!playerId) return { ok: false, error: 'Missing playerId' };
    let queue = await this.getQueue();
    const before = queue.length;
    queue = queue.filter(p => p.id !== playerId);
    await this.state.storage.put('queue', JSON.stringify(queue));
    return { ok: true, removed: queue.length < before };
  }

  async getStatus(playerId) {
    if (!playerId) return { ok: false, error: 'Missing playerId' };

    // Check queue
    const queue = await this.getQueue();
    const inQueue = queue.findIndex(p => p.id === playerId);
    if (inQueue >= 0) {
      return { ok: true, status: 'queued', position: inQueue + 1, queueSize: queue.length };
    }

    // Check active tournament
    const tournament = await this.getActiveTournament();
    if (tournament) {
      const player = tournament.players.find(p => p.id === playerId);
      if (player) {
        const myMatch = tournament.currentMatches.find(m => m.p1 === playerId || m.p2 === playerId);
        return {
          ok: true,
          status: player.eliminated ? 'eliminated' : (myMatch ? 'fighting' : 'waiting'),
          tournament: this.sanitizeTournament(tournament),
          currentMatch: myMatch || null,
          round: tournament.round,
          wins: player.wins,
          eliminated: player.eliminated,
        };
      }
    }

    return { ok: true, status: 'none' };
  }

  async reportResult(data) {
    const { tournamentId, matchId, winnerId, loserId } = data;
    if (!tournamentId || !matchId || !winnerId || !loserId) return { ok: false, error: 'Missing fields' };

    const tournament = await this.getActiveTournament();
    if (!tournament || tournament.id !== tournamentId) return { ok: false, error: 'Tournament not found' };

    // Find and resolve the match
    const matchIdx = tournament.currentMatches.findIndex(m => m.id === matchId);
    if (matchIdx < 0) return { ok: false, error: 'Match not found' };

    const match = tournament.currentMatches[matchIdx];
    if (match.resolved) return { ok: false, error: 'Already resolved' };

    match.resolved = true;
    match.winnerId = winnerId;
    match.loserId = loserId;
    match.resolvedAt = Date.now();

    // Update player states
    const winner = tournament.players.find(p => p.id === winnerId);
    const loser = tournament.players.find(p => p.id === loserId);
    if (winner) winner.wins++;
    if (loser) loser.eliminated = true;

    // Record in match history
    tournament.matches.push({ round: tournament.round, p1: match.p1, p2: match.p2, winnerId, loserId, ts: Date.now() });

    // Check if all matches this round are resolved
    const allResolved = tournament.currentMatches.every(m => m.resolved);
    if (allResolved) {
      const remaining = tournament.players.filter(p => !p.eliminated);

      if (remaining.length <= 1 || tournament.round >= tournament.maxRounds) {
        // Tournament complete!
        tournament.status = 'complete';
        tournament.champion = remaining[0] || null;
        tournament.endTime = Date.now();

        // Record in hall of fame
        if (tournament.champion) {
          const history = await this.getHistoryData();
          history.push({
            championId: tournament.champion.id,
            championName: tournament.champion.name,
            championLevel: tournament.champion.level,
            wins: tournament.champion.wins,
            totalPlayers: tournament.players.length,
            rounds: tournament.round,
            ts: Date.now(),
          });
          // Keep last 50 champions
          if (history.length > 50) history.splice(0, history.length - 50);
          await this.state.storage.put('history', JSON.stringify(history));
        }
      } else {
        // Advance to next round
        tournament.round++;
        tournament.currentMatches = this.generateMatchups(tournament);
      }
    }

    await this.state.storage.put('tournament', JSON.stringify(tournament));

    return {
      ok: true,
      tournament: this.sanitizeTournament(tournament),
      roundComplete: allResolved,
      tournamentComplete: tournament.status === 'complete',
      champion: tournament.champion,
    };
  }

  async getTournament() {
    const tournament = await this.getActiveTournament();
    const queue = await this.getQueue();
    return { ok: true, tournament: tournament ? this.sanitizeTournament(tournament) : null, queueSize: queue.length };
  }

  async getHistory() {
    const history = await this.getHistoryData();
    return { ok: true, champions: history.slice(-20).reverse() };
  }

  // ── Helpers ──

  generateMatchups(tournament) {
    const active = tournament.players.filter(p => !p.eliminated);
    // Shuffle for blind matchup
    for (let i = active.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [active[i], active[j]] = [active[j], active[i]];
    }
    const matches = [];
    for (let i = 0; i < active.length - 1; i += 2) {
      matches.push({
        id: 'match-' + tournament.round + '-' + (i / 2) + '-' + Date.now(),
        round: tournament.round,
        p1: active[i].id,
        p1Name: active[i].name,
        p1Level: active[i].level,
        p1Color: active[i].color,
        p2: active[i + 1].id,
        p2Name: active[i + 1].name,
        p2Level: active[i + 1].level,
        p2Color: active[i + 1].color,
        resolved: false,
        winnerId: null,
        loserId: null,
      });
    }
    // If odd player count, last player gets a bye (auto-win)
    if (active.length % 2 === 1) {
      const bye = active[active.length - 1];
      bye.wins++;
      tournament.matches.push({ round: tournament.round, p1: bye.id, p2: 'BYE', winnerId: bye.id, loserId: null, ts: Date.now() });
    }
    return matches;
  }

  sanitizeTournament(t) {
    return {
      id: t.id,
      round: t.round,
      maxRounds: t.maxRounds,
      status: t.status,
      champion: t.champion,
      startTime: t.startTime,
      endTime: t.endTime,
      playerCount: t.players.length,
      remaining: t.players.filter(p => !p.eliminated).length,
      players: t.players.map(p => ({ id: p.id, name: p.name, level: p.level, color: p.color, eliminated: p.eliminated, wins: p.wins })),
      currentMatches: t.currentMatches,
      recentMatches: t.matches.slice(-10),
    };
  }

  async getQueue() {
    try { return JSON.parse(await this.state.storage.get('queue') || '[]'); } catch { return []; }
  }

  async getActiveTournament() {
    try {
      const raw = await this.state.storage.get('tournament');
      if (!raw) return null;
      const t = JSON.parse(raw);
      // Auto-expire stale tournaments (older than 1 hour)
      if (Date.now() - t.startTime > 3600000) {
        await this.state.storage.delete('tournament');
        return null;
      }
      return t;
    } catch { return null; }
  }

  async getHistoryData() {
    try { return JSON.parse(await this.state.storage.get('history') || '[]'); } catch { return []; }
  }
}


// ═══════════════════════════════════════
//  FEEDBACK — In-game community feedback board
//  Categories: BUG, BALANCE, REMOVE, ADD, QOL, PRAISE
//  Topics: arena, guild, combat, pets, crafting, marketplace, etc.
// ═══════════════════════════════════════

export class Feedback {
  constructor(state, env) { this.state = state; this.env = env; }

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace('/api/feedback', '');
    const H = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' };

    try {
      // POST /submit — create a new feedback ticket
      if (request.method === 'POST' && path.startsWith('/submit')) {
        const body = await request.json();
        return new Response(JSON.stringify(await this.submit(body)), { headers: H });
      }

      // GET /list?sort=top|trending|new&topic=&category=&limit=&offset=
      if (request.method === 'GET' && path.startsWith('/list')) {
        const sort = url.searchParams.get('sort') || 'top';
        const topic = url.searchParams.get('topic') || null;
        const category = url.searchParams.get('category') || null;
        const limit = Math.min(50, parseInt(url.searchParams.get('limit')) || 20);
        const offset = parseInt(url.searchParams.get('offset')) || 0;
        return new Response(JSON.stringify(await this.list(sort, topic, category, limit, offset)), { headers: H });
      }

      // POST /vote — thumbs up or down
      if (request.method === 'POST' && path.startsWith('/vote')) {
        const body = await request.json();
        return new Response(JSON.stringify(await this.vote(body)), { headers: H });
      }

      // GET /stats — aggregate counts per topic/category
      if (request.method === 'GET' && path.startsWith('/stats')) {
        return new Response(JSON.stringify(await this.getStats()), { headers: H });
      }

      return new Response(JSON.stringify({ ok: false, error: 'Not found' }), { status: 404, headers: H });
    } catch (err) {
      return new Response(JSON.stringify({ ok: false, error: err.message }), { status: 500, headers: H });
    }
  }

  async submit(data) {
    const { playerId, playerName, category, topic, text } = data;
    if (!playerId || !playerName || !category || !topic || !text) return { ok: false, error: 'Missing fields' };
    if (text.length > 100) return { ok: false, error: 'Max 100 characters' };

    const VALID_CATEGORIES = ['bug', 'balance', 'remove', 'add', 'qol', 'praise'];
    if (!VALID_CATEGORIES.includes(category)) return { ok: false, error: 'Invalid category' };

    // Rate limit: max 5 submissions per player per hour
    const playerKey = 'rate:' + playerId;
    const rateData = JSON.parse(await this.state.storage.get(playerKey) || '{"count":0,"resetAt":0}');
    if (Date.now() < rateData.resetAt && rateData.count >= 5) return { ok: false, error: 'Rate limited — max 5/hour' };
    if (Date.now() >= rateData.resetAt) { rateData.count = 0; rateData.resetAt = Date.now() + 3600000; }
    rateData.count++;
    await this.state.storage.put(playerKey, JSON.stringify(rateData));

    const ticket = {
      id: crypto.randomUUID(),
      playerId, playerName, category, topic,
      text: text.slice(0, 100),
      up: 0, down: 0,
      voters: {}, // { playerId: 'up'|'down' }
      ts: Date.now(),
    };

    await this.state.storage.put('ticket:' + ticket.id, JSON.stringify(ticket));

    // Update topic count index
    const stats = JSON.parse(await this.state.storage.get('_stats') || '{}');
    const topicKey = topic + ':' + category;
    stats[topicKey] = (stats[topicKey] || 0) + 1;
    stats._total = (stats._total || 0) + 1;
    await this.state.storage.put('_stats', JSON.stringify(stats));

    return { ok: true, ticket: this.sanitize(ticket) };
  }

  async vote(data) {
    const { ticketId, playerId, vote } = data;
    if (!ticketId || !playerId || !['up', 'down'].includes(vote)) return { ok: false, error: 'Invalid vote' };

    const raw = await this.state.storage.get('ticket:' + ticketId);
    if (!raw) return { ok: false, error: 'Ticket not found' };

    const ticket = JSON.parse(raw);
    const prev = ticket.voters[playerId];

    // Remove previous vote
    if (prev === 'up') ticket.up--;
    if (prev === 'down') ticket.down--;

    // Toggle: if same vote, remove it; otherwise set new vote
    if (prev === vote) {
      delete ticket.voters[playerId];
    } else {
      ticket.voters[playerId] = vote;
      if (vote === 'up') ticket.up++;
      if (vote === 'down') ticket.down++;
    }

    await this.state.storage.put('ticket:' + ticketId, JSON.stringify(ticket));
    return { ok: true, up: ticket.up, down: ticket.down, myVote: ticket.voters[playerId] || null };
  }

  async list(sort, topic, category, limit, offset) {
    const entries = await this.state.storage.list({ prefix: 'ticket:' });
    let tickets = [];
    for (const [, raw] of entries) {
      try { tickets.push(JSON.parse(raw)); } catch {}
    }

    // Filter
    if (topic) tickets = tickets.filter(t => t.topic === topic);
    if (category) tickets = tickets.filter(t => t.category === category);

    // Sort
    if (sort === 'top') {
      // Ratio: up/(up+down), with minimum threshold. Wilson score lower bound simplified.
      tickets.sort((a, b) => {
        const scoreA = a.up + a.down > 0 ? (a.up - a.down) / (a.up + a.down + 1) + a.up * 0.01 : 0;
        const scoreB = b.up + b.down > 0 ? (b.up - b.down) / (b.up + b.down + 1) + b.up * 0.01 : 0;
        return scoreB - scoreA;
      });
    } else if (sort === 'trending') {
      // Recent votes weighted higher — score × recency
      const now = Date.now();
      tickets.sort((a, b) => {
        const ageA = Math.max(1, (now - a.ts) / 3600000); // hours
        const ageB = Math.max(1, (now - b.ts) / 3600000);
        const scoreA = (a.up - a.down * 0.5) / Math.pow(ageA, 0.5);
        const scoreB = (b.up - b.down * 0.5) / Math.pow(ageB, 0.5);
        return scoreB - scoreA;
      });
    } else {
      // New — most recent first
      tickets.sort((a, b) => b.ts - a.ts);
    }

    const total = tickets.length;
    tickets = tickets.slice(offset, offset + limit);

    return { ok: true, tickets: tickets.map(t => this.sanitize(t)), total, sort, offset, limit };
  }

  async getStats() {
    const stats = JSON.parse(await this.state.storage.get('_stats') || '{}');
    return { ok: true, stats };
  }

  sanitize(t) {
    return { id: t.id, playerName: t.playerName, category: t.category, topic: t.topic, text: t.text, up: t.up, down: t.down, ts: t.ts };
  }
}
