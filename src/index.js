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

// Event types the worker emits itself (server -> client).  The default
// branch of webSocketMessage rebroadcasts unknown msg.type values to
// every client, so we MUST refuse to rebroadcast any of these from a
// client -- otherwise a cheater can forge them and grief the room
// (e.g. forge player_state { hp: 0 } to one-shot everyone).
const PRIVILEGED_EVENTS = new Set([
  // Pool / progression mirrors
  'player_state', 'player_died', 'player_respawned',
  'combat_credit', 'harvest_credit', 'loot_credit',
  'stat_allocated', 'ability_rejected',
  // Combat resolution
  'monster_attack', 'monster_hit', 'monster_kill', 'pvp_hit',
  // World state fan-outs
  'loot_drop', 'loot_claimed', 'loot_despawn',
  'zone_monsters', 'zone_nodes', 'zone_loot',
  // Bootstrap + protocol
  'state_sync', 'tick', 'ping', 'player_count',
  'player_join', 'player_leave', 'player_update',
]);

export class GameRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = new Map();
    this.playerState = {};
    this.dirtyPlayers = new Set();
    this.eventBuffer = [];
    this.tickInterval = null;
    this.tickSeq = 0;
    this.TICK_RATE = 22; // 45Hz (22ms)
    this.MAX_PLAYERS = 50;
    this.EVENTS_PER_TICK_CAP = 500;
    this.WEAPON_STASH_CAP = 8; // mirrors WEAPON_STASH_MAX in src/data/gameSystems.js
    this.QUEST_AP_REWARD = 5;  // mirrors QUEST_AP_REWARD in src/data/items.js

    // §16.12 — PvP Lag Compensation
    this.stateHistory = {};
    this.LAGCOMP_BUFFER_TICKS = 14; // 300ms of history at 45Hz
    this.LAGCOMP_RTT_CAP = 300;
    this.LAGCOMP_RTT_ALPHA = 0.3;

    // Server-authoritative monsters
    this.monsters = {}; // zoneId -> [monster, ...]
    this.dirtyMonsters = new Set(); // zoneIds with changed monsters
    this.RESPAWN_TIME = 15000; // 15s respawn
    this.MONSTER_AGGRO_RANGE = 120; // pixels
    this.MONSTER_ATTACK_RANGE = 25;
    this.MONSTER_ATTACK_CD = 1500; // ms
    this.TILE = 32;

    // Server-authoritative gather nodes (trees / fish spots / ore veins).
    // Parallel to the monster pattern above: lazy-spawn on first player
    // entry per zone, store in this.nodes, mark dirty on state change,
    // tick respawns alongside _tickMonsters().
    this.nodes = {}; // zoneId -> [node, ...]
    this.dirtyNodes = new Set(); // zoneIds with changed node state
    this.NODE_RESPAWN_TIME = 120000; // 2 min — matches client v2.3.30

    // Server-authoritative ground loot.  Worker owns the canonical pile
    // list per zone; clients render from broadcasts and send pickup
    // requests via loot_pickup.  Server validates each pickup (range,
    // recipient, single-claim) and emits a private loot_credit back to
    // the picker with their authorized share + any one-of inventory.
    this.loot = {}; // zoneId -> [pile, ...]
    this.LOOT_EXPIRY_MS = 60000;
    this.LOOT_PICKUP_RANGE = 30; // px; slightly looser than client's 20 to absorb position lag
    this.SHARD_DROP_RATE = 0.10; // 10% per kill, matches client rollMonsterShard

    // AFK timeout — drop sessions that haven't sent real input (move /
    // combat / zone change / etc.) for this long.  Pong replies do NOT
    // reset this clock (see webSocketMessage below), so a tab that's
    // open but idle still gets booted instead of pinging forever and
    // consuming a session slot + tick bandwidth.
    this.IDLE_TIMEOUT_MS = 120000; // 2 minutes

    // On DO wake, close any hibernated sockets we don't have a session for.
    // These are orphans from prior wakes (crashed tabs, expired clients, etc.)
    // and would otherwise leak forever since webSocketClose only fires on TCP close.
    try {
      for (const ws of this.state.getWebSockets()) {
        try { ws.close(1000, 'stale on wake'); } catch {}
      }
    } catch {}
  }

  // Monster stat scaling (mirrors client-side monsterStat)
  _monsterStat(base, level, r1, r2, r3) {
    let v = base;
    for (let i = 1; i < level; i++) {
      if (i < 30) v *= r1;
      else if (i < 65) v *= r2;
      else v *= r3;
    }
    return Math.ceil(v);
  }

  // Archetype definitions (mirrors client ARCHETYPES — keep in sync
  // with src/data/gameSystems.js).
  _getArchetype(arch) {
    const ARCHETYPES = {
      fodder:   { hpMult: 0.6, dmgMult: 0.8, spdMult: 1.0, emoji: '🟢', color: '#3dd497' },
      brute:    { hpMult: 1.5, dmgMult: 1.3, spdMult: 0.7, emoji: '🪨', color: '#6b6b6b' },
      swarm:    { hpMult: 0.4, dmgMult: 0.6, spdMult: 1.2, emoji: '🦇', color: '#9333ea' },
      sentinel: { hpMult: 1.0, dmgMult: 1.0, spdMult: 1.0, emoji: '🛡️', color: '#e8e8e8' },
      volatile: { hpMult: 0.8, dmgMult: 1.0, spdMult: 1.0, emoji: '💥', color: '#ea580c' },
      stalker:  { hpMult: 0.7, dmgMult: 1.2, spdMult: 1.3, emoji: '👁️', color: '#2C3E50' },
      hexer:    { hpMult: 0.9, dmgMult: 0.8, spdMult: 1.0, emoji: '💀', color: '#8E44AD' },
      snowman:  { hpMult: 1.3, dmgMult: 1.1, spdMult: 0.8, emoji: '⛄', color: '#b0d8f0' },
    };
    return ARCHETYPES[arch] || ARCHETYPES.fodder;
  }

  // Zone spawn definitions (mirrors client src/data/zones.js).  w/h
  // match the client's 32x32-tile maps (1024x1024 world px) so
  // monsters spawn and roam inside the visible bounds — wider 50x40
  // values were spawning monsters off the client's map.  Level
  // ranges flattened to [1,10] across the board to match the client.
  _getZoneConfig(zoneId) {
    const ZONES = {
      meadow:  { w:32, h:32, level:[1,10], element:null,    spawns:[{arch:'fodder',count:10}] },
      ember:   { w:32, h:32, level:[1,10], element:'flame', spawns:[{arch:'fodder',count:6},{arch:'brute',count:3},{arch:'volatile',count:4}] },
      mist:    { w:32, h:32, level:[1,10], element:'venom', spawns:[{arch:'swarm',count:5},{arch:'stalker',count:3},{arch:'hexer',count:3}] },
      frost:   { w:32, h:32, level:[1,10], element:'frost', spawns:[{arch:'snowman',count:4}] },
      thunder: { w:32, h:32, level:[1,10], element:'storm', spawns:[{arch:'fodder',count:6},{arch:'volatile',count:4},{arch:'stalker',count:3}] },
      hollows: { w:32, h:32, level:[1,10], element:'stone', spawns:[{arch:'brute',count:4},{arch:'sentinel',count:3},{arch:'swarm',count:4}] },
      sky:     { w:32, h:32, level:[1,10], element:'wind',  spawns:[{arch:'stalker',count:4},{arch:'hexer',count:3},{arch:'volatile',count:3}] },
      tidal:   { w:32, h:32, level:[1,10], element:'water', spawns:[{arch:'swarm',count:4},{arch:'hexer',count:4},{arch:'brute',count:3}] },
    };
    return ZONES[zoneId] || null;
  }

  // Spawn monsters for a zone
  _spawnZoneMonsters(zoneId) {
    const zone = this._getZoneConfig(zoneId);
    if (!zone || !zone.spawns) return [];
    const W = zone.w * this.TILE;
    const H = zone.h * this.TILE;
    const margin = 4 * this.TILE;
    const monsters = [];
    let idx = 0;
    for (const spawn of zone.spawns) {
      for (let i = 0; i < spawn.count; i++) {
        const x = margin + Math.random() * (W - margin * 2);
        const y = margin + Math.random() * (H - margin * 2);
        const depthPct = Math.max(0, Math.min(1, y / H));
        const baseLvl = zone.level[0] || 1;
        const maxLvl = zone.level[1] || 10;
        const lvl = Math.max(1, Math.round(baseLvl + depthPct * (maxLvl - baseLvl)));
        const a = this._getArchetype(spawn.arch);
        const baseHp = this._monsterStat(60, lvl, 1.065, 1.035, 1.025);
        const baseDmg = this._monsterStat(12, lvl, 1.045, 1.025, 1.018);
        const baseXp = this._monsterStat(10, lvl, 1.045, 1.025, 1.018);
        const baseGold = this._monsterStat(5, lvl, 1.035, 1.020, 1.015);
        monsters.push({
          id: 'sm-' + zoneId + '-' + idx,
          arch: spawn.arch,
          level: lvl,
          element: zone.element || null,
          hp: Math.ceil(baseHp * a.hpMult),
          maxHp: Math.ceil(baseHp * a.hpMult),
          dmg: Math.ceil(baseDmg * a.dmgMult),
          xp: Math.ceil(baseXp),
          gold: Math.ceil(baseGold),
          spd: 0.5 * a.spdMult,
          emoji: a.emoji,
          color: a.color,
          x, y, spawnX: x, spawnY: y,
          alive: true,
          targetId: null, // player being chased
          atkCd: 0,
          respawnAt: 0,
        });
        idx++;
      }
    }
    return monsters;
  }

  // Ensure monsters exist for a zone (lazy spawn)
  _ensureZoneMonsters(zoneId) {
    if (!this.monsters[zoneId]) {
      this.monsters[zoneId] = this._spawnZoneMonsters(zoneId);
      if (this.monsters[zoneId].length > 0) this.dirtyMonsters.add(zoneId);
    }
    return this.monsters[zoneId];
  }

  // Get zones that have players in them
  _activeZones() {
    const zones = new Set();
    for (const ps of Object.values(this.playerState)) {
      if (ps.z && ps.z !== 'town' && ps.z !== 'farm_home') zones.add(ps.z);
    }
    return zones;
  }

  // Tick monster AI and respawns
  _tickMonsters() {
    const now = Date.now();
    const activeZones = this._activeZones();

    for (const zoneId of activeZones) {
      const monsters = this._ensureZoneMonsters(zoneId);
      if (!monsters || monsters.length === 0) continue;

      // Get players in this zone
      const playersInZone = [];
      for (const [id, ps] of Object.entries(this.playerState)) {
        if (ps.z === zoneId && !ps.dead && !ps.disconnected) {
          playersInZone.push({ id, ...ps });
        }
      }

      let zoneChanged = false;

      for (const m of monsters) {
        // Respawn check
        if (!m.alive) {
          if (m.respawnAt > 0 && now >= m.respawnAt) {
            m.alive = true;
            m.hp = m.maxHp;
            m.x = m.spawnX;
            m.y = m.spawnY;
            m.targetId = null;
            m.atkCd = 0;
            zoneChanged = true;
          }
          continue;
        }

        // Find nearest player for aggro
        let nearest = null;
        let nearestDist = Infinity;
        for (const p of playersInZone) {
          const dx = p.x - m.x;
          const dy = p.y - m.y;
          const dist = Math.sqrt(dx * dx + dy * dy);
          if (dist < nearestDist) {
            nearest = p;
            nearestDist = dist;
          }
        }

        // Movement AI
        if (nearest && nearestDist < this.MONSTER_AGGRO_RANGE) {
          m.targetId = nearest.id;
          // Move toward player
          if (nearestDist > this.MONSTER_ATTACK_RANGE) {
            const dx = nearest.x - m.x;
            const dy = nearest.y - m.y;
            const dist = Math.sqrt(dx * dx + dy * dy);
            if (dist > 0) {
              m.x += (dx / dist) * m.spd;
              m.y += (dy / dist) * m.spd;
              zoneChanged = true;
            }
          }

          // Attack player if in range
          if (nearestDist <= this.MONSTER_ATTACK_RANGE && now > m.atkCd) {
            // Don't fire damage events while the player is blocking — the
            // client's monster_attack handler also computes block reduction,
            // but that path was producing inconsistent block resolution
            // (client snapshot of monster position can drift from server,
            // making the directional arc test miss). Skipping the event
            // entirely when the player has shield up gives reliable
            // blocking. We still set atkCd so the monster doesn't keep
            // queuing while the player blocks.
            if (nearest.blocking) {
              m.atkCd = now + this.MONSTER_ATTACK_CD;
              // Block cost: 15 stamina (mirrors client at BroTown.jsx:2663).
              // Server is authoritative for stamina now, so deduct here
              // and echo via player_state so the bar visibly drops.
              const blockerPs = this.playerState[nearest.id];
              if (blockerPs && typeof blockerPs.stamina === 'number') {
                blockerPs.stamina = Math.max(0, blockerPs.stamina - 15);
                this._saveRpg(nearest.id, blockerPs);
                const blockerWs = this._wsBySessionId(nearest.id);
                if (blockerWs) this._sendPlayerState(blockerWs, nearest.id);
              }
              continue;
            }
            m.atkCd = now + this.MONSTER_ATTACK_CD;
            // Apply HP damage server-side BEFORE emitting the event so
            // dmgTaken rides on the wire and the client renders the
            // exact number the server applied.  Block already handled
            // by the early-continue above (server skips the attack
            // entirely while shielded), but pass !blocking to be
            // defensive in case the path changes.
            const targetPs = this.playerState[nearest.id];
            const dmgTaken = this._applyDamage(targetPs, m.dmg, false);
            this.eventBuffer.push({
              type: 'monster_attack',
              payload: {
                monsterId: m.id,
                targetId: nearest.id,
                dmg: m.dmg,
                dmgTaken,
                zone: zoneId,
                attackerX: m.x,
                attackerY: m.y,
              }
            });
            // Echo authoritative hp to the victim + persist.  Death
            // check feeds the player_died event below.
            if (targetPs) {
              this._saveRpg(nearest.id, targetPs);
              const victimWs = this._wsBySessionId(nearest.id);
              if (victimWs) this._sendPlayerState(victimWs, nearest.id);
              if (targetPs.hp <= 0 && !targetPs.dying) {
                this._handlePlayerDeath(targetPs, nearest.id, 'monster:' + m.id);
              }
            }
            // Mark zone dirty so the monster's position is included in the
            // outgoing tick delta. Without this, a stationary monster that
            // attacks a stationary player produces attack events but no
            // position broadcast, so any client that missed the initial sync
            // never registers the monster locally — leading to "ghost hit"
            // damage reports with no visible attacker.
            zoneChanged = true;
          }
        } else {
          // Idle wander — slow random movement back toward spawn
          m.targetId = null;
          const dxSpawn = m.spawnX - m.x;
          const dySpawn = m.spawnY - m.y;
          const distSpawn = Math.sqrt(dxSpawn * dxSpawn + dySpawn * dySpawn);
          if (distSpawn > 30) {
            m.x += (dxSpawn / distSpawn) * m.spd * 0.3;
            m.y += (dySpawn / distSpawn) * m.spd * 0.3;
            zoneChanged = true;
          }
        }
      }

      if (zoneChanged) this.dirtyMonsters.add(zoneId);
    }
  }

  // ═══ Gather nodes (trees / fish spots / ore veins) ═══
  //
  // The client owns the tier/name/flavor data tables (WOODCUTTING_TIERS
  // / FISHING_TIERS / MINING_TIERS in src/data/lifeSkills.js).  The
  // server only needs to know: how many of each type per zone, their
  // positions, alive/respawnAt, and a tierLvl per node so two clients
  // see the same tier (otherwise each client's createGatherNode() picks
  // a tier via Math.random() and they diverge).
  //
  // tierLvl values for the "shallow" depth: 1 or 6 — that's the set of
  // tier .lvl values <= 10 across all three tier tables.
  _getShallowNodeTierLvls() {
    return [1, 6]; // eligible tier .lvl values for depth=shallow
  }

  // Per-zone node count + type split — mirrors client lifeSkills.js
  // spawnGatherNodes() for shallow depth (nodeCount=8: 40% tree, 25%
  // fish, 35% ore -> 4 / 2 / 2).  Town and farm_home are skipped at
  // the call sites.
  _getZoneNodeConfig(zoneId) {
    // Every combat zone gets the same shallow-depth mix today.  Custom
    // per-zone tuning (e.g. extra fish spots in tidal) can land here
    // later without touching the client.
    return { treeCt: 4, fishCt: 2, oreCt: 2 };
  }

  // Spawn the static node layout for a zone.  Positions are randomized
  // once at first-ever zone activation; after that they're fixed for
  // the lifetime of the Durable Object (re-randomized only on DO wake).
  _spawnZoneNodes(zoneId) {
    const zone = this._getZoneConfig(zoneId);
    if (!zone) return [];
    const W = zone.w * this.TILE;
    const H = zone.h * this.TILE;
    const margin = 8 * this.TILE; // matches client lifeSkills.js inset
    const cfg = this._getZoneNodeConfig(zoneId);
    const tierLvls = this._getShallowNodeTierLvls();
    const nodes = [];
    let idx = 0;
    const placeOne = (type) => {
      const x = margin + Math.random() * (W - margin * 2);
      const y = margin + Math.random() * (H - margin * 2);
      const tierLvl = tierLvls[Math.floor(Math.random() * tierLvls.length)];
      nodes.push({
        id: 'sn-' + zoneId + '-' + idx,
        nodeType: type,
        x, y,
        tierLvl,
        alive: true,
        respawnAt: 0,
      });
      idx++;
    };
    for (let i = 0; i < cfg.treeCt; i++) placeOne('tree');
    for (let i = 0; i < cfg.fishCt; i++) placeOne('fishSpot');
    for (let i = 0; i < cfg.oreCt; i++) placeOne('oreVein');
    return nodes;
  }

  _ensureZoneNodes(zoneId) {
    if (zoneId === 'town' || zoneId === 'farm_home') return [];
    if (!this.nodes[zoneId]) {
      this.nodes[zoneId] = this._spawnZoneNodes(zoneId);
      if (this.nodes[zoneId].length > 0) this.dirtyNodes.add(zoneId);
    }
    return this.nodes[zoneId];
  }

  // Tick the node respawn loop — flip alive=true on any depleted node
  // whose respawnAt has passed.  No need to scope to "active zones"
  // like _tickMonsters; gather respawn is cheap and tiny.
  _tickNodes() {
    const now = Date.now();
    for (const zoneId of Object.keys(this.nodes)) {
      const list = this.nodes[zoneId];
      if (!list || list.length === 0) continue;
      let changed = false;
      for (const n of list) {
        if (!n.alive && n.respawnAt > 0 && now >= n.respawnAt) {
          n.alive = true;
          n.respawnAt = 0;
          changed = true;
        }
      }
      if (changed) this.dirtyNodes.add(zoneId);
    }
  }

  // Process a player's harvest strike against a gather node.  The
  // client's minigame already gates this on success (mining miss
  // does NOT send node_strike), so we just validate and apply.
  // Tier + resource key mappings for gather nodes.  Hardcoded on the
  // server so the client can't cheat the harvest by lying about what
  // tier was struck.  Limited to the "shallow" depth tier set today
  // (tierLvl 1 + 6); extend if/when deeper depths reach the server.
  _harvestNameForTier(nodeType, tierLvl) {
    const TREE = { 1: 'Kindling', 6: 'Softwood' };
    const FISH = { 1: 'Minnow',   6: 'Clownfish' };
    const ORE  = { 1: 'Copper Ore', 6: 'Iron Ore' };
    const t = tierLvl || 1;
    if (nodeType === 'tree') return TREE[t] || TREE[1];
    if (nodeType === 'fishSpot') return FISH[t] || FISH[1];
    return ORE[t] || ORE[1];
  }

  _harvestResourceType(nodeType) {
    if (nodeType === 'tree') return 'wood';
    if (nodeType === 'fishSpot') return 'fish';
    return 'ore';
  }

  _harvestInvKey(nodeType, tierLvl) {
    const name = this._harvestNameForTier(nodeType, tierLvl);
    const resType = this._harvestResourceType(nodeType);
    return resType + '_' + name.replace(/\s+/g, '_').toLowerCase();
  }

  _harvestYieldMult(accuracy) {
    if (accuracy === 'perfect') return 2;
    return 1; // 'good' / 'ok' / unknown
  }

  _harvestXpMult(accuracy) {
    if (accuracy === 'perfect') return 2.0;
    if (accuracy === 'good') return 1.5;
    return 1.0; // 'ok' / unknown
  }

  // Slice 18: rate-limit 'perfect' harvest claims.  The minigame
  // outcome is still client-trusted (server doesn't simulate the
  // minigame), so a cheater could spam accuracy:'perfect' for the
  // doubled yield + XP.  Bound it: only HARVEST_PERFECT_PER_MIN
  // "perfect" claims accepted per 60s window per player; excess
  // downgrades to 'good' (keeps the XP bonus a skilled player
  // would earn but drops the yield doubler).
  //
  // 10/min = 1 every 6 sec, well above the realistic minigame
  // cadence for legit play (each fishing / mining / wood-chop
  // minigame takes several seconds + walk-to-next-node time).
  _ratedHarvestAccuracy(ps, claimed) {
    if (claimed !== 'perfect') return claimed || 'ok';
    const now = Date.now();
    if (!Array.isArray(ps._perfectHistory)) ps._perfectHistory = [];
    // Prune entries older than 60 sec.
    ps._perfectHistory = ps._perfectHistory.filter((t) => (now - t) < 60000);
    if (ps._perfectHistory.length >= 10) {
      return 'good'; // cap exceeded
    }
    ps._perfectHistory.push(now);
    return 'perfect';
  }

  _harvestSkillName(nodeType) {
    if (nodeType === 'tree') return 'woodcutting';
    if (nodeType === 'fishSpot') return 'fishing';
    return 'mining';
  }

  // Base XP per harvest = ceil(tierLvl * 1.5 + 5); the accuracy
  // multiplier (xpMult above) is applied on top.  Mirrors the client
  // formula in createGatherNode (lifeSkills.js).
  _harvestXpForTier(tierLvl, accuracy) {
    const baseXp = Math.ceil(((tierLvl || 1) * 1.5) + 5);
    return Math.ceil(baseXp * this._harvestXpMult(accuracy));
  }

  // lifeSkill level-up threshold curve.  Mirrors LIFE_SKILL_XP on the
  // client (lifeSkills.js): ceil(500 * 1.08^(level - 1)).
  _lifeSkillXpThreshold(level) {
    return Math.ceil(500 * Math.pow(1.08, (level || 1) - 1));
  }

  // Apply XP to a lifeSkill, returns { leveled, newLevel }.  Mirrors
  // addLifeSkillXp on the client; needs to stay byte-identical so
  // local-vs-server level outcomes don't drift.
  _addLifeSkillXp(ps, skill, xpAmt) {
    if (!ps.lifeSkills) ps.lifeSkills = {};
    if (!ps.lifeSkills[skill]) ps.lifeSkills[skill] = { level: 1, xp: 0 };
    const s = ps.lifeSkills[skill];
    s.xp = (s.xp || 0) + xpAmt;
    let leveled = false;
    while (s.xp >= this._lifeSkillXpThreshold(s.level || 1)) {
      s.xp -= this._lifeSkillXpThreshold(s.level || 1);
      s.level = (s.level || 1) + 1;
      leveled = true;
    }
    return { leveled, newLevel: s.level };
  }

  // 33% shard drop per successful harvest (matches the client's
  // rollHarvestShard rate; the monster-kill path uses 10% via
  // _rollShardForKill above).  Server-rolled so a modified client
  // can't force shard drops.
  _rollHarvestShard(zoneId) {
    if (Math.random() >= 0.33) return null;
    return 'shard_' + zoneId;
  }

  // ═══ Combat XP + level (server-authoritative) ═══
  //
  // Mirrors xpRequired() in src/data/gameSystems.js so the worker
  // computes the same level-up threshold the client used to.  Three
  // segments (lvl <= 30, <= 65, <= 100) plus a post-100 prestige
  // ramp -- keep this byte-identical with the client if you ever
  // tune the curve.
  _xpRequiredForLevel(level) {
    const L = level || 1;
    if (L <= 30) return Math.ceil(500 * Math.pow(1.10, L - 1));
    const at30 = Math.ceil(500 * Math.pow(1.10, 29));
    if (L <= 65) return Math.ceil(at30 * Math.pow(1.07, L - 30));
    const at65 = Math.ceil(at30 * Math.pow(1.07, 35));
    if (L <= 100) return Math.ceil(at65 * Math.pow(1.04, L - 65));
    const at100 = Math.ceil(at65 * Math.pow(1.04, 35));
    return Math.ceil(at100 * Math.pow(1.08, L - 100));
  }

  // Apply combat XP, level-up loop, +5 unspentT2 per level (GDD §1.4).
  // Returns { leveled, levelsGained, newLevel } so the caller can
  // emit a combat_credit event for the picker's level-up popup +
  // SFX.  Mutates ps.level, ps.xp, ps.unspentT2 in place.
  _addCombatXp(ps, xpAmt) {
    if (!ps) return { leveled: false, levelsGained: 0, newLevel: 1 };
    ps.level = ps.level || 1;
    ps.xp = (ps.xp || 0) + (xpAmt || 0);
    ps.unspentT2 = ps.unspentT2 || 0;
    let levelsGained = 0;
    const LEVEL_CAP = 100;
    while (ps.level < LEVEL_CAP && ps.xp >= this._xpRequiredForLevel(ps.level)) {
      ps.xp -= this._xpRequiredForLevel(ps.level);
      ps.level += 1;
      ps.unspentT2 += 5;
      levelsGained += 1;
    }
    // At cap, drop any further xp so the counter doesn't overflow
    // Number.MAX_SAFE_INTEGER after extreme play.
    if (ps.level >= LEVEL_CAP) ps.xp = 0;
    return { leveled: levelsGained > 0, levelsGained, newLevel: ps.level };
  }

  // ═══ T2 stat allocation (server-validated) ═══
  //
  // Client sends stat_allocate { stat }; worker validates that
  // ps.unspentT2 > 0 and the stat name is in the 10-stat list,
  // decrements unspentT2 by 1, persists, and emits a private
  // stat_allocated event so the client applies R[stat]++ + recalc.
  // Closes the "spend more T2 points than you have" cheat -- the
  // client can no longer mint phantom unspentT2 via localStorage
  // because the server is the source of truth for the counter.
  //
  // What's NOT closed: directly writing R.power = 999 in DevTools.
  // T1 use-trained increments also still flow client-side.  Closing
  // those needs server-tracked stat VALUES (with T1 mutations also
  // server-mediated); a bigger slice -- this one just enforces the
  // T2 spend gate.
  _isValidStat(stat) {
    return stat === 'power' || stat === 'vitality' || stat === 'endurance'
        || stat === 'agility' || stat === 'mind' || stat === 'ferocity'
        || stat === 'elementalMastery' || stat === 'fortification'
        || stat === 'restoration' || stat === 'influence';
  }

  _handleStatAllocate(session, payload) {
    if (!session || !session.id) return;
    const { stat } = payload || {};
    if (!this._isValidStat(stat)) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if ((ps.unspentT2 || 0) <= 0) return;
    ps.unspentT2 -= 1;
    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) {
      try {
        ws.send(JSON.stringify({
          type: 'stat_allocated',
          payload: { stat, newUnspentT2: ps.unspentT2 },
        }));
      } catch (e) {}
      this._sendPlayerState(ws, session.id);
    }
  }

  // ═══ Eating cooked fish (server-authoritative HP heal) ═══
  //
  // Client sends eat_request { invKey } when the player clicks Eat on
  // a cooked_fish_* inventory item.  Server validates the player owns
  // at least one of the item, looks up the heal amount from the
  // hardcoded fish-tier table (mirrors client getFishHealAmount in
  // gameSystems.js), decrements inventory, increments hp (clamped to
  // maxHp), persists, and emits player_state.
  //
  // Closes the "eat to heal beyond what server thinks" cheat: server
  // applies the heal, so a modified client that bypasses inventory
  // decrement still gets stomped on the next player_state.  Mirrors
  // FISHING_TIERS from src/data/lifeSkills.js -- keep in sync if new
  // fish tiers ship to the client.
  _fishHealAmount(invKey) {
    if (typeof invKey !== 'string') return 0;
    if (!invKey.startsWith('cooked_fish_') && !invKey.startsWith('fish_')) return 0;
    // Strip 'fish_' or 'cooked_fish_' prefix to get the species name.
    const species = invKey.replace(/^(cooked_)?fish_/, '').toLowerCase();
    const TIERS = [
      { lvl: 1,  name: 'minnow' },
      { lvl: 6,  name: 'clownfish' },
      { lvl: 11, name: 'trout' },
    ];
    const tier = TIERS.find((t) => species.includes(t.name));
    if (!tier) return 20; // default for unmapped cooked fish
    return Math.ceil(15 + tier.lvl * 8);
  }

  _handleEatRequest(session, payload) {
    if (!session || !session.id) return;
    const { invKey } = payload || {};
    if (typeof invKey !== 'string') return;
    // Only cooked_fish_* keys are edible this slice; raw fish goes
    // through cook_request first.
    if (!invKey.startsWith('cooked_fish_')) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    if (!ps.inventory) ps.inventory = {};
    if ((ps.inventory[invKey] || 0) <= 0) return;
    const heal = this._fishHealAmount(invKey);
    if (heal <= 0) return;
    // Decrement inventory + apply heal.  Heal is "wasted" if at max;
    // we still consume the item to match client semantics (the click
    // handler returns early at full, but a race-condition cheater
    // could trigger this server-side -- consume anyway).
    ps.inventory[invKey] -= 1;
    if (ps.inventory[invKey] <= 0) delete ps.inventory[invKey];
    if (typeof ps.maxHp !== 'number') ps.maxHp = 100;
    if (typeof ps.hp !== 'number') ps.hp = ps.maxHp;
    ps.hp = Math.min(ps.maxHp, ps.hp + heal);
    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) this._sendPlayerState(ws, session.id);
  }

  // ═══ Equipment store (opaque blobs + equip_request) ═══
  //
  // Slots tracked on playerState:
  //   weapon         -- active melee weapon
  //   rangedWeapon   -- active ranged weapon (bow / crossbow)
  //   staffWeapon    -- active staff weapon
  //   activeSlot     -- 'melee' | 'ranged' | 'staff' (which is "in hand")
  //   armor          -- equipped armor
  //   shield         -- equipped shield (with off-hand)
  //   amulet         -- equipped amulet
  //   weaponStash    -- array of stored weapons (max WEAPON_STASH_MAX = 8)
  //
  // This slice stores equipment as opaque objects the client provided.
  // Server doesn't yet compute weapon stats (base damage, tier mult,
  // etc.) -- that mirror lands in the "server-computed damage" slice.
  // The cheat closure here is "is this a fake item?": with equipment
  // server-tracked, future slices can validate that a sold weapon
  // actually exists in the player's stash / active slot before
  // crediting coins or pushing to the marketplace.
  //
  // Mirror of WEAPON_TYPES base damage values from
  // src/data/gameSystems.js.  Used for sell-value math and (later)
  // server-computed weapon damage.  Keep in sync if new weapon types
  // ship to the client.
  _weaponBase(type) {
    const T = { greatsword: 48, sword: 32, bow: 35, staff: 41 };
    return T[type] || 30;
  }

  // Sell value mirrors the client at BroTown.jsx ~26613:
  //   ceil((tierMult || 1) * (WEAPON_TYPES[type].base || 30) * 0.5)
  _weaponSellValue(weapon) {
    if (!weapon) return 0;
    const tierMult = (typeof weapon.tierMult === 'number' && weapon.tierMult > 0) ? weapon.tierMult : 1;
    const base = this._weaponBase(weapon.type);
    return Math.max(1, Math.ceil(tierMult * base * 0.5));
  }

  _handleSellWeapon(session, payload) {
    if (!session || !session.id) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    const { stashIdx } = payload || {};
    if (!Number.isInteger(stashIdx) || stashIdx < 0) return;
    if (!Array.isArray(ps.weaponStash) || stashIdx >= ps.weaponStash.length) return;
    const weapon = ps.weaponStash[stashIdx];
    if (!weapon) return;
    const sellVal = this._weaponSellValue(weapon);
    ps.weaponStash.splice(stashIdx, 1);
    ps.coins = (ps.coins || 0) + sellVal;
    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) this._sendPlayerState(ws, session.id);
  }

  // equip_request swaps a stash entry with an active equipment slot.
  // Server validates stashIdx is in range + slot name is known.
  // (WEAPON_STASH_CAP set in constructor; mirrors WEAPON_STASH_MAX
  // in src/data/gameSystems.js.)
  _isValidEquipSlot(slot) {
    return slot === 'weapon' || slot === 'rangedWeapon' || slot === 'staffWeapon'
        || slot === 'armor' || slot === 'shield' || slot === 'amulet';
  }

  // ═══ Quests (accept + turn-in with reward validation) ═══
  //
  // Mirrors the 25-quest QUEST_CHAINS table in src/data/gameSystems.js
  // for reward amounts + chain progression.  The QUEST COMPLETION
  // CRITERIA (kill counts, item collection, NPC interactions) still
  // run client-side -- mirroring them all would require porting the
  // full quest.check predicate for every quest, plus tracking every
  // mutation that feeds those predicates (loot pickup keys, monster
  // kills, item drops, etc.).  Out of scope for this slice.
  //
  // What this slice closes:
  //   - quest_turn_in spam for free rewards (server checks state
  //     transitions: must be 'active' before turning in).
  //   - Cheater claiming a higher-tier quest's reward by forging
  //     the questId (server uses its own reward table lookup).
  //   - Accepting a quest the player isn't supposed to have yet
  //     (chain order: must be 'available' before active).
  //
  // What still depends on client trust:
  //   - The "quest is actually completed" claim.  Cheater can
  //     accept a quest, immediately turn it in (without doing the
  //     work), and get the reward.  Closing this needs server-
  //     tracked kill counts / inventory acquisition flags / NPC
  //     dialog state -- a separate, bigger slice.
  _QUEST_REWARDS_DATA() {
    return {
      mayor_1:    {gold:50,  xp:30,  next:'mayor_2'},
      mayor_2:    {gold:100, xp:80,  next:'mayor_3'},
      mayor_3:    {gold:300, xp:200, next:null},
      trader_1:   {gold:25,  xp:20,  next:'trader_2'},
      trader_2:   {gold:75,  xp:50,  next:'trader_3'},
      trader_3:   {gold:150, xp:100, next:null},
      enchant_1:  {gold:50,  xp:40,  next:'enchant_2'},
      enchant_2:  {gold:200, xp:150, next:'enchant_3'},
      enchant_3:  {gold:500, xp:300, next:null},
      scout_1:    {gold:100, xp:80,  next:'scout_2'},
      scout_2:    {gold:200, xp:150, next:null},
      bron_1:     {gold:60,  xp:40,  next:'bron_2'},
      bron_2:     {gold:120, xp:80,  next:'bron_3'},
      bron_3:     {gold:200, xp:150, next:'bron_4'},
      bron_4:     {gold:400, xp:250, next:null},
      luna_1:     {gold:40,  xp:30,  next:'luna_2'},
      luna_2:     {gold:100, xp:70,  next:'luna_3'},
      luna_3:     {gold:250, xp:180, next:null},
      kai_1:      {gold:80,  xp:60,  next:'kai_2'},
      kai_2:      {gold:200, xp:120, next:'kai_3'},
      kai_3:      {gold:350, xp:200, next:null},
      ash_1:      {gold:100, xp:80,  next:'ash_2'},
      ash_2:      {gold:250, xp:180, next:'ash_3'},
      ash_3:      {gold:500, xp:350, next:'ash_4'},
      ash_4:      {gold:800, xp:500, next:null},
    };
  }

  // (this.QUEST_AP_REWARD set in constructor; mirrors QUEST_AP_REWARD
  // in src/data/items.js -- 5 AP per quest.)
  _handleQuestAccept(session, payload) {
    if (!session || !session.id) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    const { questId } = payload || {};
    if (typeof questId !== 'string') return;
    const reward = this._QUEST_REWARDS_DATA()[questId];
    if (!reward) return; // unknown quest
    if (!ps._quests) ps._quests = {};
    const cur = ps._quests[questId];
    // Allow accepting from 'available' (chain entry granted) or
    // from missing (first quest in chain).  Reject if already
    // active / turnedIn.
    if (cur === 'active' || cur === 'turnedIn') return;
    ps._quests[questId] = 'active';
    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) this._sendPlayerState(ws, session.id);
  }

  _handleQuestTurnIn(session, payload) {
    if (!session || !session.id) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    const { questId } = payload || {};
    if (typeof questId !== 'string') return;
    const reward = this._QUEST_REWARDS_DATA()[questId];
    if (!reward) return;
    if (!ps._quests) ps._quests = {};
    // Must be 'active' to turn in.  This is the spam-defeat:
    // a cheater can't reclaim the reward by spamming the event,
    // and can't claim a quest they never accepted.
    if (ps._quests[questId] !== 'active') return;
    ps._quests[questId] = 'turnedIn';
    ps.coins = (ps.coins || 0) + (reward.gold || 0);
    // XP via _addCombatXp so level-up logic runs (including
    // pool restores via _recomputeMaxes inside).
    if (reward.xp > 0) {
      const { leveled } = this._addCombatXp(ps, reward.xp);
      if (leveled) {
        this._recomputeMaxes(ps);
        if (typeof ps.maxHp === 'number') ps.hp = ps.maxHp;
        if (typeof ps.maxStamina === 'number') ps.stamina = ps.maxStamina;
        if (typeof ps.maxMana === 'number') ps.mana = ps.maxMana;
      }
    }
    ps.achievementPoints = (ps.achievementPoints || 0) + this.QUEST_AP_REWARD;
    // Unlock next quest in chain.
    if (reward.next && !ps._quests[reward.next]) {
      ps._quests[reward.next] = 'available';
    }
    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) this._sendPlayerState(ws, session.id);
  }

  // ═══ Weapon crafting (blacksmith + woodworker) ═══
  //
  // Mirrors BLACKSMITH_TIERS + WOODWORKING_TIERS from src/data/
  // gameSystems.js (20 tiers each).  Only the fields the worker
  // needs are mirrored (minLvl / tierMult / statReq / *Cost +
  // wood resource key for ww).  Display fields (label / color /
  // desc) stay client-only since the worker doesn't render UI.
  //
  // Client sends forge_weapon { weaponType, tierKey, isWoodwork }.
  // Server validates:
  //   - tierKey exists in the matching tier table
  //   - ps.lifeSkills.[blacksmithing|woodworking].level >= minLvl
  //   - ps[required stat] >= statReq (per EQUIP_STAT_MAP)
  //   - ps.inventory has required ore/wood
  //   - ps.coins >= goldCost
  // Then consumes ingredients + coins, mints the new weapon
  // (matches the client weapon shape exactly), swaps old active
  // weapon to stash (rejected if stash full), applies crafting XP,
  // and emits player_state.  Closes the "forge max-tier weapon for
  // free" cheat: a cheater bypassing the local resource consume
  // still gets stomped because the worker re-validates + applies.
  _BLACKSMITH_TIERS_DATA() {
    // 20 tiers from BLACKSMITH_TIERS.  Keep in sync if the client
    // ships new tiers (greatsword/sword forge use these via
    // gearBase = tier key).
    return {
      wood:         {minLvl:1, slots:1, oreName:'wood',          oreCost:3,  goldCost:8,    tierMult:1.00, statReq:0  },
      copper:       {minLvl:6, slots:1, oreName:'copper',        oreCost:3,  goldCost:20,   tierMult:1.12, statReq:10 },
      iron:         {minLvl:11,slots:1, oreName:'iron',          oreCost:4,  goldCost:35,   tierMult:1.25, statReq:20 },
      steel:        {minLvl:16,slots:1, oreName:'steel',         oreCost:5,  goldCost:55,   tierMult:1.40, statReq:30 },
      titanium:     {minLvl:21,slots:1, oreName:'titanium',      oreCost:5,  goldCost:85,   tierMult:1.56, statReq:40 },
      obsidian:     {minLvl:26,slots:1, oreName:'obsidian',      oreCost:6,  goldCost:120,  tierMult:1.74, statReq:50 },
      mythril:      {minLvl:31,slots:2, oreName:'mythril',       oreCost:7,  goldCost:170,  tierMult:1.94, statReq:60 },
      diamond:      {minLvl:36,slots:2, oreName:'diamond',       oreCost:8,  goldCost:240,  tierMult:2.16, statReq:70 },
      abyssal:      {minLvl:41,slots:2, oreName:'abyssal',       oreCost:9,  goldCost:330,  tierMult:2.40, statReq:80 },
      dragonbone:   {minLvl:46,slots:2, oreName:'dragonbone',    oreCost:10, goldCost:440,  tierMult:2.68, statReq:90 },
      shadowsteel:  {minLvl:51,slots:2, oreName:'shadowsteel',   oreCost:11, goldCost:570,  tierMult:2.98, statReq:100},
      bloodstone:   {minLvl:56,slots:2, oreName:'bloodstone',    oreCost:12, goldCost:720,  tierMult:3.32, statReq:110},
      runestone:    {minLvl:61,slots:2, oreName:'runite',        oreCost:13, goldCost:900,  tierMult:3.70, statReq:120},
      sunstone:     {minLvl:66,slots:2, oreName:'sunstone',      oreCost:14, goldCost:1100, tierMult:4.12, statReq:130},
      demonite:     {minLvl:71,slots:2, oreName:'demonite',      oreCost:15, goldCost:1350, tierMult:4.58, statReq:140},
      spiritforge:  {minLvl:76,slots:2, oreName:'spiritore',     oreCost:16, goldCost:1650, tierMult:5.10, statReq:150},
      starforged:   {minLvl:81,slots:2, oreName:'starite',       oreCost:18, goldCost:2000, tierMult:5.68, statReq:160},
      celestial:    {minLvl:86,slots:2, oreName:'celestite',     oreCost:20, goldCost:2500, tierMult:6.32, statReq:170},
      antimatter:   {minLvl:91,slots:2, oreName:'antimatter',    oreCost:22, goldCost:3200, tierMult:7.04, statReq:180},
      worldbreaker: {minLvl:96,slots:2, oreName:'voidcrystal',   oreCost:25, goldCost:4200, tierMult:7.84, statReq:190},
    };
  }

  _WOODWORKING_TIERS_DATA() {
    return {
      wood:         {minLvl:1, slots:1, wood:'wood',         woodCost:3,  goldCost:8,    tierMult:1.00, statReq:0  },
      softwood:     {minLvl:6, slots:1, wood:'softwood',     woodCost:3,  goldCost:20,   tierMult:1.12, statReq:10 },
      hardwood:     {minLvl:11,slots:1, wood:'hardwood',     woodCost:4,  goldCost:35,   tierMult:1.25, statReq:20 },
      pine:         {minLvl:16,slots:1, wood:'pine_lumber',  woodCost:5,  goldCost:55,   tierMult:1.40, statReq:30 },
      maple:        {minLvl:21,slots:1, wood:'maple_wood',   woodCost:5,  goldCost:85,   tierMult:1.56, statReq:40 },
      ironbark:     {minLvl:26,slots:1, wood:'ironbark',     woodCost:6,  goldCost:120,  tierMult:1.74, statReq:50 },
      crystalwood:  {minLvl:31,slots:2, wood:'crystal_wood', woodCost:7,  goldCost:170,  tierMult:1.94, statReq:60 },
      elder:        {minLvl:36,slots:2, wood:'elder_wood',   woodCost:8,  goldCost:240,  tierMult:2.16, statReq:70 },
      spiritwood:   {minLvl:41,slots:2, wood:'spirit_wood',  woodCost:9,  goldCost:330,  tierMult:2.40, statReq:80 },
      dragonwood:   {minLvl:46,slots:2, wood:'dragon_wood',  woodCost:10, goldCost:440,  tierMult:2.68, statReq:90 },
      shadowthorn:  {minLvl:51,slots:2, wood:'shadowthorn',  woodCost:11, goldCost:570,  tierMult:2.98, statReq:100},
      bloodoak:     {minLvl:56,slots:2, wood:'bloodoak',     woodCost:12, goldCost:720,  tierMult:3.32, statReq:110},
      runewood:     {minLvl:61,slots:2, wood:'runewood',     woodCost:13, goldCost:900,  tierMult:3.70, statReq:120},
      sunbark:      {minLvl:66,slots:2, wood:'sunbark',      woodCost:14, goldCost:1100, tierMult:4.12, statReq:130},
      demonwood:    {minLvl:71,slots:2, wood:'demonwood',    woodCost:15, goldCost:1350, tierMult:4.58, statReq:140},
      ghostwood:    {minLvl:76,slots:2, wood:'ghostwood',    woodCost:16, goldCost:1650, tierMult:5.10, statReq:150},
      starwood:     {minLvl:81,slots:2, wood:'starwood',     woodCost:18, goldCost:2000, tierMult:5.68, statReq:160},
      worldtree:    {minLvl:86,slots:2, wood:'worldtree',    woodCost:20, goldCost:2500, tierMult:6.32, statReq:170},
      voidtimber:   {minLvl:91,slots:2, wood:'void_timber',  woodCost:22, goldCost:3200, tierMult:7.04, statReq:180},
      worldbreaker: {minLvl:96,slots:2, wood:'voidwood',     woodCost:25, goldCost:4200, tierMult:7.84, statReq:190},
    };
  }

  // EQUIP_STAT_MAP mirror.  Used for the forge statReq gate.
  _equipStatFor(weaponType) {
    if (weaponType === 'greatsword') return 'power';
    if (weaponType === 'sword') return 'agility';
    if (weaponType === 'bow') return 'agility';
    if (weaponType === 'staff') return 'mind';
    return 'power';
  }

  _handleForgeWeapon(session, payload) {
    if (!session || !session.id) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    const { weaponType, tierKey, isWoodwork } = payload || {};
    if (weaponType !== 'greatsword' && weaponType !== 'sword' && weaponType !== 'bow' && weaponType !== 'staff') return;
    if (typeof tierKey !== 'string') return;

    // Validate woodwork-vs-blacksmith match with weapon type.
    // Blacksmith forges melee (greatsword/sword); woodworking
    // forges ranged (bow/staff).  Reject mismatches.
    const wantWw = (weaponType === 'bow' || weaponType === 'staff');
    if (wantWw !== !!isWoodwork) return;

    const table = wantWw ? this._WOODWORKING_TIERS_DATA() : this._BLACKSMITH_TIERS_DATA();
    const tier = table[tierKey];
    if (!tier) return;

    // Skill level gate
    const skillName = wantWw ? 'woodworking' : 'blacksmithing';
    const skillLvl = (ps.lifeSkills && ps.lifeSkills[skillName] && ps.lifeSkills[skillName].level) || 1;
    if (skillLvl < tier.minLvl) return;

    // Stat gate (per EQUIP_STAT_MAP)
    const reqStat = this._equipStatFor(weaponType);
    if ((ps[reqStat] || 0) < (tier.statReq || 0)) return;

    // Coin + resource validation.
    if ((ps.coins || 0) < tier.goldCost) return;
    if (!ps.inventory) ps.inventory = {};
    const resourceKey = wantWw ? ('wood_' + tier.wood) : ('ore_' + tier.oreName + '_ore');
    const have = ps.inventory[resourceKey] || 0;
    const cost = wantWw ? tier.woodCost : tier.oreCost;
    if (have < cost) return;

    // Active slot for the new weapon (matches client logic).
    const slot = (weaponType === 'bow') ? 'rangedWeapon'
               : (weaponType === 'staff') ? 'staffWeapon'
               : 'weapon';

    // Stash full check -- if existing active weapon would need to
    // be stashed but stash is full, reject (matches client where
    // stash.push silently no-ops at cap).  Future: auto-sell oldest.
    const current = ps[slot];
    if (current) {
      if (!Array.isArray(ps.weaponStash)) ps.weaponStash = [];
      if (ps.weaponStash.length >= this.WEAPON_STASH_CAP) return;
    }

    // Apply: consume resources, mint new weapon, swap old to stash.
    ps.inventory[resourceKey] -= cost;
    if (ps.inventory[resourceKey] <= 0) delete ps.inventory[resourceKey];
    ps.coins -= tier.goldCost;

    if (current) {
      ps.weaponStash.push(current);
    }
    ps[slot] = {
      type: weaponType,
      tier: 'common',
      tierMult: tier.tierMult,
      element1: null,
      element2: null,
      isVolatile: false,
      // Name is built client-side from display label; server stores
      // gearBase so the client can reconstruct.
      name: tierKey + ' ' + weaponType,
      gearBase: wantWw ? ('ww_' + tierKey) : tierKey,
      reforgeBonus: null,
      hardenBonus: null,
    };

    // Crafting XP -- mirrors client at the forge sites:
    //   blacksmithing: tier.minLvl * 5
    //   woodworking:   tier.minLvl * 5  (same formula)
    this._addLifeSkillXp(ps, skillName, (tier.minLvl || 1) * 5);

    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) this._sendPlayerState(ws, session.id);
  }

  // Unequip an active equipment slot.  Weapons move to stash (if
  // room); armor/shield/amulet simply null out since they don't have
  // a stash today.  Closes the cheat where a client unequips locally
  // and gets "lost" gear that server still thinks is equipped --
  // future damage/def math would diverge from client view otherwise.
  _handleUnequipRequest(session, payload) {
    if (!session || !session.id) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    const { slot } = payload || {};
    if (!this._isValidEquipSlot(slot)) return;
    const current = ps[slot];
    if (!current) return;
    // Weapons go to stash; armor/shield/amulet just null out.
    if (slot === 'weapon' || slot === 'rangedWeapon' || slot === 'staffWeapon') {
      if (!Array.isArray(ps.weaponStash)) ps.weaponStash = [];
      if (ps.weaponStash.length >= this.WEAPON_STASH_CAP) return; // stash full -- reject
      ps.weaponStash.push(current);
    }
    ps[slot] = null;
    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) this._sendPlayerState(ws, session.id);
  }

  _handleEquipRequest(session, payload) {
    if (!session || !session.id) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    const { stashIdx, slot } = payload || {};
    if (!this._isValidEquipSlot(slot)) return;
    if (!Number.isInteger(stashIdx) || stashIdx < 0) return;
    if (!Array.isArray(ps.weaponStash)) ps.weaponStash = [];
    if (stashIdx >= ps.weaponStash.length) return;
    // Swap stash entry with current active slot.  If active slot
    // empty, the stash item moves in and the stash entry becomes
    // null (which we then splice out so stash stays compact).
    const stashItem = ps.weaponStash[stashIdx];
    // Guard against a stash entry that is null/undefined (could
    // happen from a corrupted stored blob from before the
    // splice-on-empty logic existed).  Without this, a cheater
    // could equip a "null" stash entry to wipe the active slot.
    if (!stashItem) return;
    const activeItem = ps[slot] || null;
    ps[slot] = stashItem;
    if (activeItem) {
      ps.weaponStash[stashIdx] = activeItem;
    } else {
      ps.weaponStash.splice(stashIdx, 1);
    }
    // Sanity cap so stash can't grow past the client-side limit even
    // if a cheater somehow inflates it via prior bootstrap.
    if (ps.weaponStash.length > this.WEAPON_STASH_CAP) {
      ps.weaponStash.length = this.WEAPON_STASH_CAP;
    }
    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) this._sendPlayerState(ws, session.id);
  }

  // ═══ Cooking recipes (multi-ingredient -> buff or heal) ═══
  //
  // Mirrors COOKING_RECIPES in src/data/gameSystems.js.  Client sends
  // cook_recipe { recipeIdx } when the player triggers a recipe from
  // either of the two onClick sites (cooking panel + farm food kiosk
  // -- BroTown.jsx ~18981 / ~29762).  Server validates ingredient
  // ownership via substring match (same as client), consumes the
  // ingredients, and applies the buff or heal.
  //
  // Buff state is tracked on ps._buffs as { regen: endsAt, resist:
  // endsAt, damage: endsAt, all: endsAt, hp: endsAt, mana: endsAt }
  // -- only the buffs that affect server-computed values get applied
  // server-side (regen in _tickPlayerRegen, resist in _applyDamage,
  // hp overheal cap in _tickPlayerRegen).  damage / all / spd buffs
  // affect outgoing damage + move speed which the server doesn't
  // currently enforce -- those flags are tracked for future use and
  // emitted in player_state so the client can render correctly.
  //
  // Closes the cheat surface for when recipe buffs get wired up:
  // currently no recipe has buff:'heal' so the heal path is dead
  // code on the client, but if it gets added later the worker
  // already handles it safely.
  _getCookingRecipe(idx) {
    // Mirror of COOKING_RECIPES from src/data/gameSystems.js.  Keep
    // in sync when new recipes ship.  The indices must match the
    // client's array order since the client sends the index.
    const RECIPES = [
      { ingredients: { herb_firebloom: 1 },                          buff: 'regen',  power: 0.02, duration: 60, tier: 1 },
      { ingredients: { herb_rock_vine: 1, herb_cloudpetal: 1 },      buff: 'resist', power: 0.05, duration: 60, tier: 1 },
      { ingredients: { herb_firebloom: 2 },                          buff: 'damage', power: 0.05, duration: 90, tier: 2 },
    ];
    if (!Number.isInteger(idx) || idx < 0 || idx >= RECIPES.length) return null;
    return RECIPES[idx];
  }

  // Match-then-consume helper.  Mirrors the CLIENT's behavior but with
  // a stricter matcher: client uses bare k.includes(type), which would
  // unintentionally match unrelated inventory keys that happen to
  // contain the type string as a substring (e.g. "shard_herb_firebloom"
  // would be consumed by a "herb_firebloom" ingredient).  We restrict
  // matches to k === type OR k === ('cooked_' + type) so only the
  // canonical inventory key (and its cooked variant) is consumed.
  // Client matches more loosely; the divergence means the server may
  // refuse some recipes the client would accept, but that's safer than
  // the inverse.
  _ingredientMatches(invKey, type) {
    return invKey === type || invKey === ('cooked_' + type);
  }

  _consumeIngredient(ps, type, count) {
    if (!ps.inventory) return false;
    let remaining = count;
    // First pass: count availability across matching keys.
    let total = 0;
    for (const [k, v] of Object.entries(ps.inventory)) {
      if (this._ingredientMatches(k, type) && v > 0) total += v;
    }
    if (total < count) return false;
    // Second pass: consume from matching keys until satisfied.
    for (const k of Object.keys(ps.inventory)) {
      if (remaining <= 0) break;
      if (!this._ingredientMatches(k, type) || ps.inventory[k] <= 0) continue;
      const take = Math.min(ps.inventory[k], remaining);
      ps.inventory[k] -= take;
      remaining -= take;
      if (ps.inventory[k] <= 0) delete ps.inventory[k];
    }
    return true;
  }

  _handleCookRecipe(session, payload) {
    if (!session || !session.id) return;
    const { recipeIdx } = payload || {};
    const recipe = this._getCookingRecipe(recipeIdx);
    if (!recipe) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    if (!ps.inventory) ps.inventory = {};

    // First-pass dry-run: confirm ALL ingredients are available
    // before consuming any (so we don't half-consume on a failure).
    for (const [type, count] of Object.entries(recipe.ingredients)) {
      let total = 0;
      for (const [k, v] of Object.entries(ps.inventory)) {
        if (this._ingredientMatches(k, type) && v > 0) total += v;
      }
      if (total < count) return;
    }
    // Second pass: actually consume.
    for (const [type, count] of Object.entries(recipe.ingredients)) {
      this._consumeIngredient(ps, type, count);
    }

    // Apply the recipe effect.  Buffs go onto ps._buffs as endsAt
    // timestamps; heal modifies hp directly.  Duration is seconds
    // in the recipe table, ms on the wire.
    if (!ps._buffs) ps._buffs = {};
    const dur = (recipe.duration || 0) * 1000;
    const endsAt = Date.now() + dur;
    if (recipe.buff === 'heal') {
      if (typeof ps.maxHp !== 'number') ps.maxHp = 100;
      ps.hp = Math.min(ps.maxHp, (ps.hp || 0) + (recipe.power || 0));
    } else if (recipe.buff === 'regen') {
      ps._buffs.regen = endsAt;
    } else if (recipe.buff === 'resist') {
      ps._buffs.resist = endsAt;
    } else if (recipe.buff === 'damage') {
      ps._buffs.damage = endsAt;
    } else if (recipe.buff === 'all') {
      // 'all' buff sets all four sub-buffs.  Mirrors the client at
      // BroTown.jsx ~29766: damage + spd + hp + mana all extended.
      ps._buffs.damage = endsAt;
      ps._buffs.spd = endsAt;
      ps._buffs.hp = endsAt;
      ps._buffs.mana = endsAt;
    }

    // Cooking XP grant -- mirrors addLifeSkillXp on the client.
    this._addLifeSkillXp(ps, 'cooking', (recipe.tier || 1) * 25);

    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) this._sendPlayerState(ws, session.id);
  }

  // Buff-active helpers used in regen + damage paths.  Treat undefined
  // / 0 / past timestamps as inactive.
  _buffActive(ps, name) {
    return !!(ps && ps._buffs && ps._buffs[name] && Date.now() < ps._buffs[name]);
  }

  // ═══ NPC consumables shop (server-authoritative purchase) ═══
  //
  // Client sends shop_purchase { itemId } when the player clicks Buy
  // on the NPC vendor.  Server mirrors the 5-item table (see client at
  // BroTown.jsx ~17905), validates ps.coins >= discounted cost (where
  // discount = min(0.20, ps.influence * 0.002) per §2.6), deducts coins,
  // applies the effect to the appropriate playerState field, persists,
  // and emits player_state.
  //
  // Closes the "buy infinite potions" + "buy without spending coins"
  // cheats: server is the only writer for coins/inventory/pools after
  // a purchase.  The dmgBuff effect is transient client-only (_dmgBuff
  // timer); no server tracking needed for that one.
  _getShopItem(itemId) {
    const TABLE = {
      cookedMinnow:  { cost: 8,  effect: 'healFish', power: 23 },
      basicTrap:     { cost: 20, effect: 'trap' },
      staminaSalts:  { cost: 12, effect: 'stamina', power: 60 },
      manaShard:     { cost: 18, effect: 'mana', power: 40 },
      whetstone:     { cost: 35, effect: 'dmgBuff' },
    };
    return TABLE[itemId] || null;
  }

  _handleShopPurchase(session, payload) {
    if (!session || !session.id) return;
    const { itemId } = payload || {};
    if (typeof itemId !== 'string') return;
    const item = this._getShopItem(itemId);
    if (!item) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    // §2.6 Influence discount — 0.2% per point, max 20%.
    const discount = Math.min(0.20, (ps.influence || 0) * 0.002);
    const finalCost = Math.max(1, Math.floor(item.cost * (1 - discount)));
    if ((ps.coins || 0) < finalCost) return;
    ps.coins -= finalCost;
    // Apply effect.  Pool restores clamp to max; trap grants inventory;
    // dmgBuff is transient client-only (server doesn't track buff timers).
    if (item.effect === 'healFish') {
      if (typeof ps.maxHp !== 'number') ps.maxHp = 100;
      if (typeof ps.hp !== 'number') ps.hp = ps.maxHp;
      ps.hp = Math.min(ps.maxHp, ps.hp + (item.power || 23));
    } else if (item.effect === 'stamina') {
      if (typeof ps.maxStamina !== 'number') ps.maxStamina = 100;
      if (typeof ps.stamina !== 'number') ps.stamina = ps.maxStamina;
      ps.stamina = Math.min(ps.maxStamina, ps.stamina + (item.power || 60));
    } else if (item.effect === 'mana') {
      if (typeof ps.maxMana !== 'number') ps.maxMana = 100;
      if (typeof ps.mana !== 'number') ps.mana = ps.maxMana;
      ps.mana = Math.min(ps.maxMana, ps.mana + (item.power || 40));
    } else if (item.effect === 'trap') {
      if (!ps.inventory) ps.inventory = {};
      ps.inventory.basic_trap = (ps.inventory.basic_trap || 0) + 1;
    }
    // dmgBuff: no-op server-side (transient buff state).
    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) this._sendPlayerState(ws, session.id);
  }

  // ═══ Cooking (raw fish -> cooked / burnt) ═══
  //
  // Client sends cook_request { fishKey, kind } when the cooking
  // minigame finishes.  Server validates the player actually holds the
  // raw fish, consumes 1, and applies the outcome:
  //   kind === 'cooked' -> +1 cooked_<fishKey>, +8 cooking XP
  //   kind === 'burnt'  -> +1 burnt_dust
  // Then persists + emits player_state so the client overwrites its
  // inventory + lifeSkills with the authoritative values.
  //
  // Trusts the client on `kind` (the minigame outcome).  Closing that
  // needs server-side minigame validation -- separate slice.
  _handleCookRequest(session, payload) {
    if (!session || !session.id) return;
    const { fishKey, kind } = payload || {};
    if (typeof fishKey !== 'string' || !fishKey.startsWith('fish_')) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (!ps.inventory) ps.inventory = {};
    if ((ps.inventory[fishKey] || 0) <= 0) return;
    ps.inventory[fishKey] -= 1;
    if (ps.inventory[fishKey] <= 0) delete ps.inventory[fishKey];
    if (kind === 'cooked') {
      const cookedKey = 'cooked_' + fishKey;
      ps.inventory[cookedKey] = (ps.inventory[cookedKey] || 0) + 1;
      this._addLifeSkillXp(ps, 'cooking', 8);
    } else {
      ps.inventory.burnt_dust = (ps.inventory.burnt_dust || 0) + 1;
    }
    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) this._sendPlayerState(ws, session.id);
  }

  _handleNodeStrike(session, payload) {
    if (!session || !session.id) return;
    const { id, zone, accuracy } = payload || {};
    if (!id || !zone) return;
    const list = this.nodes[zone];
    if (!list) return;
    const n = list.find((x) => x.id === id);
    if (!n || !n.alive) return;
    /* Position gate -- player must actually be near the node.  The
       client's minigame wouldn't open without proximity but a
       handcrafted node_strike would; check anyway. */
    const ps = this.playerState[session.id];
    if (!ps || ps.z !== zone || ps.dead || ps.disconnected) return;
    const dx = ps.x - n.x;
    const dy = ps.y - n.y;
    if (dx * dx + dy * dy > this.LOOT_PICKUP_RANGE * this.LOOT_PICKUP_RANGE) return;

    n.alive = false;
    n.respawnAt = Date.now() + this.NODE_RESPAWN_TIME;
    this.dirtyNodes.add(zone);

    /* Apply the inventory grant server-side and persist.  Client used
       to do this in _applyFishingReward / _applyWoodReward /
       _applyMiningReward; now it just sends node_strike with the
       accuracy and waits for the player_state event we emit below.
       Slice 18: rate-limit 'perfect' claims so a cheater can't spam
       perfect-accuracy for the doubled yield + XP. */
    const ratedAccuracy = this._ratedHarvestAccuracy(ps, accuracy);
    const invKey = this._harvestInvKey(n.nodeType, n.tierLvl);
    const yieldQty = this._harvestYieldMult(ratedAccuracy);
    if (!ps.inventory) ps.inventory = {};
    ps.inventory[invKey] = (ps.inventory[invKey] || 0) + yieldQty;

    /* lifeSkill XP -- server applies the XP gain and detects level-up.
       Client used to do this via addLifeSkillXp(R.lifeSkills, ...);
       now it predicts the popup locally but the authoritative
       lifeSkills snapshot rides on the player_state event below. */
    const skillName = this._harvestSkillName(n.nodeType);
    const xpAmt = this._harvestXpForTier(n.tierLvl, ratedAccuracy);
    const { leveled, newLevel } = this._addLifeSkillXp(ps, skillName, xpAmt);

    /* Shard roll -- 33% per successful harvest.  Server-rolled so a
       modified client can't force shard drops.  Goes straight into
       inventory under shard_<zone> keyed off node.zone. */
    const shard = this._rollHarvestShard(n.zoneId || zone);
    if (shard) {
      ps.inventory[shard] = (ps.inventory[shard] || 0) + 1;
    }

    this._saveRpg(session.id, ps);

    /* Push the new authoritative totals to the picker.  Same
       player_state event the loot path uses; client OVERWRITES
       R.coins / R.inventory / R.lifeSkills wholesale on receive. */
    const ws = this._wsBySessionId(session.id);
    if (ws) {
      this._sendPlayerState(ws, session.id);
      /* Non-deterministic feedback the client can't predict on its
         own (shard roll outcome + level-up confirmation): private
         harvest_credit event so the client can fire the appropriate
         floating popups. */
      try {
        ws.send(JSON.stringify({
          type: 'harvest_credit',
          payload: {
            nodeId: id,
            zone,
            skillName,
            xpAmt,
            leveled,
            newLevel,
            shard,
          },
        }));
      } catch (e) {}
    }
  }

  // ═══ Server-authoritative loot ═══
  //
  // The worker owns the ground-loot list per zone.  When a monster
  // dies in _handleMonsterDamage, we compute the contribution-weighted
  // recipients (existing code) and ALSO push a pile object into
  // this.loot[zone] with the total gold, optional skull/shard, and the
  // recipients list.  Pickup is a client request (loot_pickup); the
  // server checks position + recipient + not-already-claimed and emits
  // a private loot_credit to the picker with their share.  Public
  // loot_claimed broadcasts visibility changes; loot_despawn finalises.
  _isRemnantSkullArch(arch) {
    return arch === 'fodder' || arch === 'snowman' || arch === 'fireGoblin' || arch === 'mummy' || arch === 'skeleton';
  }

  _rollShardForKill(zoneId) {
    if (Math.random() >= this.SHARD_DROP_RATE) return null;
    return 'shard_' + zoneId;
  }

  // ═══ Server-authoritative RPG state (coins + inventory) ═══
  //
  // The worker owns each player's coins and inventory.  Loot pickups
  // (and, in future slices, sales / harvest / quest grants) apply
  // increments here, persist to DO storage, and emit a player_state
  // event so the client mirrors the authoritative totals -- a modified
  // client overwriting R.coins locally gets stomped on the next sync.
  //
  // Bootstrap: on a player's first connection to this DO we don't have
  // their state yet, so we read rpgCoins/rpgInventory from the join
  // payload as the initial value.  Cheat surface (one-time, at first
  // connect only); after that the server is the source.

  _invKeyForSkull(skull) {
    if (skull === 'fodder') return 'slime-remnants';
    if (skull === 'fireGoblin') return 'fire-goblin-remnants';
    return skull;
  }

  async _loadRpg(playerId) {
    try {
      const stored = await this.state.storage.get('rpg:' + playerId);
      return stored || null;
    } catch (e) {
      return null;
    }
  }

  // Prune expired buff entries from ps._buffs.  _buffActive treats
  // past timestamps as inactive, but unpruned entries would otherwise
  // accumulate forever (each persisted to storage).  Called from
  // _saveRpg so pruning lands every time we persist.
  _pruneBuffs(ps) {
    if (!ps || !ps._buffs) return;
    const now = Date.now();
    for (const k of Object.keys(ps._buffs)) {
      if (typeof ps._buffs[k] !== 'number' || ps._buffs[k] <= now) {
        delete ps._buffs[k];
      }
    }
  }

  async _saveRpg(playerId, ps) {
    if (!playerId || !ps) return;
    this._pruneBuffs(ps);
    try {
      await this.state.storage.put('rpg:' + playerId, {
        coins: ps.coins || 0,
        inventory: ps.inventory || {},
        lifeSkills: ps.lifeSkills || {},
        level: ps.level || 1,
        xp: ps.xp || 0,
        unspentT2: ps.unspentT2 || 0,
        hp: typeof ps.hp === 'number' ? ps.hp : 100,
        maxHp: typeof ps.maxHp === 'number' ? ps.maxHp : 100,
        stamina: typeof ps.stamina === 'number' ? ps.stamina : 100,
        maxStamina: typeof ps.maxStamina === 'number' ? ps.maxStamina : 100,
        mana: typeof ps.mana === 'number' ? ps.mana : 100,
        maxMana: typeof ps.maxMana === 'number' ? ps.maxMana : 100,
        // Raw stats (clamped to per-level cap by _handleStatsUpdate).
        // Persisted so reconnects don't bootstrap from a freshly-spoofed
        // join payload.  Cheater would need to re-cheat through the
        // clamp on every stats_update.
        power: ps.power || 0,
        vitality: ps.vitality || 0,
        endurance: ps.endurance || 0,
        agility: ps.agility || 0,
        mind: ps.mind || 0,
        ferocity: ps.ferocity || 0,
        elementalMastery: ps.elementalMastery || 0,
        fortification: ps.fortification || 0,
        restoration: ps.restoration || 0,
        influence: ps.influence || 0,
        // Active food buff timers (endsAt timestamps).  Persisted so
        // they survive reconnect.  Expired entries get pruned lazily
        // by _buffActive checks; no need to clean on save.
        _buffs: ps._buffs || {},
        // Equipment slots.  Stored as opaque objects the client
        // provided; server doesn't compute weapon stats from these
        // yet (separate slice).  Validating ownership on sell /
        // marketplace flows is the immediate cheat closure.
        weapon: ps.weapon || null,
        rangedWeapon: ps.rangedWeapon || null,
        staffWeapon: ps.staffWeapon || null,
        activeSlot: ps.activeSlot || 'melee',
        armor: ps.armor || null,
        shield: ps.shield || null,
        amulet: ps.amulet || null,
        weaponStash: Array.isArray(ps.weaponStash) ? ps.weaponStash.slice(0, this.WEAPON_STASH_CAP) : [],
        // Quest state (slice 17).  Chain progression + flags +
        // kill counters.  Server validates accept/turn-in state
        // transitions but currently trusts the client's claim
        // that the underlying criteria are met -- see comments
        // on _handleQuestAccept / _handleQuestTurnIn.
        _quests: ps._quests || {},
        _questFlags: ps._questFlags || {},
        _questKills: ps._questKills || {},
        achievementPoints: ps.achievementPoints || 0,
        // Slice 18 rate-limit history.  Persisted so a cheater
        // can't reset the 60-second window by reconnecting (which
        // would otherwise let them claim 'perfect' indefinitely
        // by cycling the WS connection between batches).
        _perfectHistory: Array.isArray(ps._perfectHistory) ? ps._perfectHistory : [],
      });
    } catch (e) {}
  }

  _sendPlayerState(ws, playerId) {
    const ps = this.playerState[playerId];
    if (!ps || !ws) return;
    try {
      ws.send(JSON.stringify({
        type: 'player_state',
        payload: {
          coins: ps.coins || 0,
          inventory: ps.inventory || {},
          lifeSkills: ps.lifeSkills || {},
          level: ps.level || 1,
          xp: ps.xp || 0,
          unspentT2: ps.unspentT2 || 0,
          hp: typeof ps.hp === 'number' ? ps.hp : (ps.maxHp || 100),
          maxHp: typeof ps.maxHp === 'number' ? ps.maxHp : 100,
          stamina: typeof ps.stamina === 'number' ? ps.stamina : (ps.maxStamina || 100),
          maxStamina: typeof ps.maxStamina === 'number' ? ps.maxStamina : 100,
          mana: typeof ps.mana === 'number' ? ps.mana : (ps.maxMana || 100),
          maxMana: typeof ps.maxMana === 'number' ? ps.maxMana : 100,
          // Active food buff timers.  Client renders the buff icons +
          // computes its own multipliers; server's view is authoritative
          // for the timer (cheater can't extend by writing _dmgBuff =
          // Infinity locally, since the next player_state clobbers).
          _buffs: ps._buffs || {},
          // Equipment slots.  Worker is authoritative for ownership;
          // client renders from these on player_state arrival.
          weapon: ps.weapon || null,
          rangedWeapon: ps.rangedWeapon || null,
          staffWeapon: ps.staffWeapon || null,
          activeSlot: ps.activeSlot || 'melee',
          armor: ps.armor || null,
          shield: ps.shield || null,
          amulet: ps.amulet || null,
          weaponStash: Array.isArray(ps.weaponStash) ? ps.weaponStash.slice(0, this.WEAPON_STASH_CAP) : [],
          // Quest state mirror (slice 17).
          _quests: ps._quests || {},
          _questFlags: ps._questFlags || {},
          _questKills: ps._questKills || {},
          achievementPoints: ps.achievementPoints || 0,
        },
      }));
    } catch (e) {}
  }

  // ═══ HP store + damage application (server-authoritative) ═══
  //
  // Server owns current hp; clamps to [0, maxHp].  Damage flows through
  // _applyDamage which mirrors the client formula at BroTown.jsx:2655.
  // Defense (def) + amulet hpRegen + restoration are session-only fields
  // pushed by the client via stats_update whenever recalcDerived runs
  // (equipment / stat-allocation / level change).  Cheater claiming
  // def=999 makes themselves tanky, but they're a separate cheat
  // surface from "infinite-heal R.hp = 99999" which this slice closes.
  _applyDamage(ps, rawDmg, isBlock) {
    if (!ps) return 0;
    const def = ps.def || 0;
    const r = Math.max(1, Math.round(rawDmg || 0));
    let dmgTaken = isBlock ? 0 : Math.max(1, Math.ceil(r - def * 0.3));
    // Resist buff (cooking recipe with buff:'resist', power 0.05 = 5%
    // reduction).  Cooking recipe power values are stored as the
    // fractional reduction; mirror the client's intent here.
    if (dmgTaken > 0 && this._buffActive(ps, 'resist')) {
      dmgTaken = Math.max(1, Math.ceil(dmgTaken * (1 - 0.05)));
    }
    if (typeof ps.maxHp !== 'number') ps.maxHp = 100;
    if (typeof ps.hp !== 'number') ps.hp = ps.maxHp;
    ps.hp = Math.max(0, ps.hp - dmgTaken);
    ps.lastDamageAt = Date.now();
    return dmgTaken;
  }

  // Apply stats_update payload to playerState.  Client sends after
  // every recalcDerived (BroTown.jsx mutation sites listed in the plan).
  // Clamps current hp to the new maxHp so re-derives that shrink the
  // pool don't leave hp > maxHp.
  // ═══ Stat validation (clamp raw stats to per-level cap) ═══
  //
  // Without this, a client could push stats_update { maxHp: 99999 } and
  // the worker would believe it -- effectively giving themselves an
  // infinite HP bar.  We close this by tracking the 10 raw stats
  // (vit / end / mind / power / etc.) ourselves, clamping each to a
  // per-level cap, and computing maxHp / maxStamina / maxMana from the
  // formulas in src/data/gameSystems.js (calcMaxHp / Stam / Mana).
  //
  // Cap formula: level * 10 + 20.  Each level grants 5 T2 stat points
  // (one stat could legitimately reach level*5+1 just from T2), plus
  // T1 use-trained increments, plus amulet stat bonuses.  level*10+20
  // is ~2x the realistic per-stat ceiling -- generous enough for legit
  // play (preserves T1 + amulet contributions), tight enough to block
  // R.vit = 99999 cheats.
  //
  // Client's pushed maxHp / maxStamina / maxMana are IGNORED -- the
  // worker computes its own from the clamped raw stats.
  _statCap(level) {
    return Math.max(20, (level || 1) * 10 + 20);
  }

  _clampStat(value, level) {
    const cap = this._statCap(level);
    return Math.max(0, Math.min(cap, Math.floor(value || 0)));
  }

  _calcMaxHp(level, vitality) {
    return 100 + ((level || 1) - 1) * 12 + (vitality || 0) * 10;
  }

  _calcMaxStamina(endurance) {
    return Math.floor(100 + (endurance || 0) * 3);
  }

  _calcMaxMana(mind) {
    return Math.floor(100 + (mind || 0) * 3.5);
  }

  _recomputeMaxes(ps) {
    if (!ps) return;
    const lvl = ps.level || 1;
    const oldMaxHp = ps.maxHp || 100;
    const oldMaxStam = ps.maxStamina || 100;
    const oldMaxMana = ps.maxMana || 100;
    ps.maxHp = this._calcMaxHp(lvl, ps.vitality || 0);
    ps.maxStamina = this._calcMaxStamina(ps.endurance || 0);
    ps.maxMana = this._calcMaxMana(ps.mind || 0);
    // Clamp current values into the new ranges.
    if (typeof ps.hp !== 'number') ps.hp = ps.maxHp;
    ps.hp = Math.min(ps.hp, ps.maxHp);
    if (typeof ps.stamina !== 'number') ps.stamina = ps.maxStamina;
    ps.stamina = Math.min(ps.stamina, ps.maxStamina);
    if (typeof ps.mana !== 'number') ps.mana = ps.maxMana;
    ps.mana = Math.min(ps.mana, ps.maxMana);
  }

  _handleStatsUpdate(session, payload) {
    if (!session || !session.id) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    const lvl = ps.level || 1;
    // Raw stats: accept client value, clamp to per-level cap.  Server
    // computes its own max values from these and ignores any maxHp /
    // maxStamina / maxMana the client tries to push.
    const RAW_STATS = ['power', 'vitality', 'endurance', 'agility', 'mind',
      'ferocity', 'elementalMastery', 'fortification', 'restoration', 'influence'];
    let statsChanged = false;
    for (const s of RAW_STATS) {
      if (typeof payload[s] === 'number') {
        const clamped = this._clampStat(payload[s], lvl);
        if (ps[s] !== clamped) {
          ps[s] = clamped;
          statsChanged = true;
        }
      }
    }
    if (statsChanged) {
      this._recomputeMaxes(ps);
    }
    // Session-only equipment-derived values flow from client but are
    // capped to per-level bounds.  Without a cap, a cheater can push
    // def: 999999 and take 1 damage forever (since _applyDamage's
    // `max(1, ceil(r - def * 0.3))` floors at 1).  Same risk for
    // amulet regen mults (60k HP/regen tick).
    //
    // def cap: max armor tier mult is 5 + endurance contribution. At
    // level N, max endurance = level*10+20, contributing 0.5x.  Max
    // armor.tierMult = 5, contributing 3x.  So legit max def = (level*10+20)*0.5 + 15.
    // Cap at 4x that to leave headroom for unknown equipment additions.
    const defCap = lvl * 20 + 100;
    if (typeof payload.def === 'number') {
      ps.def = Math.max(0, Math.min(defCap, payload.def));
    }
    // Amulet regen mults are percentages.  Real amulets cap around 30%
    // per tier; 100% is double, well above any realistic stack.
    if (typeof payload.amuletHpRegen === 'number') {
      ps.amuletHpRegen = Math.max(0, Math.min(100, payload.amuletHpRegen));
    }
    if (typeof payload.amuletStaminaRegen === 'number') {
      ps.amuletStaminaRegen = Math.max(0, Math.min(100, payload.amuletStaminaRegen));
    }
    // Persist (raw stats + pool values get carried via _saveRpg).
    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) this._sendPlayerState(ws, session.id);
  }

  // ═══ Ability cost gating (server-authoritative stamina / mana) ═══
  //
  // Client sends ability_use { type, tier? } when the player triggers
  // a stamina-/mana-costing action.  Server computes the cost from
  // ps.maxStamina / hardcoded swipe ramp (mirrors client constants),
  // validates sufficient pool, deducts, and emits player_state.  A
  // separate ability_rejected event flies back when the pool is empty
  // so the client can surface "Not enough stamina!" without waiting on
  // the player_state diff.
  //
  // Closes the "infinite-dodge" / "infinite-stamina write" cheat:
  // server is the only writer for ps.stamina/mana, so a modified
  // client that sets R.stamina = 99999 gets stomped on the next
  // player_state.  Client still predicts the deduction locally for
  // snappy UX (the dash animates immediately); server's value wins.
  _abilityCost(ps, type, tier) {
    if (!ps) return 0;
    if (type === 'dodge')   return Math.ceil((ps.maxStamina || 100) * 0.20);
    if (type === 'lunge')   return Math.ceil((ps.maxStamina || 100) * 0.25);
    if (type === 'retreat') return Math.ceil((ps.maxStamina || 100) * 0.20);
    if (type === 'swipe')   return 15 + Math.max(0, Math.min(3, tier || 0)) * 3;
    return 0;
  }

  _abilityPool(type) {
    if (type === 'swipe') return 'mana';
    return 'stamina';
  }

  _handleAbilityUse(session, payload) {
    if (!session || !session.id) return;
    const { type, tier } = payload || {};
    if (type !== 'dodge' && type !== 'lunge' && type !== 'retreat' && type !== 'swipe') return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    const cost = this._abilityCost(ps, type, tier);
    const pool = this._abilityPool(type);
    const have = (pool === 'mana') ? (ps.mana || 0) : (ps.stamina || 0);
    const ws = this._wsBySessionId(session.id);
    if (have < cost) {
      if (ws) {
        try {
          ws.send(JSON.stringify({
            type: 'ability_rejected',
            payload: { type, pool, cost, have },
          }));
        } catch (e) {}
      }
      return;
    }
    if (pool === 'mana') {
      ps.mana = Math.max(0, have - cost);
    } else {
      ps.stamina = Math.max(0, have - cost);
    }
    this._saveRpg(session.id, ps);
    if (ws) this._sendPlayerState(ws, session.id);
  }

  // Player death.  Marks the player as dying for the respawn window;
  // _tickPlayerRespawn flips them back when respawnAt elapses.
  _handlePlayerDeath(ps, playerId, cause) {
    if (!ps || ps.dying) return;
    ps.dying = true;
    ps.dead = true;
    ps.respawnAt = Date.now() + 5000;
    const ws = this._wsBySessionId(playerId);
    if (ws) {
      try {
        ws.send(JSON.stringify({
          type: 'player_died',
          payload: { cause: cause || 'unknown', respawnInMs: 5000 },
        }));
      } catch (e) {}
    }
  }

  // Walk active players for respawn-ready dying players.  Resets hp
  // to max and emits player_respawned + player_state so the client
  // teleports to town and clears its death state.  Cheap; runs once
  // per tick alongside _tickMonsters.
  _tickPlayerRespawn() {
    const now = Date.now();
    for (const [id, ps] of Object.entries(this.playerState)) {
      if (!ps.dying) continue;
      if (now < (ps.respawnAt || 0)) continue;
      ps.hp = ps.maxHp || 100;
      ps.stamina = ps.maxStamina || 100;
      ps.mana = ps.maxMana || 100;
      ps.dying = false;
      ps.dead = false;
      ps.respawnAt = 0;
      ps.z = 'town';
      ps.lastDamageAt = 0;
      this._saveRpg(id, ps);
      const ws = this._wsBySessionId(id);
      if (ws) {
        try {
          ws.send(JSON.stringify({
            type: 'player_respawned',
            payload: { zone: 'town' },
          }));
        } catch (e) {}
        this._sendPlayerState(ws, id);
      }
    }
  }

  // Pool regen tick + shield drain.  Runs every 30 server ticks
  // (~670 ms at TICK_RATE=22) for all three pools:
  //
  //   HP:
  //     OOC:        ceil(maxHp * 0.001 * restMult * amuletMult) * 10
  //     In-combat:  ceil(maxHp * 0.0005) * 6
  //   Stamina:
  //     Always:     ~7/tick (matches client's 10/sec at 60 fps),
  //                 * (1 + amuletStaminaRegen/100)
  //     Override:   when ps.blocking, drain 5/tick instead of regenning
  //                 (mirrors client's 0.167/frame * 30 frames shield drain)
  //   Mana:
  //     OOC (>2s):  maxMana * 0.018/tick (~2.7%/sec)
  //     In-combat:  maxMana * 0.007/tick (~1%/sec)
  //
  // Only emits player_state when at least one pool actually changed.
  // Rate calibration is approximate -- see plan file "Regen rate math".
  _tickPlayerRegen() {
    const now = Date.now();
    for (const [id, ps] of Object.entries(this.playerState)) {
      if (!ps || ps.dying || ps.dead || ps.disconnected) continue;
      if (typeof ps.hp !== 'number' || typeof ps.maxHp !== 'number') continue;
      const ooc = (now - (ps.lastDamageAt || 0)) > 5000;
      const oocMana = (now - (ps.lastDamageAt || 0)) > 2000;
      let changed = false;

      // HP regen.  Cooking recipe regen buff (1.3x mult) and hp buff
      // (1.25x maxHp overheal cap) layered on top of the existing
      // restoration + amulet multipliers.
      const regenBuffActive = this._buffActive(ps, 'regen');
      const hpBuffActive = this._buffActive(ps, 'hp');
      const effectiveMaxHp = hpBuffActive ? Math.floor(ps.maxHp * 1.25) : ps.maxHp;
      if (ps.hp < effectiveMaxHp) {
        let heal;
        if (ooc) {
          const restMult = 1 + (ps.restoration || 0) * 0.001;
          const amuletMult = 1 + (ps.amuletHpRegen || 0) / 100;
          const buffMult = regenBuffActive ? 1.3 : 1.0;
          heal = Math.max(1, Math.ceil(ps.maxHp * 0.001 * restMult * amuletMult * buffMult)) * 10;
        } else {
          heal = Math.max(1, Math.ceil(ps.maxHp * 0.0005)) * 6;
        }
        const beforeHp = ps.hp;
        ps.hp = Math.min(effectiveMaxHp, ps.hp + heal);
        if (ps.hp !== beforeHp) changed = true;
      }

      // Stamina: shield drain takes priority over regen.  When blocking,
      // drain ~5/tick and auto-release at 0 (mirrors client behavior at
      // BroTown.jsx:9370 -- 0.167 stamina/frame at 60 fps).
      if (typeof ps.maxStamina === 'number' && typeof ps.stamina === 'number') {
        if (ps.blocking && ps.stamina > 0) {
          const beforeSt = ps.stamina;
          ps.stamina = Math.max(0, ps.stamina - 5);
          if (ps.stamina !== beforeSt) changed = true;
          if (ps.stamina <= 0) {
            // Auto-release shield to match client's drop-at-0 behavior.
            ps.blocking = false;
          }
        } else if (ps.stamina < ps.maxStamina) {
          const stAmuletMult = 1 + (ps.amuletStaminaRegen || 0) / 100;
          const stRestMult = 1 + (ps.restoration || 0) * 0.001;
          const stHeal = Math.max(1, Math.ceil(7 * stAmuletMult * stRestMult));
          const beforeSt = ps.stamina;
          ps.stamina = Math.min(ps.maxStamina, ps.stamina + stHeal);
          if (ps.stamina !== beforeSt) changed = true;
        }
      }

      // Mana.  manaBuff (1.3x regen mult) layered on top of restoration.
      const manaBuffActive = this._buffActive(ps, 'mana');
      if (typeof ps.maxMana === 'number' && typeof ps.mana === 'number' && ps.mana < ps.maxMana) {
        const restMult = 1 + (ps.restoration || 0) * 0.001;
        const buffMult = manaBuffActive ? 1.3 : 1.0;
        const rate = oocMana ? 0.018 : 0.007;
        const manaHeal = Math.max(1, Math.ceil(ps.maxMana * rate * restMult * buffMult));
        const beforeMn = ps.mana;
        ps.mana = Math.min(ps.maxMana, ps.mana + manaHeal);
        if (ps.mana !== beforeMn) changed = true;
      }

      if (changed) {
        this._saveRpg(id, ps);
        const ws = this._wsBySessionId(id);
        if (ws) this._sendPlayerState(ws, id);
      }
    }
  }

  // Pile shape (server-side, full):
  //   { lootId, zone, x, y, coins, skull, shard, recipients,
  //     shares: {pid: number}, killerName, ts, inventoryClaimed,
  //     claimedBy: {pid: true} }
  _spawnLootForKill(zone, monster, killerSessionId, recipients, shares) {
    const lootId = 'mk-' + monster.id;
    const skull = this._isRemnantSkullArch(monster.arch) ? monster.arch : null;
    const shard = this._rollShardForKill(zone);
    if (!skull && (!recipients || recipients.length === 0) && monster.gold <= 0 && !shard) {
      // Nothing of value would drop -- skip the pile entirely.
      return null;
    }
    const killerSession = this._sessionById(killerSessionId);
    const killerName = (killerSession && killerSession.name) || 'Player';
    const pile = {
      lootId,
      zone,
      x: monster.x,
      y: monster.y,
      coins: monster.gold || 0,
      skull,
      shard,
      recipients: recipients.slice(),
      shares: { ...shares },
      killerName,
      ts: Date.now(),
      inventoryClaimed: false,
      claimedBy: {},
    };
    if (!this.loot[zone]) this.loot[zone] = [];
    this.loot[zone].push(pile);
    return pile;
  }

  // Serialize a pile for the wire.  Strips server-only fields
  // (claimedBy) and keeps just what clients need to render + decide
  // visual state.  inventoryClaimed is part of the wire because
  // late-joiners + zone-change syncs need it.
  _serializePile(p) {
    return {
      lootId: p.lootId,
      zone: p.zone,
      x: p.x, y: p.y,
      coins: p.coins,
      skull: p.skull,
      shard: p.shard,
      recipients: p.recipients,
      shares: p.shares,
      killerName: p.killerName,
      ts: p.ts,
      inventoryClaimed: p.inventoryClaimed,
    };
  }

  _zoneLootForWire(zone) {
    const list = this.loot[zone] || [];
    return list.map((p) => this._serializePile(p));
  }

  _sessionById(sessionId) {
    for (const [, s] of this.sessions) {
      if (s.id === sessionId) return s;
    }
    return null;
  }

  _wsBySessionId(sessionId) {
    for (const [ws, s] of this.sessions) {
      if (s.id === sessionId) return ws;
    }
    return null;
  }

  _despawnLoot(zone, lootId) {
    const list = this.loot[zone];
    if (!list) return;
    const idx = list.findIndex((p) => p.lootId === lootId);
    if (idx < 0) return;
    list.splice(idx, 1);
    this.eventBuffer.push({
      type: 'loot_despawn',
      payload: { lootId, zone },
    });
  }

  _tickLoot() {
    const now = Date.now();
    for (const zoneId of Object.keys(this.loot)) {
      const list = this.loot[zoneId];
      if (!list) continue;
      // Walk back-to-front so splice is safe.
      for (let i = list.length - 1; i >= 0; i--) {
        const p = list[i];
        if (now - p.ts > this.LOOT_EXPIRY_MS) {
          list.splice(i, 1);
          this.eventBuffer.push({
            type: 'loot_despawn',
            payload: { lootId: p.lootId, zone: zoneId },
          });
        }
      }
    }
  }

  _handleLootPickup(session, payload) {
    if (!session || !session.id) return;
    const { lootId, zone } = payload || {};
    if (!lootId || !zone) return;
    const list = this.loot[zone];
    if (!list) return;
    const pile = list.find((p) => p.lootId === lootId);
    if (!pile) return;

    // Already claimed this pile?  Per-player single claim for gold;
    // inventory is single-claim across the pile.
    if (pile.claimedBy[session.id]) return;

    // Recipient gate.
    if (!pile.recipients.includes(session.id)) return;

    // Range gate -- player must be near the pile per server-tracked
    // position.  Slightly looser than the client's 20 px to absorb
    // the 100 ms+ position lag between move events.
    const ps = this.playerState[session.id];
    if (!ps || ps.z !== zone || ps.dead || ps.disconnected) return;
    const dx = ps.x - pile.x;
    const dy = ps.y - pile.y;
    if (dx * dx + dy * dy > this.LOOT_PICKUP_RANGE * this.LOOT_PICKUP_RANGE) return;

    // Compute the player's authorized share.
    const share = pile.shares[session.id] || 0;
    const coinsForMe = Math.round(pile.coins * share);

    // First picker also gets the one-of inventory drop.
    let skullForMe = null;
    let shardForMe = null;
    let inventoryClaimedNow = false;
    if (!pile.inventoryClaimed) {
      if (pile.skull || pile.shard) inventoryClaimedNow = true;
      skullForMe = pile.skull || null;
      shardForMe = pile.shard || null;
      pile.inventoryClaimed = true;
    }
    pile.claimedBy[session.id] = true;

    // Apply the grant to server-tracked playerState (the authoritative
    // store) BEFORE we emit the credit -- a cheating client that tries
    // to manipulate the local R.coins value will get overwritten on
    // the next player_state event we send.  Persist async; the in-
    // memory state is what subsequent operations read.  Reusing the
    // `ps` variable from the range check above.
    ps.coins = (ps.coins || 0) + coinsForMe;
    if (skullForMe) {
      if (!ps.inventory) ps.inventory = {};
      const invKey = this._invKeyForSkull(skullForMe);
      ps.inventory[invKey] = (ps.inventory[invKey] || 0) + 1;
    }
    if (shardForMe) {
      if (!ps.inventory) ps.inventory = {};
      ps.inventory[shardForMe] = (ps.inventory[shardForMe] || 0) + 1;
    }
    this._saveRpg(session.id, ps);

    // Private credit to the picker -- this is the authoritative grant.
    // Goes direct via ws.send (not the room broadcast) so other clients
    // can't see another player's per-share amount.  The accompanying
    // player_state (sent right after) carries the new authoritative
    // totals so the client can overwrite its local rpg state -- the
    // loot_credit values are kept here for popup display only.
    const ws = this._wsBySessionId(session.id);
    if (ws) {
      try {
        ws.send(JSON.stringify({
          type: 'loot_credit',
          payload: {
            lootId,
            zone,
            coins: coinsForMe,
            skull: skullForMe,
            shard: shardForMe,
          },
        }));
      } catch (e) {}
      this._sendPlayerState(ws, session.id);
    }

    // Public broadcast: visual state changed (skull/shard removed from
    // the rendered pile, picker logged for future "X claimed" feedback
    // if the client wants it).
    this.eventBuffer.push({
      type: 'loot_claimed',
      payload: { lootId, zone, byPlayer: session.id, inventoryClaimedNow },
    });

    // If every recipient has now claimed, the pile is fully spent --
    // despawn so watchers stop seeing it.
    if (Object.keys(pile.claimedBy).length >= pile.recipients.length) {
      this._despawnLoot(zone, lootId);
    }
  }

  // Process player damage to a monster
  // Weapon-aware damage cap.  Replaces the prior level-only cap
  // ((level+5)*100) with a tighter bound computed from the attacker's
  // actual equipped weapon + power + ferocity (all server-tracked
  // since slices 12 / stat-validation).  Closes the "claim huge
  // damage to one-shot tough monsters" cheat with much less false-
  // positive headroom -- a level 1 player with a wood weapon can no
  // longer claim 600 dmg, only ~350.
  //
  // Formula mirrors calcWeaponDmg in src/data/gameSystems.js:
  //   base = (WEAPON_TYPES[type].base + power * 0.8) * weapon.tierMult
  // Multiplied by crit cap (1.75 + ferocity * 0.0008) and a generous
  // 5x "combo + status + amulet + lunge" boost to cover the legit
  // upper bound without rejecting real hits.
  _maxWeaponDmg(ps) {
    if (!ps) return 0;
    const candidates = [ps.weapon, ps.rangedWeapon, ps.staffWeapon].filter(Boolean);
    if (candidates.length === 0) return 30; // fists fallback
    let max = 0;
    const power = ps.power || 0;
    for (const w of candidates) {
      const base = (this._weaponBase(w.type) + power * 0.8) * (w.tierMult || 1);
      if (base > max) max = base;
    }
    return max;
  }

  _maxDmgForAttacker(ps) {
    if (!ps) return 100;
    const maxWpn = this._maxWeaponDmg(ps);
    const critMult = 1.75 + (ps.ferocity || 0) * 0.0008;
    const comboBoost = 5; // covers combo + status amplifier + amulet elemDmg + lunge mult
    return Math.max(100, Math.ceil(maxWpn * critMult * comboBoost));
  }

  _handleMonsterDamage(session, payload) {
    const { monsterId, zone, dmg, isCrit, element } = payload;
    if (!monsterId || !zone || !dmg) return;
    const monsters = this.monsters[zone];
    if (!monsters) return;
    const m = monsters.find(x => x.id === monsterId);
    if (!m || !m.alive) return;

    // Apply damage. Clamp the credited amount to the monster's remaining
    // HP so the overkill on the final blow doesn't inflate the killer's
    // contribution share (GDD §7: DPS = damage / monster_max_hp).
    // Also clamp the incoming value to the per-level cap so a cheater
    // can't claim 99999 damage to one-shot tough monsters.
    const attackerPs = this.playerState[session.id];
    // Weapon-aware cap (slice 16): replaces level-only with formula
    // computed from server-tracked weapon + power + ferocity.
    const dmgCap = this._maxDmgForAttacker(attackerPs);
    const rawDmg = Math.max(1, Math.min(dmgCap, Math.round(dmg)));
    const actualDmg = Math.min(rawDmg, Math.max(0, m.hp));
    // Subtract actualDmg (capped at remaining hp) so m.hp doesn't go
    // negative on overkill -- otherwise the broadcast hpPct goes < 0
    // and any subsequent code reading m.hp sees a nonsensical value.
    m.hp -= actualDmg;

    // Track per-player damage contribution for the kill-time share.
    // dmgByPlayer is created lazily so existing monster snapshots
    // without it stay compatible.
    if (!m.dmgByPlayer) m.dmgByPlayer = {};
    m.dmgByPlayer[session.id] = (m.dmgByPlayer[session.id] || 0) + actualDmg;

    this.dirtyMonsters.add(zone);

    // Push damage event for all clients to see
    this.eventBuffer.push({
      type: 'monster_hit',
      payload: {
        monsterId: m.id,
        zone,
        dmg: actualDmg,
        isCrit: !!isCrit,
        attackerId: session.id,
        hpPct: Math.max(0, m.hp / m.maxHp),
      }
    });

    // Kill check
    if (m.hp <= 0) {
      m.alive = false;
      m.respawnAt = Date.now() + this.RESPAWN_TIME;

      // GDD §7 — contribution-weighted XP/gold distribution.
      // DPS share = dmgByPlayer[id] / m.maxHp.  We also require the
      // recipient to be alive, connected, and still in the kill zone
      // (anyone who tagged the monster then walked away or died forfeits).
      const contributions = m.dmgByPlayer || {};
      const totalShareDenom = Object.values(contributions).reduce((a, b) => a + b, 0) || 1;
      const xpRecipients = [];
      const goldRecipients = [];
      const shares = {};
      for (const [pid, contributed] of Object.entries(contributions)) {
        const ps = this.playerState[pid];
        if (!ps || ps.dead || ps.disconnected || ps.z !== zone) continue;
        const share = contributed / totalShareDenom;
        shares[pid] = share;
        xpRecipients.push(pid);
        // GDD §7: gold cutoff at 0.05 contribution; below → no gold
        if (share >= 0.05) goldRecipients.push(pid);
      }
      // Fallback: if every contributor dropped out (dead/left zone),
      // fall back to last-hit credit so the loot doesn't vanish.
      if (xpRecipients.length === 0) {
        xpRecipients.push(session.id);
        goldRecipients.push(session.id);
        shares[session.id] = 1.0;
      }

      this.eventBuffer.push({
        type: 'monster_kill',
        payload: {
          monsterId: m.id,
          zone,
          killerId: session.id,
          xp: m.xp,
          gold: m.gold,
          level: m.level,
          arch: m.arch,
          element: m.element,
          x: m.x,
          y: m.y,
          // GDD §7 contribution-weighted recipients.  Each gets
          // xp_per_player = m.xp * shares[id], gold_per_player =
          // m.gold * shares[id] if their share >= 0.05.
          recipients: xpRecipients,
          goldRecipients,
          shares,
        }
      });

      // Server-authoritative loot drop.  The pile lives on the worker;
      // clients render it from the broadcast and request pickup via
      // loot_pickup.  Cheaters can't credit themselves coins/inventory
      // without a server-emitted loot_credit acknowledging a valid
      // pickup request (range + recipient + single-claim gates in
      // _handleLootPickup).  The client still applies the credit to
      // local rpg state -- moving that store to the worker is a
      // follow-up slice.
      const pile = this._spawnLootForKill(zone, m, session.id, goldRecipients, shares);
      if (pile) {
        this.eventBuffer.push({
          type: 'loot_drop',
          payload: { pile: this._serializePile(pile) },
        });
      }

      // Server-authoritative combat XP.  For every contribution-weighted
      // recipient (xpRecipients above), apply their share of m.xp to
      // playerState[id].xp + run the level-up loop.  Emit a private
      // combat_credit event so the picker's "+N XP" popup + level-up
      // SFX fire on receive; player_state then carries the new
      // authoritative totals so the client overwrites R.xp / R.level /
      // R.unspentT2.
      for (const rid of xpRecipients) {
        const recipPs = this.playerState[rid];
        if (!recipPs) continue;
        const share = shares[rid] || 0;
        const xpForRecipient = Math.round((m.xp || 0) * share);
        if (xpForRecipient <= 0) continue;
        const { leveled, levelsGained, newLevel } = this._addCombatXp(recipPs, xpForRecipient);
        // Level-up restores all three pools to max (mirrors the client's
        // existing level-up restore at BroTown.jsx:8973 / 8504 / 9851).
        // Also recompute maxes since level bumps the maxHp formula
        // (each level adds 12 base HP).
        if (leveled) {
          this._recomputeMaxes(recipPs);
          if (typeof recipPs.maxHp === 'number') recipPs.hp = recipPs.maxHp;
          if (typeof recipPs.maxStamina === 'number') recipPs.stamina = recipPs.maxStamina;
          if (typeof recipPs.maxMana === 'number') recipPs.mana = recipPs.maxMana;
        }
        this._saveRpg(rid, recipPs);
        const recipWs = this._wsBySessionId(rid);
        if (recipWs) {
          try {
            recipWs.send(JSON.stringify({
              type: 'combat_credit',
              payload: {
                monsterId: m.id,
                zone,
                xpAmt: xpForRecipient,
                leveled,
                levelsGained,
                newLevel,
              },
            }));
          } catch (e) {}
          this._sendPlayerState(recipWs, rid);
        }
      }

      // Clear contribution tracking for the next life of this monster.
      m.dmgByPlayer = {};
    }
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
    this.sessions.set(server, { id: null, name: 'Anon', data: {}, rtt: 80, lastPing: 0, lastRecv: Date.now() });
    if (!this.tickInterval && this.sessions.size === 1) this.startTickLoop();
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    const session = this.sessions.get(ws);
    if (!session) return;
    let msg;
    try { msg = JSON.parse(message); } catch { return; }
    // Reset the AFK clock on real input only.  Pong replies are
    // keepalive heartbeats, not player activity -- counting them would
    // mean the timeout fires only on TCP death, not on AFK players
    // (the original 45 s behavior, which never actually kicked anyone
    // who had a live tab open).
    if (msg.type !== 'pong') session.lastRecv = Date.now();

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
        /* Load (or bootstrap) the player's server-authoritative
           coins + inventory.  Stored entry wins; if there's no
           record yet, fall back to the values the client sent in
           the join payload (one-time trust at first connection)
           and persist them so subsequent connects use the stored
           value. */
        {
          const stored = await this._loadRpg(msg.id);
          if (stored) {
            this.playerState[msg.id].coins = stored.coins || 0;
            this.playerState[msg.id].inventory = stored.inventory || {};
            this.playerState[msg.id].lifeSkills = stored.lifeSkills || {};
            this.playerState[msg.id].level = stored.level || 1;
            this.playerState[msg.id].xp = stored.xp || 0;
            this.playerState[msg.id].unspentT2 = stored.unspentT2 || 0;
            this.playerState[msg.id].hp = typeof stored.hp === 'number' ? stored.hp : 100;
            this.playerState[msg.id].maxHp = typeof stored.maxHp === 'number' ? stored.maxHp : 100;
            this.playerState[msg.id].stamina = typeof stored.stamina === 'number' ? stored.stamina : 100;
            this.playerState[msg.id].maxStamina = typeof stored.maxStamina === 'number' ? stored.maxStamina : 100;
            this.playerState[msg.id].mana = typeof stored.mana === 'number' ? stored.mana : 100;
            this.playerState[msg.id].maxMana = typeof stored.maxMana === 'number' ? stored.maxMana : 100;
            this.playerState[msg.id]._buffs = (stored._buffs && typeof stored._buffs === 'object') ? { ...stored._buffs } : {};
            // Equipment from stored.  Server doesn't validate the
            // shape of these blobs -- just preserves what was last
            // persisted.  Stash truncated to cap.
            this.playerState[msg.id].weapon = stored.weapon || null;
            this.playerState[msg.id].rangedWeapon = stored.rangedWeapon || null;
            this.playerState[msg.id].staffWeapon = stored.staffWeapon || null;
            this.playerState[msg.id].activeSlot = stored.activeSlot || 'melee';
            this.playerState[msg.id].armor = stored.armor || null;
            this.playerState[msg.id].shield = stored.shield || null;
            this.playerState[msg.id].amulet = stored.amulet || null;
            this.playerState[msg.id].weaponStash = Array.isArray(stored.weaponStash) ? stored.weaponStash.slice(0, this.WEAPON_STASH_CAP) : [];
            this.playerState[msg.id]._quests = (stored._quests && typeof stored._quests === 'object') ? { ...stored._quests } : {};
            this.playerState[msg.id]._questFlags = (stored._questFlags && typeof stored._questFlags === 'object') ? { ...stored._questFlags } : {};
            this.playerState[msg.id]._questKills = (stored._questKills && typeof stored._questKills === 'object') ? { ...stored._questKills } : {};
            this.playerState[msg.id].achievementPoints = stored.achievementPoints || 0;
            // Restore the perfect-claim history so the rate-limit
            // window survives reconnects.  Stale entries (>60s old)
            // get pruned on the next _ratedHarvestAccuracy call.
            this.playerState[msg.id]._perfectHistory = Array.isArray(stored._perfectHistory) ? stored._perfectHistory : [];
          } else {
            // First-connect bootstrap caps.  Stored values (the
            // branch above) win on reconnect; this branch only runs
            // when a player has no DO storage entry yet.  Cheaters
            // who localStorage-tamper before their first ever connect
            // would otherwise inject huge values that then persist
            // forever.  Cap each field at "reasonable migrated SP
            // character" thresholds; legit new players are unaffected
            // (their values are tiny), legit veteran SP players see
            // some progression capped (acceptable trade — the user
            // can raise these caps if they hear complaints).
            const BOOTSTRAP_LEVEL_CAP = 15;
            const BOOTSTRAP_XP_CAP = 50000;
            const BOOTSTRAP_UT2_CAP = 75;
            const BOOTSTRAP_COINS_CAP = 2000;
            const BOOTSTRAP_INV_PER_ITEM_CAP = 50;
            const BOOTSTRAP_INV_KEY_COUNT_CAP = 100;

            const _rawInv = (msg.data && msg.data.rpgInventory && typeof msg.data.rpgInventory === 'object') ? msg.data.rpgInventory : {};
            const _cappedInv = {};
            let _kc = 0;
            for (const [k, v] of Object.entries(_rawInv)) {
              if (_kc >= BOOTSTRAP_INV_KEY_COUNT_CAP) break;
              const n = Number(v);
              if (!Number.isFinite(n) || n <= 0) continue;
              _cappedInv[k] = Math.min(BOOTSTRAP_INV_PER_ITEM_CAP, Math.floor(n));
              _kc++;
            }

            this.playerState[msg.id].coins = Math.max(0, Math.min(BOOTSTRAP_COINS_CAP,
              (msg.data && typeof msg.data.rpgCoins === 'number') ? Math.floor(msg.data.rpgCoins) : 0));
            this.playerState[msg.id].inventory = _cappedInv;
            this.playerState[msg.id].lifeSkills = (msg.data && msg.data.rpgLifeSkills && typeof msg.data.rpgLifeSkills === 'object') ? { ...msg.data.rpgLifeSkills } : {};
            this.playerState[msg.id].level = Math.max(1, Math.min(BOOTSTRAP_LEVEL_CAP,
              (msg.data && typeof msg.data.rpgLevel === 'number') ? Math.floor(msg.data.rpgLevel) : 1));
            this.playerState[msg.id].xp = Math.max(0, Math.min(BOOTSTRAP_XP_CAP,
              (msg.data && typeof msg.data.rpgXp === 'number') ? Math.floor(msg.data.rpgXp) : 0));
            this.playerState[msg.id].unspentT2 = Math.max(0, Math.min(BOOTSTRAP_UT2_CAP,
              (msg.data && typeof msg.data.rpgUnspentT2 === 'number') ? Math.floor(msg.data.rpgUnspentT2) : 0));
            this.playerState[msg.id].hp = (msg.data && typeof msg.data.rpgHp === 'number') ? msg.data.rpgHp : 100;
            this.playerState[msg.id].maxHp = (msg.data && typeof msg.data.rpgMaxHp === 'number') ? msg.data.rpgMaxHp : 100;
            this.playerState[msg.id].stamina = (msg.data && typeof msg.data.rpgStamina === 'number') ? msg.data.rpgStamina : 100;
            this.playerState[msg.id].maxStamina = (msg.data && typeof msg.data.rpgMaxStamina === 'number') ? msg.data.rpgMaxStamina : 100;
            this.playerState[msg.id].mana = (msg.data && typeof msg.data.rpgMana === 'number') ? msg.data.rpgMana : 100;
            this.playerState[msg.id].maxMana = (msg.data && typeof msg.data.rpgMaxMana === 'number') ? msg.data.rpgMaxMana : 100;
            this.playerState[msg.id]._buffs = {};
            // Equipment bootstrap.  No first-connect cap on the
            // equipment blobs themselves -- they're opaque objects
            // and any "cheating" of weapon stats would only matter
            // once we compute damage server-side (later slice).
            // Stash truncated to cap to prevent join-time inflation.
            this.playerState[msg.id].weapon = (msg.data && msg.data.rpgWeapon && typeof msg.data.rpgWeapon === 'object') ? { ...msg.data.rpgWeapon } : null;
            this.playerState[msg.id].rangedWeapon = (msg.data && msg.data.rpgRangedWeapon && typeof msg.data.rpgRangedWeapon === 'object') ? { ...msg.data.rpgRangedWeapon } : null;
            this.playerState[msg.id].staffWeapon = (msg.data && msg.data.rpgStaffWeapon && typeof msg.data.rpgStaffWeapon === 'object') ? { ...msg.data.rpgStaffWeapon } : null;
            this.playerState[msg.id].activeSlot = (msg.data && typeof msg.data.rpgActiveSlot === 'string') ? msg.data.rpgActiveSlot : 'melee';
            this.playerState[msg.id].armor = (msg.data && msg.data.rpgArmor && typeof msg.data.rpgArmor === 'object') ? { ...msg.data.rpgArmor } : null;
            this.playerState[msg.id].shield = (msg.data && msg.data.rpgShield && typeof msg.data.rpgShield === 'object') ? { ...msg.data.rpgShield } : null;
            this.playerState[msg.id].amulet = (msg.data && msg.data.rpgAmulet && typeof msg.data.rpgAmulet === 'object') ? { ...msg.data.rpgAmulet } : null;
            this.playerState[msg.id].weaponStash = (msg.data && Array.isArray(msg.data.rpgWeaponStash)) ? msg.data.rpgWeaponStash.slice(0, this.WEAPON_STASH_CAP) : [];
            // Quest state bootstrap (slice 17).  Trust shape but not
            // size -- a cheater could pass a 10000-entry _questKills
            // map to inflate storage.  Strip non-numeric values and
            // cap key count.
            const _qK = (msg.data && msg.data.rpgQuestKills && typeof msg.data.rpgQuestKills === 'object') ? msg.data.rpgQuestKills : {};
            const _qKclean = {};
            let _qKc = 0;
            for (const [k, v] of Object.entries(_qK)) {
              if (_qKc >= 50) break;
              const n = Number(v);
              if (Number.isFinite(n) && n >= 0) {
                _qKclean[k] = Math.min(99999, Math.floor(n));
                _qKc++;
              }
            }
            // Cap _quests + _questFlags key counts so a cheater
            // can't fill storage with a 100k-entry map at first
            // connect.  100 keys is well above the known
            // QUEST_CHAINS table size (25 quests) + a generous
            // buffer for flags + future expansion.
            const _capObjKeys = (src) => {
              const out = {};
              if (!src || typeof src !== 'object') return out;
              let n = 0;
              for (const [k, v] of Object.entries(src)) {
                if (n >= 100) break;
                out[k] = v;
                n++;
              }
              return out;
            };
            this.playerState[msg.id]._quests = _capObjKeys((msg.data && msg.data.rpgQuests) || null);
            this.playerState[msg.id]._questFlags = _capObjKeys((msg.data && msg.data.rpgQuestFlags) || null);
            this.playerState[msg.id]._questKills = _qKclean;
            this.playerState[msg.id].achievementPoints = Math.max(0, Math.min(99999,
              (msg.data && typeof msg.data.rpgAchievementPoints === 'number') ? Math.floor(msg.data.rpgAchievementPoints) : 0));
            this.playerState[msg.id]._perfectHistory = [];
            await this._saveRpg(msg.id, this.playerState[msg.id]);
          }
          // Session-only equipment-derived values.  Always read from join
          // — recomputed client-side on every recalcDerived.
          this.playerState[msg.id].def = (msg.data && typeof msg.data.rpgDef === 'number') ? Math.max(0, msg.data.rpgDef) : 0;
          this.playerState[msg.id].amuletHpRegen = (msg.data && typeof msg.data.rpgAmuletHpRegen === 'number') ? Math.max(0, msg.data.rpgAmuletHpRegen) : 0;
          this.playerState[msg.id].amuletStaminaRegen = (msg.data && typeof msg.data.rpgAmuletStaminaRegen === 'number') ? Math.max(0, msg.data.rpgAmuletStaminaRegen) : 0;
          this.playerState[msg.id].lastDamageAt = 0;
          this.playerState[msg.id].dying = false;
          this.playerState[msg.id].respawnAt = 0;

          // Raw stats: prefer stored (already-clamped) values; bootstrap
          // from join payload otherwise, clamped to the per-level cap.
          // Cheater spoofing rpgVitality: 99999 on join gets clamped to
          // level * 10 + 20 -- bounded forever after, even on reconnect.
          {
            const _ps = this.playerState[msg.id];
            const _lvl = _ps.level || 1;
            const RAW_STATS = ['power', 'vitality', 'endurance', 'agility', 'mind',
              'ferocity', 'elementalMastery', 'fortification', 'restoration', 'influence'];
            const _storedHasStats = stored && typeof stored.vitality === 'number';
            for (const s of RAW_STATS) {
              if (_storedHasStats && typeof stored[s] === 'number') {
                _ps[s] = stored[s];
              } else {
                const joinKey = 'rpg' + s.charAt(0).toUpperCase() + s.slice(1);
                const joinVal = (msg.data && typeof msg.data[joinKey] === 'number') ? msg.data[joinKey] : 0;
                _ps[s] = this._clampStat(joinVal, _lvl);
              }
            }
            // Server-owned max values: compute from clamped raw stats.
            // Persisted hp / stamina / mana already loaded above; clamp
            // them to the recomputed maxes here.
            this._recomputeMaxes(_ps);
            this._saveRpg(msg.id, _ps);
          }
        }
        this.broadcastExcept(ws, { type: 'player_join', id: msg.id, name: msg.name, data: msg.data });
        // Send current state + monsters for player's zone
        const joinZone = msg.data?.z || 'town';
        const zoneMonsters = (joinZone !== 'town' && joinZone !== 'farm_home') ? this._ensureZoneMonsters(joinZone) : [];
        const zoneNodes = (joinZone !== 'town' && joinZone !== 'farm_home') ? this._ensureZoneNodes(joinZone) : [];
        const zoneLootForJoin = (joinZone !== 'town' && joinZone !== 'farm_home') ? this._zoneLootForWire(joinZone) : [];
        ws.send(JSON.stringify({
          type: 'state_sync',
          players: this.getAllPlayerData(),
          playerCount: this.getPlayerCount(),
          monsters: zoneMonsters.map(m => ({
            id: m.id, arch: m.arch, level: m.level, element: m.element,
            x: m.x, y: m.y, hp: m.hp, maxHp: m.maxHp, dmg: m.dmg,
            xp: m.xp, gold: m.gold, spd: m.spd, emoji: m.emoji, color: m.color,
            alive: m.alive,
          })),
          nodes: zoneNodes.map(n => ({
            id: n.id, nodeType: n.nodeType, x: n.x, y: n.y,
            tierLvl: n.tierLvl, alive: n.alive, respawnAt: n.respawnAt,
          })),
          loot: zoneLootForJoin,
          monsterZone: joinZone,
        }));
        /* Authoritative rpg state sync -- the client overwrites its
           local R.coins / R.inventory with whatever's on the worker.
           Bootstrap-from-join (above) means this matches what the
           client just sent on the first connect, and matches the
           stored value on subsequent connects. */
        this._sendPlayerState(ws, msg.id);
        this.broadcastAll({ type: 'player_count', count: this.getPlayerCount() });
        this.reportToLeaderboard(session);
        break;

      case 'move':
        if (session.id && this.playerState[session.id]) {
          const ps = this.playerState[session.id];
          const oldZone = ps.z;
          const newZone = msg.z || ps.z;

          // ═══ Movement validation (anti-teleport) ═══
          //
          // Worker used to trust msg.x / msg.y blindly.  A cheater
          // could write into the move event and warp anywhere -- which
          // bypassed the range checks in _handleLootPickup,
          // _handleNodeStrike, _resolvePvPAttack, and the monster aggro
          // distance (all of those compare against ps.x/y after the
          // overwrite).  Now we cap the per-event position delta to a
          // speed * elapsed-time bound.
          //
          // Client max walk speed (calcMoveSpeed in gameSystems.js):
          //   baseSpd = calcMoveSpeed(agility)/5.0 * SPEED
          //           = (1 + min(agility*0.0012, 0.60)) * 2.5 px/frame
          //   Max ~4 px/frame * 60fps = 240 px/sec.  spdBuff adds 15%
          //   = 276 px/sec.  Dodge / lunge burst ~48 px instantaneously.
          //
          // Cap: 500 px/sec sustained + 80 px burst slack (covers
          // dodge/lunge + a bit of network jitter).  Far below the
          // egregious "teleport across the room" cheat (1024+ px),
          // generous enough for legit lag-recovery jumps.
          //
          // Zone changes legitimately move the player to a new zone's
          // spawn coords -- bypass the check on z-change.  Also bypass
          // on the FIRST move event (no prior position to delta from).
          if (typeof msg.x !== 'number' || typeof msg.y !== 'number') break;
          const _now = Date.now();
          const zoneChanged = newZone !== oldZone;
          const firstMove = typeof ps.lastMoveAt !== 'number';
          let accept = true;
          if (!zoneChanged && !firstMove && typeof ps.x === 'number' && typeof ps.y === 'number') {
            const dt = Math.max(0.001, (_now - ps.lastMoveAt) / 1000);
            const maxDist = 500 * dt + 80;
            const dx = msg.x - ps.x;
            const dy = msg.y - ps.y;
            if (dx * dx + dy * dy > maxDist * maxDist) {
              // Reject: do not update ps.x/y.  Still update lastMoveAt
              // so spam-bursts don't compound dt.  dropped silently --
              // client's next legit move will snap back to server view
              // via the broadcast tick.
              accept = false;
            }
          }
          ps.lastMoveAt = _now;

          // Position + velocity + flags update only on accept.  On
          // reject, ps.z is still updated for zone-change bypasses
          // (those always set accept=true); for non-zone-change
          // rejections, we drop EVERYTHING so a cheater can't flip
          // blocking/dodging/dead while teleporting.
          if (accept) {
            ps.x = msg.x; ps.y = msg.y;
            ps.d = msg.d || ps.d; ps.z = newZone;
            ps.vx = msg.vx || 0; ps.vy = msg.vy || 0;
            if (msg.dodging !== undefined) ps.dodging = !!msg.dodging;
            if (msg.blocking !== undefined) ps.blocking = !!msg.blocking;
            if (msg.dead !== undefined) ps.dead = !!msg.dead;
            this.dirtyPlayers.add(session.id);
          }

          // Zone change — send monster + gather node state for new zone
          if (ps.z !== oldZone && ps.z !== 'town' && ps.z !== 'farm_home') {
            const newMonsters = this._ensureZoneMonsters(ps.z);
            ws.send(JSON.stringify({
              type: 'zone_monsters',
              zone: ps.z,
              monsters: newMonsters.map(m => ({
                id: m.id, arch: m.arch, level: m.level, element: m.element,
                x: m.x, y: m.y, hp: m.hp, maxHp: m.maxHp, dmg: m.dmg,
                xp: m.xp, gold: m.gold, spd: m.spd, emoji: m.emoji, color: m.color,
                alive: m.alive,
              })),
            }));
            const newNodes = this._ensureZoneNodes(ps.z);
            ws.send(JSON.stringify({
              type: 'zone_nodes',
              zone: ps.z,
              nodes: newNodes.map(n => ({
                id: n.id, nodeType: n.nodeType, x: n.x, y: n.y,
                tierLvl: n.tierLvl, alive: n.alive, respawnAt: n.respawnAt,
              })),
            }));
            ws.send(JSON.stringify({
              type: 'zone_loot',
              zone: ps.z,
              loot: this._zoneLootForWire(ps.z),
            }));
          }
        }
        break;

      case 'pong':
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
        if (session.id) {
          this._resolvePvPAttack(session, msg.payload || msg);
        }
        break;

      case 'monster_damage':
        // Client reports damage dealt to a server monster
        if (session.id) {
          this._handleMonsterDamage(session, msg.payload || msg);
        }
        break;

      case 'node_strike':
        // Client reports a successful harvest action on a gather node.
        // The minigame already gates this on success (mining miss does
        // NOT send), so we just validate and apply.
        if (session.id) {
          this._handleNodeStrike(session, msg.payload || msg);
        }
        break;

      case 'loot_pickup':
        // Client requests to pick up a loot pile.  Server validates
        // (range, recipient, single-claim) and emits a private
        // loot_credit back to the picker with their authorized share.
        if (session.id) {
          this._handleLootPickup(session, msg.payload || msg);
        }
        break;

      case 'stat_allocate':
        // Client requests to spend 1 unspentT2 on a named stat.
        // Server checks ps.unspentT2 > 0 + stat name validity, applies,
        // and emits stat_allocated so the client mirrors the increment.
        if (session.id) {
          this._handleStatAllocate(session, msg.payload || msg);
        }
        break;

      case 'cook_request':
        // Cooking minigame finished; client reports the outcome (kind)
        // and the raw fish key.  Server validates the player has the
        // raw fish, applies the consume + cooked/burnt outcome + cooking
        // XP, and emits player_state.
        if (session.id) {
          this._handleCookRequest(session, msg.payload || msg);
        }
        break;

      case 'stats_update':
        // Client recalcDerived ran (equipment / stat-alloc / level-up
        // changed derived stats); push new maxHp + def + regen mods so
        // the worker's damage math and regen tick use current values.
        if (session.id) {
          this._handleStatsUpdate(session, msg.payload || msg);
        }
        break;

      case 'ability_use':
        // Player triggered a stamina/mana-costing action (dodge / lunge
        // / retreat / swipe).  Server computes cost from ps.maxStamina
        // or the swipe ramp, validates, deducts, and emits player_state.
        if (session.id) {
          this._handleAbilityUse(session, msg.payload || msg);
        }
        break;

      case 'eat_request':
        // Player clicked Eat on a cooked_fish_* inventory item.
        // Server validates ownership, consumes 1, heals hp, emits
        // player_state.
        if (session.id) {
          this._handleEatRequest(session, msg.payload || msg);
        }
        break;

      case 'shop_purchase':
        // Player clicked Buy on the NPC vendor.  Server validates
        // coins + applies effect (pool restore or inventory grant).
        if (session.id) {
          this._handleShopPurchase(session, msg.payload || msg);
        }
        break;

      case 'cook_recipe':
        // Cooking recipe triggered (multi-ingredient -> buff or heal).
        // Server validates ingredient ownership, consumes, applies
        // buff timer to ps._buffs, emits player_state.
        if (session.id) {
          this._handleCookRecipe(session, msg.payload || msg);
        }
        break;

      case 'equip_request':
        // Swap a weaponStash entry with an active equipment slot.
        // Server validates stashIdx + slot name, performs the swap,
        // emits player_state with the new equipment layout.
        if (session.id) {
          this._handleEquipRequest(session, msg.payload || msg);
        }
        break;

      case 'sell_weapon':
        // Sell a weaponStash entry to an NPC for coins.  Server
        // validates ownership + computes sell value, credits coins,
        // emits player_state.
        if (session.id) {
          this._handleSellWeapon(session, msg.payload || msg);
        }
        break;

      case 'unequip_request':
        // Unequip an active equipment slot (weapon -> stash,
        // armor/shield/amulet -> null).
        if (session.id) {
          this._handleUnequipRequest(session, msg.payload || msg);
        }
        break;

      case 'forge_weapon':
        // Blacksmith / woodworker forge.  Server validates resource +
        // coin + skill + stat gates, consumes, mints new weapon,
        // swaps old to stash, applies crafting XP.
        if (session.id) {
          this._handleForgeWeapon(session, msg.payload || msg);
        }
        break;

      case 'quest_accept':
        // Player accepted a quest from the NPC dialog.  Server
        // validates the questId + current state, transitions to
        // 'active'.
        if (session.id) {
          this._handleQuestAccept(session, msg.payload || msg);
        }
        break;

      case 'quest_turn_in':
        // Player turning in a completed quest.  Server validates
        // 'active' state + applies reward (gold + xp + AP) + unlocks
        // next in chain.
        if (session.id) {
          this._handleQuestTurnIn(session, msg.payload || msg);
        }
        break;

      default:
        // Critical security gate: the default branch rebroadcasts the
        // message to every client in the room via eventBuffer.  Without
        // a deny-list, a malicious client could forge any server-only
        // event type (player_state hp:0 to kill everyone, player_died
        // to grief, combat_credit to fire fake level-up popups, etc.)
        // and the worker would faithfully rebroadcast it -- every
        // client's WS switch would then trigger the handler for that
        // type, which assumes the event came from the server.
        //
        // Anything the worker emits itself (player_state, player_died,
        // _credit fan-outs, monster_* events, loot_* events, tick,
        // state_sync, etc.) is privileged: never accept it from a
        // client.  Legitimate client→client broadcasts (chat, emote,
        // pvp_confirmed, player_shield, player_died_to_monster, etc.)
        // still flow through here -- they hit the deny-list miss and
        // get rebroadcast normally.
        if (PRIVILEGED_EVENTS.has(msg.type)) break;
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

    // Gate: dead / dying / disconnected attackers can't keep firing
    // PvP hits.  Other handlers (ability_use, eat_request, etc.) all
    // gate on these flags; PvP was missing the check.
    if (attackerPs.dying || attackerPs.dead || attackerPs.disconnected) return;
    // Bound the client-supplied attack geometry so a cheater can't
    // claim a 99999-pixel range or full-circle arc to hit every player
    // in the room.  Realistic max: bow range = 200 + amulet bonus,
    // greatsword arc = PI*0.85 ≈ 2.67 rad.  Cap a bit above those.
    const range = Math.max(10, Math.min(250, payload.range || 40));
    const arc = Math.max(0.1, Math.min(Math.PI * 1.1, payload.arc || 1.2));
    const angle = payload.angle || 0;
    // Weapon-aware cap (slice 16) -- mirrors monster_damage cap above.
    // Server now owns the weapon table so the bound is tighter than
    // the previous level-only formula.
    const dmgCap = this._maxDmgForAttacker(attackerPs);
    const dmgBase = Math.max(1, Math.min(dmgCap, payload.dmgBase || 10));
    const critChance = Math.max(0, Math.min(100, payload.critChance || 0));

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

      // Apply HP damage server-side.  Mirrors the client formula at
      // BroTown.jsx:3029-3035: rawDmg = dmgBase * (crit ? 1.5 : 1) *
      // (blocked ? 0.25 : 1).  We apply via _applyDamage which handles
      // the def * 0.3 reduction.  Pass isBlock=false so blocked just
      // scales the raw dmg (matches client's 0.25× partial-block),
      // not a full 0-dmg server block.
      const rawDmg = dmgBase * (isCrit ? 1.5 : 1) * (blocked ? 0.25 : 1);
      const dmgTaken = this._applyDamage(targetPs, rawDmg, false);

      // Build hit event — server-authoritative hp now mirrors via
      // player_state below, but dmgTaken in the payload drives the
      // damage popup so it doesn't have to wait a round-trip.
      const hitEvent = {
        type: 'pvp_hit',
        payload: {
          attacker: attackerId,
          attackerName: attackerSession.name,
          target: targetId,
          dmgBase: dmgBase,
          dmgTaken,
          isCrit: isCrit,
          blocked: blocked,
          ts: Date.now(),
          rewindTicks: rewindTicks,
        }
      };
      this.eventBuffer.push(hitEvent);

      // Echo authoritative hp + death check.
      this._saveRpg(targetId, targetPs);
      const victimWs = this._wsBySessionId(targetId);
      if (victimWs) this._sendPlayerState(victimWs, targetId);
      if (targetPs.hp <= 0 && !targetPs.dying) {
        this._handlePlayerDeath(targetPs, targetId, 'pvp:' + attackerId);
      }
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
    let pingCounter = 0;
    let regenCounter = 0;

    this.tickInterval = setInterval(() => {
      // §16.12 — Snapshot player states to history buffer
      for (const [id, ps] of Object.entries(this.playerState)) {
        if (!this.stateHistory[id]) this.stateHistory[id] = [];
        this.stateHistory[id].push({
          x: ps.x, y: ps.y, d: ps.d, z: ps.z,
          dodging: ps.dodging || false,
          blocking: ps.blocking || false,
          dead: ps.dead || false,
          tick: this.tickSeq,
        });
        if (this.stateHistory[id].length > this.LAGCOMP_BUFFER_TICKS) {
          this.stateHistory[id].shift();
        }
      }

      // Monster AI tick
      this._tickMonsters();

      // Gather-node respawn tick (cheap; iterates Object.keys(this.nodes))
      this._tickNodes();

      // Loot pile expiry tick -- piles older than LOOT_EXPIRY_MS get
      // despawned with a broadcast event so clients drop them too.
      this._tickLoot();

      // Player respawn tick — flip dying=>alive when respawnAt elapses.
      // Cheap; iterates active player entries.
      this._tickPlayerRespawn();

      // HP regen tick — every 30 server ticks (~670 ms at TICK_RATE=22).
      // Skip when no one needs healing to avoid wasted iteration.
      regenCounter++;
      if (regenCounter >= 30) {
        regenCounter = 0;
        this._tickPlayerRegen();
      }

      // Periodic ping for RTT estimation + idle-session eviction (every ~3s at 30Hz)
      pingCounter++;
      if (pingCounter >= 90) {
        pingCounter = 0;
        const nowMs = Date.now();
        const pingMsg = JSON.stringify({ type: 'ping', ts: nowMs });
        for (const [ws, session] of this.sessions) {
          if (nowMs - session.lastRecv > this.IDLE_TIMEOUT_MS) {
            try { ws.close(1000, 'idle timeout'); } catch {}
            continue;
          }
          session.lastPing = nowMs;
          try { ws.send(pingMsg); } catch {}
        }
      }

      const hasDirty = this.dirtyPlayers.size > 0;
      const hasEvents = this.eventBuffer.length > 0;
      const hasMonsters = this.dirtyMonsters.size > 0;
      const hasNodes = this.dirtyNodes.size > 0;
      if (!hasDirty && !hasEvents && !hasMonsters && !hasNodes) { this.tickSeq++; return; }

      // Build single room-wide tick delta
      const delta = { type: 'tick', seq: this.tickSeq++, ts: Date.now() };

      // Batched player positions (only dirty)
      if (hasDirty) {
        const players = {};
        for (const id of this.dirtyPlayers) {
          const ps = this.playerState[id];
          if (ps) players[id] = { x: ps.x, y: ps.y, d: ps.d, z: ps.z, vx: ps.vx, vy: ps.vy };
        }
        delta.players = players;
        this.dirtyPlayers.clear();
      }

      // Batched game events (capped)
      if (hasEvents) {
        delta.events = this.eventBuffer.length <= this.EVENTS_PER_TICK_CAP
          ? this.eventBuffer
          : this.eventBuffer.slice(0, this.EVENTS_PER_TICK_CAP);
        this.eventBuffer = [];
      }

      // Monster state updates (only dirty zones, only alive + recently died)
      if (hasMonsters) {
        const mData = {};
        for (const zoneId of this.dirtyMonsters) {
          const monsters = this.monsters[zoneId];
          if (!monsters) continue;
          mData[zoneId] = monsters.map(m => ({
            id: m.id, x: Math.round(m.x), y: Math.round(m.y),
            hp: m.hp, alive: m.alive,
          }));
        }
        delta.monsters = mData;
        this.dirtyMonsters.clear();
      }

      // Gather-node deltas — only state-change fields (alive / respawnAt).
      // The full node payload (type / x / y / tierLvl) is sent once at
      // state_sync or zone_nodes; the client already has the position.
      if (hasNodes) {
        const nData = {};
        for (const zoneId of this.dirtyNodes) {
          const list = this.nodes[zoneId];
          if (!list) continue;
          nData[zoneId] = list.map((n) => ({
            id: n.id, alive: n.alive, respawnAt: n.respawnAt,
          }));
        }
        delta.nodes = nData;
        this.dirtyNodes.clear();
      }

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
