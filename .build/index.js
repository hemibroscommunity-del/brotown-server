var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/index.js
var index_default = {
  async fetch(request, env) {
    const url = new URL(request.url);
    const corsHeaders = { "Access-Control-Allow-Origin": "*", "Content-Type": "application/json" };
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: { ...corsHeaders, "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS", "Access-Control-Allow-Headers": "*" } });
    }
    if (url.pathname === "/ws") {
      const room = url.searchParams.get("room") || "brotown";
      return env.GAME_ROOM.get(env.GAME_ROOM.idFromName(room)).fetch(request);
    }
    if (url.pathname === "/api/lobby") {
      const PREFIX = "brotown";
      const SOFT_CAP = 40;
      const SCAN_ROOMS = 10;
      for (let i = 1; i <= SCAN_ROOMS; i++) {
        const room = PREFIX + "-" + i;
        try {
          const stub = env.GAME_ROOM.get(env.GAME_ROOM.idFromName(room));
          const probe = await stub.fetch(new Request("https://internal/_room_count"));
          if (probe.ok) {
            const { count } = await probe.json();
            if (typeof count === "number" && count < SOFT_CAP) {
              return new Response(JSON.stringify({ room, count }), { headers: corsHeaders });
            }
          }
        } catch (e) {
        }
      }
      return new Response(JSON.stringify({ room: PREFIX + "-" + (SCAN_ROOMS + 1) }), { headers: corsHeaders });
    }
    if (url.pathname.startsWith("/api/market")) {
      return env.MARKETPLACE.get(env.MARKETPLACE.idFromName("global")).fetch(request);
    }
    if (url.pathname.startsWith("/api/leaderboard")) {
      return env.LEADERBOARD.get(env.LEADERBOARD.idFromName("global")).fetch(request);
    }
    if (url.pathname.startsWith("/api/arena")) {
      return env.ARENA.get(env.ARENA.idFromName("global")).fetch(request);
    }
    if (url.pathname.startsWith("/api/feedback")) {
      return env.FEEDBACK.get(env.FEEDBACK.idFromName("global")).fetch(request);
    }
    if (url.pathname === "/health") {
      return new Response(JSON.stringify({ status: "ok", ts: Date.now() }), { headers: corsHeaders });
    }
    return new Response("Hemi Bros Game Server", { status: 200 });
  }
};
var PRIVILEGED_EVENTS = /* @__PURE__ */ new Set([
  // Pool / progression mirrors
  "player_state",
  "player_died",
  // 'player_respawned' intentionally OMITTED: the client broadcasts it
  // to peers as a visual signal (clears _isDead on remote entries so
  // the corpse stops rendering).  Blocking it here leaves other clients
  // showing the respawned player as a corpse forever.  The server's
  // own server->self player_respawned still fires (direct ws.send,
  // doesn't route through this deny-list).  Forgery risk is purely
  // visual -- a cheater can clear their own corpse on others' screens
  // but can't actually revive themselves server-side.
  "combat_credit",
  "harvest_credit",
  "loot_credit",
  "lifesteal_credit",
  "loot_pickup_rejected",
  "stat_allocated",
  "ability_rejected",
  // Combat resolution
  "monster_attack",
  "monster_hit",
  "monster_kill",
  "pvp_hit",
  // World state fan-outs
  "loot_drop",
  "loot_claimed",
  "loot_despawn",
  "zone_monsters",
  "zone_nodes",
  "zone_loot",
  // Bootstrap + protocol
  "state_sync",
  "tick",
  "ping",
  "player_count",
  "player_join",
  "player_leave",
  "player_update"
]);
var GameRoom = class {
  static {
    __name(this, "GameRoom");
  }
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sessions = /* @__PURE__ */ new Map();
    this.playerState = {};
    this.dirtyPlayers = /* @__PURE__ */ new Set();
    this.eventBuffer = [];
    this.tickInterval = null;
    this.tickSeq = 0;
    this.TICK_RATE = 22;
    this.MAX_PLAYERS = 50;
    this.EVENTS_PER_TICK_CAP = 500;
    this.WEAPON_STASH_CAP = 8;
    this.QUEST_AP_REWARD = 5;
    this.pendingPlayerStateFlush = /* @__PURE__ */ new Set();
    this.stateHistory = {};
    this.LAGCOMP_BUFFER_TICKS = 14;
    this.LAGCOMP_RTT_CAP = 300;
    this.LAGCOMP_RTT_ALPHA = 0.3;
    this.monsters = {};
    this.dirtyMonsters = /* @__PURE__ */ new Set();
    this.RESPAWN_TIME = 15e3;
    this.MONSTER_AGGRO_RANGE = 120;
    this.MONSTER_ATTACK_RANGE = 45;
    this.MONSTER_ATTACK_CD = 1500;
    this.TILE = 32;
    this.nodes = {};
    this.dirtyNodes = /* @__PURE__ */ new Set();
    this.NODE_RESPAWN_TIME = 12e4;
    this.EXTRACT_WINDOW_MS = 1500;
    this.EXTRACT_OPEN_MIN = 2e3;
    this.EXTRACT_OPEN_MAX = 1e4;
    this.EXTRACT_OPEN_BASE = 4e3;
    this.EXTRACT_JITTER = 0.15;
    this.EXTRACTION_TIMEOUT_MS = 15e3;
    this.EXTRACTION_GRACE_MS = 250;
    this.SWIPE_FP_CAP_PER_SESSION = 100;
    this.LATENCY_CAP_PER_SESSION = 200;
    this.extractions = {};
    this.loot = {};
    this.LOOT_EXPIRY_MS = 6e4;
    this.DEATH_PILE_OWNER_MS = 6e4;
    this.DEATH_PILE_TOTAL_MS = 12e4;
    this.LOOT_PICKUP_RANGE = 60;
    this.ZONE_ENTRY_GRACE_MS = 500;
    this.SHARD_DROP_RATE = 0.1;
    this.IDLE_TIMEOUT_MS = 12e4;
    try {
      for (const ws of this.state.getWebSockets()) {
        try {
          ws.close(1e3, "stale on wake");
        } catch {
        }
      }
    } catch {
    }
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
      fodder: { hpMult: 0.6, dmgMult: 0.8, spdMult: 1, emoji: "\u{1F7E2}", color: "#3dd497" },
      brute: { hpMult: 1.5, dmgMult: 1.3, spdMult: 0.7, emoji: "\u{1FAA8}", color: "#6b6b6b" },
      swarm: { hpMult: 0.4, dmgMult: 0.6, spdMult: 1.2, emoji: "\u{1F987}", color: "#9333ea" },
      sentinel: { hpMult: 1, dmgMult: 1, spdMult: 1, emoji: "\u{1F6E1}\uFE0F", color: "#e8e8e8" },
      volatile: { hpMult: 0.8, dmgMult: 1, spdMult: 1, emoji: "\u{1F4A5}", color: "#ea580c" },
      stalker: { hpMult: 0.7, dmgMult: 1.2, spdMult: 1.3, emoji: "\u{1F441}\uFE0F", color: "#2C3E50" },
      hexer: { hpMult: 0.9, dmgMult: 0.8, spdMult: 1, emoji: "\u{1F480}", color: "#8E44AD" },
      snowman: { hpMult: 1.3, dmgMult: 1.1, spdMult: 0.8, emoji: "\u26C4", color: "#b0d8f0" }
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
      meadow: { w: 32, h: 32, level: [1, 10], element: null, spawns: [{ arch: "fodder", count: 10 }] },
      ember: { w: 32, h: 32, level: [1, 10], element: "flame", spawns: [{ arch: "fodder", count: 6 }] },
      mist: { w: 32, h: 32, level: [1, 10], element: "venom", spawns: [] },
      frost: { w: 32, h: 32, level: [1, 10], element: "frost", spawns: [{ arch: "snowman", count: 4 }] },
      thunder: { w: 32, h: 32, level: [1, 10], element: "storm", spawns: [{ arch: "fodder", count: 6 }] },
      hollows: { w: 32, h: 32, level: [1, 10], element: "stone", spawns: [{ arch: "brute", count: 4 }] },
      sky: { w: 32, h: 32, level: [1, 10], element: "wind", spawns: [{ arch: "stalker", count: 4 }, { arch: "hexer", count: 3 }, { arch: "volatile", count: 3 }] },
      tidal: { w: 32, h: 32, level: [1, 10], element: "water", spawns: [{ arch: "brute", count: 3 }] }
    };
    return ZONES[zoneId] || null;
  }
  // Per-zone monster variant overrides.  Mirrors ZONE_VARIANT_MAP in
  // src/data/monsterVariants.js.  Server AI runs against the BASE
  // archetype (fodder/brute/etc.) so this only affects the variant
  // name used for skull / inventory-key resolution on kill --
  // without it, killing a flame-zone fodder (rendered as a fire
  // goblin on the client) drops 'slime-remnants' instead of
  // 'fire-goblin-remnants'.  Keep in sync if new variants ship.
  _variantForArchInZone(arch, zoneId) {
    const MAP = {
      ember: { fodder: "fireGoblin" },
      sky: {
        fodder: "mummy",
        stalker: "mummy",
        hexer: "mummy",
        volatile: "mummy",
        brute: "mummy",
        swarm: "mummy",
        sentinel: "mummy"
      }
    };
    const zm = MAP[zoneId];
    if (zm && zm[arch]) return zm[arch];
    return null;
  }
  // Per-variant gameplay overrides (speed only for now).  Mirrors the
  // `spd` field on entries in src/data/monsterVariants.js.  Keep in
  // sync if a variant's speed changes -- the server's AI uses this to
  // move the monster at the correct pace, so a stale value here means
  // server-driven movement runs at the wrong speed and clients with
  // the new variants on screen drift away from the server position.
  //
  // skeleton is listed but currently spawns only via the (still
  // client-side) mummy->skeleton transform.  Once that transform
  // moves server-side, dropping `clientSideMovement: true` from the
  // skeleton variant becomes safe; until then, the client keeps
  // running skeleton AI locally and this entry is informational.
  _variantSpeed(variantKey) {
    const SPEEDS = {
      fireGoblin: 1.5,
      mummy: 0.4,
      skeleton: 1.4
    };
    return SPEEDS[variantKey];
  }
  // Variant transform thresholds + targets.  Mirrors the
  // transformAt + transformsTo fields on entries in
  // src/data/monsterVariants.js.  Returns null for variants that
  // don't transform.
  //
  // Mummy at 50% HP shreds its bandages and becomes a skeleton.  The
  // worker runs this check in _tickMonsters for every alive variant
  // monster + emits a monster_transform event; the client plays the
  // shred animation locally on receipt and updates its archetype.
  _variantTransform(variantKey) {
    const T = {
      mummy: { at: 0.5, to: "skeleton" }
    };
    return T[variantKey] || null;
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
        const baseHp = this._monsterStat(12.5, lvl, 1.065, 1.035, 1.025);
        const baseDmg = this._monsterStat(12, lvl, 1.045, 1.025, 1.018);
        const baseXp = this._monsterStat(10, lvl, 1.045, 1.025, 1.018);
        const baseGold = this._monsterStat(5, lvl, 1.035, 1.02, 1.015);
        const variantKey = this._variantForArchInZone(spawn.arch, zoneId);
        const variantSpd = variantKey ? this._variantSpeed(variantKey) : null;
        const finalSpd = variantSpd != null ? variantSpd : 0.5 * a.spdMult;
        monsters.push({
          id: "sm-" + zoneId + "-" + idx,
          arch: spawn.arch,
          // Variant tag used at kill time for skull / inventory key
          // resolution AND for visual / AI dispatch on the client.
          // Server's AI uses m.spd directly (set above with variant
          // override applied), so variant is also the source of truth
          // for movement pace -- not just cosmetics.
          variant: variantKey,
          // Mid-fight transforms (mummy -> skeleton) mutate m.variant
          // + m.spd; the respawn path resets to these spawn values so
          // a re-spawned mummy starts back in mummy form instead of
          // re-spawning as a skeleton.
          spawnVariant: variantKey,
          spawnSpd: finalSpd,
          level: lvl,
          element: zone.element || null,
          hp: Math.ceil(baseHp * a.hpMult),
          maxHp: Math.ceil(baseHp * a.hpMult),
          dmg: Math.ceil(baseDmg * a.dmgMult),
          xp: Math.ceil(baseXp),
          gold: Math.ceil(baseGold),
          spd: finalSpd,
          emoji: a.emoji,
          color: a.color,
          x,
          y,
          spawnX: x,
          spawnY: y,
          alive: true,
          targetId: null,
          // player being chased
          atkCd: 0,
          respawnAt: 0
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
    const zones = /* @__PURE__ */ new Set();
    for (const ps of Object.values(this.playerState)) {
      if (ps.z && ps.z !== "town" && ps.z !== "farm_home") zones.add(ps.z);
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
      const playersInZone = [];
      for (const [id, ps] of Object.entries(this.playerState)) {
        if (ps.z === zoneId && !ps.dead && !ps.disconnected) {
          playersInZone.push({ id, ...ps });
        }
      }
      let zoneChanged = false;
      for (const m of monsters) {
        if (!m.alive) {
          if (m.respawnAt > 0 && now >= m.respawnAt) {
            m.alive = true;
            m.hp = m.maxHp;
            m.x = m.spawnX;
            m.y = m.spawnY;
            m.targetId = null;
            m.atkCd = 0;
            if (m.spawnVariant !== void 0) {
              m.variant = m.spawnVariant;
              const respawnSpd = m.spawnVariant ? this._variantSpeed(m.spawnVariant) : null;
              if (respawnSpd != null) m.spd = respawnSpd;
              else if (m.spawnSpd != null) m.spd = m.spawnSpd;
            }
            zoneChanged = true;
          }
          continue;
        }
        if (m.variant) {
          const tx = this._variantTransform(m.variant);
          if (tx && m.maxHp > 0 && m.hp / m.maxHp <= tx.at) {
            const fromVariant = m.variant;
            const toVariant = tx.to;
            m.variant = toVariant;
            const newSpd = this._variantSpeed(toVariant);
            if (newSpd != null) m.spd = newSpd;
            this.eventBuffer.push({
              type: "monster_transform",
              payload: { id: m.id, zone: zoneId, fromVariant, toVariant }
            });
            zoneChanged = true;
          }
        }
        let nearest = null;
        let nearestDist = Infinity;
        const stickyAggroActive = m._aggroOverrideUntil && now < m._aggroOverrideUntil;
        if (stickyAggroActive) {
          const stickyP = playersInZone.find((p) => p.id === m._aggroOverrideTarget);
          if (stickyP) {
            const dxS = stickyP.x - m.x;
            const dyS = stickyP.y - m.y;
            nearest = stickyP;
            nearestDist = Math.sqrt(dxS * dxS + dyS * dyS);
          } else {
            m._aggroOverrideTarget = null;
            m._aggroOverrideUntil = 0;
          }
        }
        if (!nearest) {
          for (const p of playersInZone) {
            const dx = p.x - m.x;
            const dy = p.y - m.y;
            const dist = Math.sqrt(dx * dx + dy * dy);
            if (dist < nearestDist) {
              nearest = p;
              nearestDist = dist;
            }
          }
        }
        const ATTACK_RANGE = 45;
        const Y_SCALE = 3;
        const effAggroRange = stickyAggroActive ? 1200 : this.MONSTER_AGGRO_RANGE;
        if (nearest && nearestDist < effAggroRange) {
          m.targetId = nearest.id;
          const dxA = nearest.x - m.x;
          const dyA = nearest.y - m.y;
          const attackDist = Math.sqrt(dxA * dxA + dyA * Y_SCALE * (dyA * Y_SCALE));
          const attackingNow = m._attackingUntil && now < m._attackingUntil;
          if (attackDist > ATTACK_RANGE && !attackingNow) {
            const dx = nearest.x - m.x;
            const dy = nearest.y - m.y;
            const dist = Math.sqrt(dx * dx + dy * dy);
            if (dist > 0) {
              m.x += dx / dist * m.spd;
              m.y += dy / dist * m.spd;
              zoneChanged = true;
            }
          }
          if (attackDist <= ATTACK_RANGE && now > m.atkCd) {
            if (nearest.blocking) {
              m.atkCd = now + this.MONSTER_ATTACK_CD;
              m._attackingUntil = now + 400;
              const blockerPs = this.playerState[nearest.id];
              const staminaCost = 15;
              if (blockerPs && typeof blockerPs.stamina === "number") {
                blockerPs.stamina = Math.max(0, blockerPs.stamina - staminaCost);
                this._saveRpg(nearest.id, blockerPs);
                this._queuePlayerStateFlush(nearest.id);
              }
              this.eventBuffer.push({
                type: "monster_attack",
                payload: {
                  monsterId: m.id,
                  targetId: nearest.id,
                  dmg: m.dmg,
                  dmgTaken: 0,
                  blocked: true,
                  staminaDrain: staminaCost,
                  zone: zoneId,
                  attackerX: m.x,
                  attackerY: m.y
                }
              });
              continue;
            }
            m.atkCd = now + this.MONSTER_ATTACK_CD;
            m._attackingUntil = now + 400;
            const targetPs = this.playerState[nearest.id];
            const dmgResult = this._applyDamage(targetPs, m.dmg, false);
            const dmgTaken = dmgResult.dmgTaken;
            if (!dmgResult.dodged) {
              const trackAmt = dmgResult.graced ? dmgResult.dmgIntent || 0 : dmgTaken;
              this._trackMonsterDamage(targetPs, m.id, trackAmt);
            }
            this.eventBuffer.push({
              type: "monster_attack",
              payload: {
                monsterId: m.id,
                targetId: nearest.id,
                dmg: m.dmg,
                dmgTaken,
                dodged: dmgResult.dodged,
                zone: zoneId,
                attackerX: m.x,
                attackerY: m.y
              }
            });
            if (targetPs) {
              this._saveRpg(nearest.id, targetPs);
              this._queuePlayerStateFlush(nearest.id);
              if (targetPs.hp <= 0 && !targetPs.dying) {
                this._handlePlayerDeath(targetPs, nearest.id, "monster:" + m.id);
              }
            }
            zoneChanged = true;
          }
        } else {
          m.targetId = null;
          const WANDER_STEP_MIN = 30;
          const WANDER_STEP_MAX = 80;
          const WANDER_REACH = 6;
          const WANDER_PAUSE_MIN_MS = 500;
          const WANDER_PAUSE_MAX_MS = 1500;
          const WANDER_LEASH = 180;
          const distSpawn = Math.sqrt(
            (m.spawnX - m.x) * (m.spawnX - m.x) + (m.spawnY - m.y) * (m.spawnY - m.y)
          );
          if (distSpawn > WANDER_LEASH) {
            const dxL = m.spawnX - m.x;
            const dyL = m.spawnY - m.y;
            m.x += dxL / distSpawn * m.spd;
            m.y += dyL / distSpawn * m.spd;
            zoneChanged = true;
            m._wanderTx = null;
            m._wanderTy = null;
          } else if (m._wanderPausedUntil && now < m._wanderPausedUntil) {
          } else {
            if (m._wanderTx == null || m._wanderTy == null) {
              const ang = Math.random() * Math.PI * 2;
              const step = WANDER_STEP_MIN + Math.random() * (WANDER_STEP_MAX - WANDER_STEP_MIN);
              let tx = m.x + Math.cos(ang) * step;
              let ty = m.y + Math.sin(ang) * step;
              const dxC = tx - m.spawnX;
              const dyC = ty - m.spawnY;
              const distC = Math.sqrt(dxC * dxC + dyC * dyC);
              if (distC > WANDER_LEASH * 0.8) {
                const k = WANDER_LEASH * 0.8 / Math.max(distC, 1);
                tx = m.spawnX + dxC * k;
                ty = m.spawnY + dyC * k;
              }
              m._wanderTx = tx;
              m._wanderTy = ty;
            }
            const dxw = m._wanderTx - m.x;
            const dyw = m._wanderTy - m.y;
            const distw = Math.sqrt(dxw * dxw + dyw * dyw);
            if (distw < WANDER_REACH) {
              m._wanderTx = null;
              m._wanderTy = null;
              m._wanderPausedUntil = now + WANDER_PAUSE_MIN_MS + Math.random() * (WANDER_PAUSE_MAX_MS - WANDER_PAUSE_MIN_MS);
            } else {
              m.x += dxw / distw * m.spd;
              m.y += dyw / distw * m.spd;
              zoneChanged = true;
            }
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
    return [1, 6];
  }
  // Per-zone node count + type split.  Entry-level resource extraction
  // is zone-specialized -- one resource type per zone so each life-skill
  // has its own home base.  Other zones get no nodes until specific
  // resources are designed for them.
  //   - meadow:  fishing holes
  //   - hollows: ore veins (the rock zone)
  //   - frost:   trees (the snowy zone -- client renders these as pines)
  _getZoneNodeConfig(zoneId) {
    const ZONE_NODES = {
      meadow: { treeCt: 0, fishCt: 6, oreCt: 0 },
      hollows: { treeCt: 0, fishCt: 0, oreCt: 6 },
      frost: { treeCt: 6, fishCt: 0, oreCt: 0 }
    };
    return ZONE_NODES[zoneId] || { treeCt: 0, fishCt: 0, oreCt: 0 };
  }
  // Spawn the static node layout for a zone.  Positions are randomized
  // once at first-ever zone activation; after that they're fixed for
  // the lifetime of the Durable Object (re-randomized only on DO wake).
  _spawnZoneNodes(zoneId) {
    const zone = this._getZoneConfig(zoneId);
    if (!zone) return [];
    const W = zone.w * this.TILE;
    const H = zone.h * this.TILE;
    const margin = 8 * this.TILE;
    const cfg = this._getZoneNodeConfig(zoneId);
    const nodes = [];
    let idx = 0;
    const placeOne = /* @__PURE__ */ __name((type) => {
      const x = margin + Math.random() * (W - margin * 2);
      const y = margin + Math.random() * (H - margin * 2);
      const tierLvl = 1;
      nodes.push({
        id: "sn-" + zoneId + "-" + idx,
        nodeType: type,
        x,
        y,
        tierLvl,
        alive: true,
        respawnAt: 0
      });
      idx++;
    }, "placeOne");
    for (let i = 0; i < cfg.treeCt; i++) placeOne("tree");
    for (let i = 0; i < cfg.fishCt; i++) placeOne("fishSpot");
    for (let i = 0; i < cfg.oreCt; i++) placeOne("oreVein");
    return nodes;
  }
  _ensureZoneNodes(zoneId) {
    if (zoneId === "town" || zoneId === "farm_home") return [];
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
    const TREE = { 1: "Kindling", 6: "Softwood" };
    const FISH = { 1: "Minnow", 6: "Clownfish" };
    const ORE = { 1: "Copper Ore", 6: "Iron Ore" };
    const t = tierLvl || 1;
    if (nodeType === "tree") return TREE[t] || TREE[1];
    if (nodeType === "fishSpot") return FISH[t] || FISH[1];
    return ORE[t] || ORE[1];
  }
  _harvestResourceType(nodeType) {
    if (nodeType === "tree") return "wood";
    if (nodeType === "fishSpot") return "fish";
    return "ore";
  }
  _harvestInvKey(nodeType, tierLvl) {
    const name = this._harvestNameForTier(nodeType, tierLvl);
    const resType = this._harvestResourceType(nodeType);
    return resType + "_" + name.replace(/\s+/g, "_").toLowerCase();
  }
  _harvestYieldMult(accuracy) {
    if (accuracy === "perfect") return 2;
    return 1;
  }
  _harvestXpMult(accuracy) {
    if (accuracy === "perfect") return 2;
    if (accuracy === "good") return 1.5;
    return 1;
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
    if (claimed !== "perfect") return claimed || "ok";
    const now = Date.now();
    if (!Array.isArray(ps._perfectHistory)) ps._perfectHistory = [];
    ps._perfectHistory = ps._perfectHistory.filter((t) => now - t < 6e4);
    if (ps._perfectHistory.length >= 10) {
      return "good";
    }
    ps._perfectHistory.push(now);
    return "perfect";
  }
  _harvestSkillName(nodeType) {
    if (nodeType === "tree") return "woodcutting";
    if (nodeType === "fishSpot") return "fishing";
    return "mining";
  }
  // Base XP per harvest = ceil(tierLvl * 1.5 + 5); the accuracy
  // multiplier (xpMult above) is applied on top.  Mirrors the client
  // formula in createGatherNode (lifeSkills.js).
  _harvestXpForTier(tierLvl, accuracy) {
    const baseXp = Math.ceil((tierLvl || 1) * 1.5 + 5);
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
    return "shard_" + zoneId;
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
    if (L <= 30) return Math.ceil(500 * Math.pow(1.1, L - 1));
    const at30 = Math.ceil(500 * Math.pow(1.1, 29));
    if (L <= 65) return Math.ceil(at30 * Math.pow(1.07, L - 30));
    const at65 = Math.ceil(at30 * Math.pow(1.07, 35));
    if (L <= 100) return Math.ceil(at65 * Math.pow(1.04, L - 65));
    const at100 = Math.ceil(at65 * Math.pow(1.04, 35));
    return Math.ceil(at100 * Math.pow(1.08, L - 100));
  }
  // Accumulate combat XP for the bar / analytics only.  Per
  // docs/specs/build-points-gate-server.md, combat level-up is now
  // gated purely on build points (5 BP = 1 level, fired by the
  // build_point_earned event), not on XP thresholds.  killXp still
  // accumulates on ps.xp so the XP bar can repurpose into a BP bar
  // or analytics without losing the running total.
  _addCombatXp(ps, xpAmt) {
    if (!ps) return { leveled: false, levelsGained: 0, newLevel: 1 };
    ps.level = ps.level || 1;
    ps.xp = (ps.xp || 0) + (xpAmt || 0);
    return { leveled: false, levelsGained: 0, newLevel: ps.level };
  }
  // Drain build points into combat levels: every 5 BP = +1 level +
  // 5 unspentT2 + full pool restore.  Carries excess (10 BP → +2
  // levels).  Returns { leveled, levelsGained, newLevel } matching
  // the old _addCombatXp shape so combat_credit consumers keep
  // working unchanged.
  _tryLevelUpFromBuildPoints(ps) {
    if (!ps) return { leveled: false, levelsGained: 0, newLevel: 1 };
    ps.level = ps.level || 1;
    ps.unspentT2 = ps.unspentT2 || 0;
    ps.buildPointsThisLvl = ps.buildPointsThisLvl || 0;
    let levelsGained = 0;
    const LEVEL_CAP = 100;
    while (ps.level < LEVEL_CAP && ps.buildPointsThisLvl >= 5) {
      ps.buildPointsThisLvl -= 5;
      ps.level += 1;
      ps.unspentT2 += 5;
      levelsGained += 1;
    }
    if (levelsGained > 0) {
      this._recomputeMaxes(ps);
      if (typeof ps.maxHp === "number") ps.hp = ps.maxHp;
      if (typeof ps.maxStamina === "number") ps.stamina = ps.maxStamina;
      if (typeof ps.maxMana === "number") ps.mana = ps.maxMana;
    }
    return { leveled: levelsGained > 0, levelsGained, newLevel: ps.level };
  }
  _handleBuildPointEarned(session) {
    if (!session || !session.id) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    ps.buildPointsThisLvl = (ps.buildPointsThisLvl || 0) + 1;
    this._tryLevelUpFromBuildPoints(ps);
    this._saveRpg(session.id, ps);
    this._queuePlayerStateFlush(session.id);
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
    return stat === "power" || stat === "vitality" || stat === "endurance" || stat === "agility" || stat === "mind" || stat === "ferocity" || stat === "elementalMastery" || stat === "fortification" || stat === "restoration" || stat === "influence";
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
          type: "stat_allocated",
          payload: { stat, newUnspentT2: ps.unspentT2 }
        }));
      } catch (e) {
      }
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
    if (typeof invKey !== "string") return 0;
    if (!invKey.startsWith("cooked_fish_") && !invKey.startsWith("fish_")) return 0;
    const species = invKey.replace(/^(cooked_)?fish_/, "").toLowerCase();
    const TIERS = [
      { lvl: 1, name: "minnow" },
      { lvl: 6, name: "clownfish" },
      { lvl: 11, name: "trout" }
    ];
    const tier = TIERS.find((t) => species.includes(t.name));
    if (!tier) return 20;
    return Math.ceil(15 + tier.lvl * 8);
  }
  _handleEatRequest(session, payload) {
    if (!session || !session.id) return;
    const { invKey } = payload || {};
    if (typeof invKey !== "string") return;
    if (!invKey.startsWith("cooked_fish_")) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    if (!ps.inventory) ps.inventory = {};
    if ((ps.inventory[invKey] || 0) <= 0) return;
    const heal = this._fishHealAmount(invKey);
    if (heal <= 0) return;
    ps.inventory[invKey] -= 1;
    if (ps.inventory[invKey] <= 0) delete ps.inventory[invKey];
    if (typeof ps.maxHp !== "number") ps.maxHp = 100;
    if (typeof ps.hp !== "number") ps.hp = ps.maxHp;
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
    const T = { greatsword: 10, sword: 6.67, bow: 7.29, staff: 8.54 };
    return T[type] || 6.25;
  }
  // Sell value mirrors the client at BroTown.jsx ~26613:
  //   ceil((tierMult || 1) * (WEAPON_TYPES[type].base || 30) * 0.5)
  _weaponSellValue(weapon) {
    if (!weapon) return 0;
    const tierMult = typeof weapon.tierMult === "number" && weapon.tierMult > 0 ? weapon.tierMult : 1;
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
    return slot === "weapon" || slot === "rangedWeapon" || slot === "staffWeapon" || slot === "armor" || slot === "shield" || slot === "amulet";
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
      mayor_1: { gold: 50, xp: 30, next: "mayor_2" },
      mayor_2: { gold: 100, xp: 80, next: "mayor_3" },
      mayor_3: { gold: 300, xp: 200, next: null },
      trader_1: { gold: 25, xp: 20, next: "trader_2" },
      trader_2: { gold: 75, xp: 50, next: "trader_3" },
      trader_3: { gold: 150, xp: 100, next: null },
      enchant_1: { gold: 50, xp: 40, next: "enchant_2" },
      enchant_2: { gold: 200, xp: 150, next: "enchant_3" },
      enchant_3: { gold: 500, xp: 300, next: null },
      scout_1: { gold: 100, xp: 80, next: "scout_2" },
      scout_2: { gold: 200, xp: 150, next: null },
      bron_1: { gold: 60, xp: 40, next: "bron_2" },
      bron_2: { gold: 120, xp: 80, next: "bron_3" },
      bron_3: { gold: 200, xp: 150, next: "bron_4" },
      bron_4: { gold: 400, xp: 250, next: null },
      luna_1: { gold: 40, xp: 30, next: "luna_2" },
      luna_2: { gold: 100, xp: 70, next: "luna_3" },
      luna_3: { gold: 250, xp: 180, next: null },
      kai_1: { gold: 80, xp: 60, next: "kai_2" },
      kai_2: { gold: 200, xp: 120, next: "kai_3" },
      kai_3: { gold: 350, xp: 200, next: null },
      ash_1: { gold: 100, xp: 80, next: "ash_2" },
      ash_2: { gold: 250, xp: 180, next: "ash_3" },
      ash_3: { gold: 500, xp: 350, next: "ash_4" },
      ash_4: { gold: 800, xp: 500, next: null }
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
    if (typeof questId !== "string") return;
    const reward = this._QUEST_REWARDS_DATA()[questId];
    if (!reward) return;
    if (!ps._quests) ps._quests = {};
    const cur = ps._quests[questId];
    if (cur === "active" || cur === "turnedIn") return;
    ps._quests[questId] = "active";
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
    if (typeof questId !== "string") return;
    const reward = this._QUEST_REWARDS_DATA()[questId];
    if (!reward) return;
    if (!ps._quests) ps._quests = {};
    if (ps._quests[questId] !== "active") return;
    ps._quests[questId] = "turnedIn";
    ps.coins = (ps.coins || 0) + (reward.gold || 0);
    if (reward.xp > 0) {
      const { leveled } = this._addCombatXp(ps, reward.xp);
      if (leveled) {
        this._recomputeMaxes(ps);
        if (typeof ps.maxHp === "number") ps.hp = ps.maxHp;
        if (typeof ps.maxStamina === "number") ps.stamina = ps.maxStamina;
        if (typeof ps.maxMana === "number") ps.mana = ps.maxMana;
      }
    }
    ps.achievementPoints = (ps.achievementPoints || 0) + this.QUEST_AP_REWARD;
    if (reward.next && !ps._quests[reward.next]) {
      ps._quests[reward.next] = "available";
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
    return {
      wood: { minLvl: 1, slots: 1, oreName: "wood", oreCost: 3, goldCost: 8, tierMult: 1, statReq: 0 },
      copper: { minLvl: 6, slots: 1, oreName: "copper", oreCost: 3, goldCost: 20, tierMult: 1.12, statReq: 10 },
      iron: { minLvl: 11, slots: 1, oreName: "iron", oreCost: 4, goldCost: 35, tierMult: 1.25, statReq: 20 },
      steel: { minLvl: 16, slots: 1, oreName: "steel", oreCost: 5, goldCost: 55, tierMult: 1.4, statReq: 30 },
      titanium: { minLvl: 21, slots: 1, oreName: "titanium", oreCost: 5, goldCost: 85, tierMult: 1.56, statReq: 40 },
      obsidian: { minLvl: 26, slots: 1, oreName: "obsidian", oreCost: 6, goldCost: 120, tierMult: 1.74, statReq: 50 },
      mythril: { minLvl: 31, slots: 2, oreName: "mythril", oreCost: 7, goldCost: 170, tierMult: 1.94, statReq: 60 },
      diamond: { minLvl: 36, slots: 2, oreName: "diamond", oreCost: 8, goldCost: 240, tierMult: 2.16, statReq: 70 },
      abyssal: { minLvl: 41, slots: 2, oreName: "abyssal", oreCost: 9, goldCost: 330, tierMult: 2.4, statReq: 80 },
      dragonbone: { minLvl: 46, slots: 2, oreName: "dragonbone", oreCost: 10, goldCost: 440, tierMult: 2.68, statReq: 90 },
      shadowsteel: { minLvl: 51, slots: 2, oreName: "shadowsteel", oreCost: 11, goldCost: 570, tierMult: 2.98, statReq: 100 },
      bloodstone: { minLvl: 56, slots: 2, oreName: "bloodstone", oreCost: 12, goldCost: 720, tierMult: 3.32, statReq: 110 },
      runestone: { minLvl: 61, slots: 2, oreName: "runite", oreCost: 13, goldCost: 900, tierMult: 3.7, statReq: 120 },
      sunstone: { minLvl: 66, slots: 2, oreName: "sunstone", oreCost: 14, goldCost: 1100, tierMult: 4.12, statReq: 130 },
      demonite: { minLvl: 71, slots: 2, oreName: "demonite", oreCost: 15, goldCost: 1350, tierMult: 4.58, statReq: 140 },
      spiritforge: { minLvl: 76, slots: 2, oreName: "spiritore", oreCost: 16, goldCost: 1650, tierMult: 5.1, statReq: 150 },
      starforged: { minLvl: 81, slots: 2, oreName: "starite", oreCost: 18, goldCost: 2e3, tierMult: 5.68, statReq: 160 },
      celestial: { minLvl: 86, slots: 2, oreName: "celestite", oreCost: 20, goldCost: 2500, tierMult: 6.32, statReq: 170 },
      antimatter: { minLvl: 91, slots: 2, oreName: "antimatter", oreCost: 22, goldCost: 3200, tierMult: 7.04, statReq: 180 },
      worldbreaker: { minLvl: 96, slots: 2, oreName: "voidcrystal", oreCost: 25, goldCost: 4200, tierMult: 7.84, statReq: 190 }
    };
  }
  _WOODWORKING_TIERS_DATA() {
    return {
      wood: { minLvl: 1, slots: 1, wood: "wood", woodCost: 3, goldCost: 8, tierMult: 1, statReq: 0 },
      softwood: { minLvl: 6, slots: 1, wood: "softwood", woodCost: 3, goldCost: 20, tierMult: 1.12, statReq: 10 },
      hardwood: { minLvl: 11, slots: 1, wood: "hardwood", woodCost: 4, goldCost: 35, tierMult: 1.25, statReq: 20 },
      pine: { minLvl: 16, slots: 1, wood: "pine_lumber", woodCost: 5, goldCost: 55, tierMult: 1.4, statReq: 30 },
      maple: { minLvl: 21, slots: 1, wood: "maple_wood", woodCost: 5, goldCost: 85, tierMult: 1.56, statReq: 40 },
      ironbark: { minLvl: 26, slots: 1, wood: "ironbark", woodCost: 6, goldCost: 120, tierMult: 1.74, statReq: 50 },
      crystalwood: { minLvl: 31, slots: 2, wood: "crystal_wood", woodCost: 7, goldCost: 170, tierMult: 1.94, statReq: 60 },
      elder: { minLvl: 36, slots: 2, wood: "elder_wood", woodCost: 8, goldCost: 240, tierMult: 2.16, statReq: 70 },
      spiritwood: { minLvl: 41, slots: 2, wood: "spirit_wood", woodCost: 9, goldCost: 330, tierMult: 2.4, statReq: 80 },
      dragonwood: { minLvl: 46, slots: 2, wood: "dragon_wood", woodCost: 10, goldCost: 440, tierMult: 2.68, statReq: 90 },
      shadowthorn: { minLvl: 51, slots: 2, wood: "shadowthorn", woodCost: 11, goldCost: 570, tierMult: 2.98, statReq: 100 },
      bloodoak: { minLvl: 56, slots: 2, wood: "bloodoak", woodCost: 12, goldCost: 720, tierMult: 3.32, statReq: 110 },
      runewood: { minLvl: 61, slots: 2, wood: "runewood", woodCost: 13, goldCost: 900, tierMult: 3.7, statReq: 120 },
      sunbark: { minLvl: 66, slots: 2, wood: "sunbark", woodCost: 14, goldCost: 1100, tierMult: 4.12, statReq: 130 },
      demonwood: { minLvl: 71, slots: 2, wood: "demonwood", woodCost: 15, goldCost: 1350, tierMult: 4.58, statReq: 140 },
      ghostwood: { minLvl: 76, slots: 2, wood: "ghostwood", woodCost: 16, goldCost: 1650, tierMult: 5.1, statReq: 150 },
      starwood: { minLvl: 81, slots: 2, wood: "starwood", woodCost: 18, goldCost: 2e3, tierMult: 5.68, statReq: 160 },
      worldtree: { minLvl: 86, slots: 2, wood: "worldtree", woodCost: 20, goldCost: 2500, tierMult: 6.32, statReq: 170 },
      voidtimber: { minLvl: 91, slots: 2, wood: "void_timber", woodCost: 22, goldCost: 3200, tierMult: 7.04, statReq: 180 },
      worldbreaker: { minLvl: 96, slots: 2, wood: "voidwood", woodCost: 25, goldCost: 4200, tierMult: 7.84, statReq: 190 }
    };
  }
  // EQUIP_STAT_MAP mirror.  Used for the forge statReq gate.
  _equipStatFor(weaponType) {
    if (weaponType === "greatsword") return "power";
    if (weaponType === "sword") return "agility";
    if (weaponType === "bow") return "agility";
    if (weaponType === "staff") return "mind";
    return "power";
  }
  _handleForgeWeapon(session, payload) {
    if (!session || !session.id) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    const { weaponType, tierKey, isWoodwork } = payload || {};
    if (weaponType !== "greatsword" && weaponType !== "sword" && weaponType !== "bow" && weaponType !== "staff") return;
    if (typeof tierKey !== "string") return;
    const wantWw = weaponType === "bow" || weaponType === "staff";
    if (wantWw !== !!isWoodwork) return;
    const table = wantWw ? this._WOODWORKING_TIERS_DATA() : this._BLACKSMITH_TIERS_DATA();
    const tier = table[tierKey];
    if (!tier) return;
    const skillName = wantWw ? "woodworking" : "blacksmithing";
    const skillLvl = ps.lifeSkills && ps.lifeSkills[skillName] && ps.lifeSkills[skillName].level || 1;
    if (skillLvl < tier.minLvl) return;
    const reqStat = this._equipStatFor(weaponType);
    if ((ps[reqStat] || 0) < (tier.statReq || 0)) return;
    if ((ps.coins || 0) < tier.goldCost) return;
    if (!ps.inventory) ps.inventory = {};
    const resourceKey = wantWw ? "wood_" + tier.wood : "ore_" + tier.oreName + "_ore";
    const have = ps.inventory[resourceKey] || 0;
    const cost = wantWw ? tier.woodCost : tier.oreCost;
    if (have < cost) return;
    const slot = weaponType === "bow" ? "rangedWeapon" : weaponType === "staff" ? "staffWeapon" : "weapon";
    const current = ps[slot];
    if (current) {
      if (!Array.isArray(ps.weaponStash)) ps.weaponStash = [];
      if (ps.weaponStash.length >= this.WEAPON_STASH_CAP) return;
    }
    ps.inventory[resourceKey] -= cost;
    if (ps.inventory[resourceKey] <= 0) delete ps.inventory[resourceKey];
    ps.coins -= tier.goldCost;
    if (current) {
      ps.weaponStash.push(current);
    }
    ps[slot] = {
      type: weaponType,
      tier: "common",
      tierMult: tier.tierMult,
      element1: null,
      element2: null,
      isVolatile: false,
      // Name is built client-side from display label; server stores
      // gearBase so the client can reconstruct.
      name: tierKey + " " + weaponType,
      gearBase: wantWw ? "ww_" + tierKey : tierKey,
      reforgeBonus: null,
      hardenBonus: null
    };
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
    if (slot === "weapon" || slot === "rangedWeapon" || slot === "staffWeapon") {
      if (!Array.isArray(ps.weaponStash)) ps.weaponStash = [];
      if (ps.weaponStash.length >= this.WEAPON_STASH_CAP) return;
      ps.weaponStash.push(current);
    }
    ps[slot] = null;
    if (slot === "armor") this._recomputeMaxes(ps);
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
    const stashItem = ps.weaponStash[stashIdx];
    if (!stashItem) return;
    const activeItem = ps[slot] || null;
    ps[slot] = stashItem;
    if (activeItem) {
      ps.weaponStash[stashIdx] = activeItem;
    } else {
      ps.weaponStash.splice(stashIdx, 1);
    }
    if (ps.weaponStash.length > this.WEAPON_STASH_CAP) {
      ps.weaponStash.length = this.WEAPON_STASH_CAP;
    }
    if (slot === "armor") this._recomputeMaxes(ps);
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
    const RECIPES = [
      { ingredients: { herb_firebloom: 1 }, buff: "regen", power: 0.02, duration: 60, tier: 1 },
      { ingredients: { herb_rock_vine: 1, herb_cloudpetal: 1 }, buff: "resist", power: 0.05, duration: 60, tier: 1 },
      { ingredients: { herb_firebloom: 2 }, buff: "damage", power: 0.05, duration: 90, tier: 2 }
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
    return invKey === type || invKey === "cooked_" + type;
  }
  _consumeIngredient(ps, type, count) {
    if (!ps.inventory) return false;
    let remaining = count;
    let total = 0;
    for (const [k, v] of Object.entries(ps.inventory)) {
      if (this._ingredientMatches(k, type) && v > 0) total += v;
    }
    if (total < count) return false;
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
    for (const [type, count] of Object.entries(recipe.ingredients)) {
      let total = 0;
      for (const [k, v] of Object.entries(ps.inventory)) {
        if (this._ingredientMatches(k, type) && v > 0) total += v;
      }
      if (total < count) return;
    }
    for (const [type, count] of Object.entries(recipe.ingredients)) {
      this._consumeIngredient(ps, type, count);
    }
    if (!ps._buffs) ps._buffs = {};
    const dur = (recipe.duration || 0) * 1e3;
    const endsAt = Date.now() + dur;
    if (recipe.buff === "heal") {
      if (typeof ps.maxHp !== "number") ps.maxHp = 100;
      ps.hp = Math.min(ps.maxHp, (ps.hp || 0) + (recipe.power || 0));
    } else if (recipe.buff === "regen") {
      ps._buffs.regen = endsAt;
    } else if (recipe.buff === "resist") {
      ps._buffs.resist = endsAt;
    } else if (recipe.buff === "damage") {
      ps._buffs.damage = endsAt;
    } else if (recipe.buff === "all") {
      ps._buffs.damage = endsAt;
      ps._buffs.spd = endsAt;
      ps._buffs.hp = endsAt;
      ps._buffs.mana = endsAt;
    }
    this._addLifeSkillXp(ps, "cooking", (recipe.tier || 1) * 25);
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
      cookedMinnow: { cost: 8, effect: "healFish", power: 23 },
      basicTrap: { cost: 20, effect: "trap" },
      staminaSalts: { cost: 12, effect: "stamina", power: 60 },
      manaShard: { cost: 18, effect: "mana", power: 40 },
      whetstone: { cost: 35, effect: "dmgBuff" }
    };
    return TABLE[itemId] || null;
  }
  _handleShopPurchase(session, payload) {
    if (!session || !session.id) return;
    const { itemId } = payload || {};
    if (typeof itemId !== "string") return;
    const item = this._getShopItem(itemId);
    if (!item) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    const discount = Math.min(0.2, (ps.influence || 0) * 2e-3);
    const finalCost = Math.max(1, Math.floor(item.cost * (1 - discount)));
    if ((ps.coins || 0) < finalCost) return;
    ps.coins -= finalCost;
    if (item.effect === "healFish") {
      if (typeof ps.maxHp !== "number") ps.maxHp = 100;
      if (typeof ps.hp !== "number") ps.hp = ps.maxHp;
      ps.hp = Math.min(ps.maxHp, ps.hp + (item.power || 23));
    } else if (item.effect === "stamina") {
      if (typeof ps.maxStamina !== "number") ps.maxStamina = 100;
      if (typeof ps.stamina !== "number") ps.stamina = ps.maxStamina;
      ps.stamina = Math.min(ps.maxStamina, ps.stamina + (item.power || 60));
    } else if (item.effect === "mana") {
      if (typeof ps.maxMana !== "number") ps.maxMana = 100;
      if (typeof ps.mana !== "number") ps.mana = ps.maxMana;
      ps.mana = Math.min(ps.maxMana, ps.mana + (item.power || 40));
    } else if (item.effect === "trap") {
      if (!ps.inventory) ps.inventory = {};
      ps.inventory.basic_trap = (ps.inventory.basic_trap || 0) + 1;
    }
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
    if (typeof fishKey !== "string" || !fishKey.startsWith("fish_")) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (!ps.inventory) ps.inventory = {};
    if ((ps.inventory[fishKey] || 0) <= 0) return;
    ps.inventory[fishKey] -= 1;
    if (ps.inventory[fishKey] <= 0) delete ps.inventory[fishKey];
    if (kind === "cooked") {
      const cookedKey = "cooked_" + fishKey;
      ps.inventory[cookedKey] = (ps.inventory[cookedKey] || 0) + 1;
      this._addLifeSkillXp(ps, "cooking", 8);
    } else {
      ps.inventory.burnt_dust = (ps.inventory.burnt_dust || 0) + 1;
    }
    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) this._sendPlayerState(ws, session.id);
  }
  // Mirror of computeOpenDelay() in src/data/gameSystems.js, sans the
  // jitter sample -- returns the BASE delay so the validator can bound
  // the per-attempt window by base * (1 ± EXTRACT_JITTER).
  _computeOpenDelayBase(skillLevel, nodeTier) {
    const lvl = Number(skillLevel) || 0;
    const tier = Number(nodeTier) || 1;
    const gap = tier - lvl;
    let base;
    if (gap > 0) base = this.EXTRACT_OPEN_BASE + gap * 1200;
    else if (gap < 0) base = this.EXTRACT_OPEN_BASE + gap * 250;
    else base = this.EXTRACT_OPEN_BASE;
    return Math.max(this.EXTRACT_OPEN_MIN, Math.min(this.EXTRACT_OPEN_MAX, base));
  }
  // Sweep extraction entries past EXTRACTION_TIMEOUT_MS.  Walk-away
  // cancel is silent on the client -- the player just stops getting the
  // swipe cue; the server cleans up so the map doesn't grow unbounded.
  _sweepStaleExtractions(nowMs) {
    const cutoff = (nowMs || Date.now()) - this.EXTRACTION_TIMEOUT_MS;
    for (const sid of Object.keys(this.extractions)) {
      const e = this.extractions[sid];
      if (!e || e.startedAt < cutoff) delete this.extractions[sid];
    }
  }
  // Client sent extraction_start { nodeId, zone, skill } -- record what
  // we need to validate the eventual node_strike (the swipe-landed
  // event).  Server also captures skillLevel + nodeTier at the start so
  // a mid-attempt level-up doesn't shift the expected window.
  _handleExtractionStart(session, payload) {
    if (!session || !session.id) return;
    const { nodeId, zone, skill } = payload || {};
    if (!nodeId || !zone || !skill) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    const list = this.nodes[zone];
    if (!list) return;
    const n = list.find((x) => x.id === nodeId);
    if (!n || !n.alive) return;
    const skillLevel = ps.lifeSkills && ps.lifeSkills[skill] && ps.lifeSkills[skill].level || 0;
    const nodeTier = n.tierLvl || 1;
    this.extractions[session.id] = {
      nodeId,
      zone,
      skill,
      startedAt: Date.now(),
      skillLevel,
      nodeTier,
      openDelayBase: this._computeOpenDelayBase(skillLevel, nodeTier)
    };
  }
  _handleNodeStrike(session, payload) {
    if (!session || !session.id) return;
    const { id, zone, accuracy, swipeFp } = payload || {};
    if (!id || !zone) return;
    const list = this.nodes[zone];
    if (!list) return;
    const n = list.find((x) => x.id === id);
    if (!n || !n.alive) return;
    const ps = this.playerState[session.id];
    if (!ps || ps.z !== zone || ps.dead || ps.disconnected) return;
    const dx = ps.x - n.x;
    const dy = ps.y - n.y;
    if (dx * dx + dy * dy > this.LOOT_PICKUP_RANGE * this.LOOT_PICKUP_RANGE) return;
    const now = Date.now();
    const ex = this.extractions[session.id];
    let coercedAccuracy = accuracy || "good";
    let openLatencyMs = null;
    if (ex && ex.nodeId === id && ex.zone === zone) {
      const jitterLo = 1 - this.EXTRACT_JITTER;
      const jitterHi = 1 + this.EXTRACT_JITTER;
      const earliestOpen = ex.startedAt + Math.floor(ex.openDelayBase * jitterLo) - this.EXTRACTION_GRACE_MS;
      const latestClose = ex.startedAt + Math.ceil(ex.openDelayBase * jitterHi) + this.EXTRACT_WINDOW_MS + this.EXTRACTION_GRACE_MS;
      if (now < earliestOpen) {
        if (!session._extractionRejects) session._extractionRejects = 0;
        session._extractionRejects++;
        return;
      }
      if (now > latestClose) {
        coercedAccuracy = "miss";
      }
      openLatencyMs = now - earliestOpen;
    } else if (!ex) {
      if (!session._extractionMissing) session._extractionMissing = 0;
      session._extractionMissing++;
    }
    delete this.extractions[session.id];
    if (swipeFp && typeof swipeFp === "object" && coercedAccuracy === "good") {
      if (!session._swipeFps) session._swipeFps = [];
      const fp = {
        ts: now,
        nodeId: id,
        len: Number(swipeFp.len) || 0,
        ent: Number(swipeFp.ent) || 0,
        dur: Number(swipeFp.dur) || 0,
        latency: openLatencyMs
      };
      session._swipeFps.push(fp);
      if (session._swipeFps.length > this.SWIPE_FP_CAP_PER_SESSION) {
        session._swipeFps.shift();
      }
    }
    if (openLatencyMs != null && coercedAccuracy === "good") {
      if (!session._extractionLatencies) session._extractionLatencies = [];
      session._extractionLatencies.push(openLatencyMs);
      if (session._extractionLatencies.length > this.LATENCY_CAP_PER_SESSION) {
        session._extractionLatencies.shift();
      }
    }
    n.alive = false;
    n.respawnAt = Date.now() + this.NODE_RESPAWN_TIME;
    this.dirtyNodes.add(zone);
    if (coercedAccuracy === "miss") return;
    const ratedAccuracy = this._ratedHarvestAccuracy(ps, accuracy);
    const invKey = this._harvestInvKey(n.nodeType, n.tierLvl);
    const yieldQty = this._harvestYieldMult(ratedAccuracy);
    if (!ps.inventory) ps.inventory = {};
    ps.inventory[invKey] = (ps.inventory[invKey] || 0) + yieldQty;
    const skillName = this._harvestSkillName(n.nodeType);
    const xpAmt = this._harvestXpForTier(n.tierLvl, ratedAccuracy);
    const { leveled, newLevel } = this._addLifeSkillXp(ps, skillName, xpAmt);
    const shard = this._rollHarvestShard(n.zoneId || zone);
    if (shard) {
      ps.inventory[shard] = (ps.inventory[shard] || 0) + 1;
    }
    this._saveRpg(session.id, ps);
    const ws = this._wsBySessionId(session.id);
    if (ws) {
      this._sendPlayerState(ws, session.id);
      try {
        ws.send(JSON.stringify({
          type: "harvest_credit",
          payload: {
            nodeId: id,
            zone,
            skillName,
            xpAmt,
            leveled,
            newLevel,
            shard
          }
        }));
      } catch (e) {
      }
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
    return arch === "fodder" || arch === "snowman" || arch === "fireGoblin" || arch === "mummy" || arch === "skeleton";
  }
  _rollShardForKill(zoneId) {
    if (Math.random() >= this.SHARD_DROP_RATE) return null;
    return "shard_" + zoneId;
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
    if (skull === "fodder") return "slime-remnants";
    if (skull === "fireGoblin") return "fire-goblin-remnants";
    if (skull === "mummy" || skull === "skeleton") return "skeleton-remnants";
    return skull;
  }
  async _loadRpg(playerId) {
    try {
      const stored = await this.state.storage.get("rpg:" + playerId);
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
      if (typeof ps._buffs[k] !== "number" || ps._buffs[k] <= now) {
        delete ps._buffs[k];
      }
    }
  }
  async _saveRpg(playerId, ps) {
    if (!playerId || !ps) return;
    this._pruneBuffs(ps);
    try {
      await this.state.storage.put("rpg:" + playerId, {
        coins: ps.coins || 0,
        inventory: ps.inventory || {},
        lifeSkills: ps.lifeSkills || {},
        level: ps.level || 1,
        xp: ps.xp || 0,
        unspentT2: ps.unspentT2 || 0,
        buildPointsThisLvl: ps.buildPointsThisLvl || 0,
        hp: typeof ps.hp === "number" ? ps.hp : 100,
        maxHp: typeof ps.maxHp === "number" ? ps.maxHp : 100,
        stamina: typeof ps.stamina === "number" ? ps.stamina : 100,
        maxStamina: typeof ps.maxStamina === "number" ? ps.maxStamina : 100,
        mana: typeof ps.mana === "number" ? ps.mana : 100,
        maxMana: typeof ps.maxMana === "number" ? ps.maxMana : 100,
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
        activeSlot: ps.activeSlot || "melee",
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
        _perfectHistory: Array.isArray(ps._perfectHistory) ? ps._perfectHistory : []
      });
    } catch (e) {
    }
  }
  // Queue a player_state emit for the next tick flush.  Used by
  // tick-path mutators (regen, monster attack, respawn, combat XP)
  // to coalesce multiple per-tick mutations into one wire emit per
  // affected player.  Action handlers (eat / shop / forge / etc.)
  // still call _sendPlayerState directly for immediate response.
  _queuePlayerStateFlush(playerId) {
    if (playerId) this.pendingPlayerStateFlush.add(playerId);
  }
  _flushPendingPlayerStates() {
    if (this.pendingPlayerStateFlush.size === 0) return;
    for (const id of this.pendingPlayerStateFlush) {
      const ws = this._wsBySessionId(id);
      if (ws) this._sendPlayerState(ws, id);
    }
    this.pendingPlayerStateFlush.clear();
  }
  _sendPlayerState(ws, playerId) {
    const ps = this.playerState[playerId];
    if (!ps || !ws) return;
    try {
      ws.send(JSON.stringify({
        type: "player_state",
        payload: {
          coins: ps.coins || 0,
          inventory: ps.inventory || {},
          lifeSkills: ps.lifeSkills || {},
          level: ps.level || 1,
          xp: ps.xp || 0,
          unspentT2: ps.unspentT2 || 0,
          buildPointsThisLvl: ps.buildPointsThisLvl || 0,
          hp: typeof ps.hp === "number" ? ps.hp : ps.maxHp || 100,
          maxHp: typeof ps.maxHp === "number" ? ps.maxHp : 100,
          stamina: typeof ps.stamina === "number" ? ps.stamina : ps.maxStamina || 100,
          maxStamina: typeof ps.maxStamina === "number" ? ps.maxStamina : 100,
          mana: typeof ps.mana === "number" ? ps.mana : ps.maxMana || 100,
          maxMana: typeof ps.maxMana === "number" ? ps.maxMana : 100,
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
          activeSlot: ps.activeSlot || "melee",
          armor: ps.armor || null,
          shield: ps.shield || null,
          amulet: ps.amulet || null,
          weaponStash: Array.isArray(ps.weaponStash) ? ps.weaponStash.slice(0, this.WEAPON_STASH_CAP) : [],
          // Quest state mirror (slice 17).
          _quests: ps._quests || {},
          _questFlags: ps._questFlags || {},
          _questKills: ps._questKills || {},
          achievementPoints: ps.achievementPoints || 0
        }
      }));
    } catch (e) {
    }
  }
  // ═══ HP store + damage application (server-authoritative) ═══
  //
  // Server owns current hp; clamps to [0, maxHp].  Damage flows through
  // Per docs/specs/t1-t2-stat-redesign-server.md:
  //   - Phase 1: `def` reduction retired -- armor now folds into maxHp
  //     via _armorHp, no per-hit damage reduction.  Resist cooking buff
  //     still applies (separate mechanic).
  //   - Phase 4: Agility rolls a per-hit passive dodge, capped at 30%.
  //     A successful roll zeros the hit; the caller emits a dodged: true
  //     event so the client can render the popup.
  //   - Phase 2: Full block invuln stays for the monster→player path
  //     (caller short-circuits when blocking) and is enforced here via
  //     isBlock=true (PvP partial-block path callers can opt in).
  //
  // Returns { dmgTaken, dodged } -- dmgTaken is 0 for both block and
  // dodge, dodged disambiguates so the caller can route to the right
  // popup.
  _applyDamage(ps, rawDmg, isBlock) {
    if (!ps) return { dmgTaken: 0, dodged: false, graced: false };
    const r = Math.max(1, Math.round(rawDmg || 0));
    if (ps._zoneEntryGraceUntil && Date.now() < ps._zoneEntryGraceUntil) {
      return { dmgTaken: 0, dodged: false, graced: true, dmgIntent: r };
    }
    if (isBlock) {
      ps.lastDamageAt = Date.now();
      return { dmgTaken: 0, dodged: false };
    }
    const dodgePct = Math.min((ps.agility || 0) * 8e-4, 0.3);
    if (Math.random() < dodgePct) {
      ps.lastDamageAt = Date.now();
      return { dmgTaken: 0, dodged: true };
    }
    let dmgTaken = Math.max(1, r);
    if (this._buffActive(ps, "resist")) {
      dmgTaken = Math.max(1, Math.ceil(dmgTaken * (1 - 0.05)));
    }
    if (typeof ps.maxHp !== "number") ps.maxHp = 100;
    if (typeof ps.hp !== "number") ps.hp = ps.maxHp;
    ps.hp = Math.max(0, ps.hp - dmgTaken);
    ps.lastDamageAt = Date.now();
    return { dmgTaken, dodged: false };
  }
  // ═══ Melee lifesteal (per docs/specs/lifesteal-server.md) ═══
  //
  // Track net damage each monster has dealt to a player; on a melee
  // kill, refund 90% of that accumulated amount as healing.  Only
  // melee kills qualify (ranged/staff use a separate vitality-progress
  // path, not health).  Ephemeral session state -- not persisted.
  _trackMonsterDamage(ps, monsterId, amount) {
    if (!ps || !monsterId || !(amount > 0)) return;
    if (!ps.dmgFromMonster) ps.dmgFromMonster = {};
    ps.dmgFromMonster[monsterId] = (ps.dmgFromMonster[monsterId] || 0) + amount;
  }
  // slotOverride: if the client passed an explicit slot in monster_damage
  // (the slot the killing hit was actually struck with), trust that
  // over ps.activeSlot.  ps.activeSlot only updates when the client
  // sends set_active_slot, which the desktop slot-select UI skips --
  // a stale 'ranged' value there silently kills lifesteal for what the
  // player sees as a melee swing.
  //
  // Returns { refund, reason }.  reason is one of:
  //   'ok'           — heal applied, refund > 0
  //   'no-ps'        — attackerPs missing (player disconnected mid-kill)
  //   'not-melee'    — slot resolved to ranged/staff (denied by design)
  //   'no-damage'    — dmgFromMonster map empty (player took no damage from any monster)
  //   'no-this-mon'  — player took damage but not from this specific monster
  // Caller can use reason to surface debug info in the lifesteal_credit
  // event so a "no heal" outcome is diagnosable.
  _applyMeleeLifesteal(ps, monsterId, slotOverride) {
    if (!ps || !monsterId) return { refund: 0, reason: "no-ps" };
    const slot = slotOverride || ps.activeSlot || "melee";
    if (slot !== "melee") return { refund: 0, reason: "not-melee" };
    if (!ps.dmgFromMonster) return { refund: 0, reason: "no-damage" };
    const taken = ps.dmgFromMonster[monsterId] || 0;
    if (taken <= 0) return { refund: 0, reason: "no-this-mon" };
    const refund = Math.ceil(taken * 0.9);
    const maxHp = ps.maxHp || 100;
    ps.hp = Math.min(maxHp, (ps.hp || 0) + refund);
    delete ps.dmgFromMonster[monsterId];
    return { refund, reason: "ok" };
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
  // Armor HP contribution -- mirrors getArmorHp() in
  // src/data/gameSystems.js per docs/specs/t1-t2-stat-redesign-server.md.
  // Phase 1: armor went from damage-reduction (def) to flat-HP.
  // tierMult is clamped to a defensive ceiling (8) so a forged-shape
  // armor with `tierMult: 999` can't inflate maxHp out of bounds.
  _armorHp(armor, vitality) {
    if (!armor) return 0;
    const ARMOR_HP_BASE = 20;
    const ARMOR_TIER_MULT_CAP = 8;
    const tmRaw = typeof armor.tierMult === "number" && armor.tierMult > 0 ? armor.tierMult : 1;
    const tm = Math.min(ARMOR_TIER_MULT_CAP, tmRaw);
    return Math.floor(ARMOR_HP_BASE * tm * (1 + (vitality || 0) * 0.01));
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
    ps.maxHp = this._calcMaxHp(lvl, ps.vitality || 0) + this._armorHp(ps.armor, ps.vitality || 0);
    ps.maxStamina = this._calcMaxStamina(ps.endurance || 0);
    ps.maxMana = this._calcMaxMana(ps.mind || 0);
    if (typeof ps.hp !== "number") ps.hp = ps.maxHp;
    ps.hp = Math.min(ps.hp, ps.maxHp);
    if (typeof ps.stamina !== "number") ps.stamina = ps.maxStamina;
    ps.stamina = Math.min(ps.stamina, ps.maxStamina);
    if (typeof ps.mana !== "number") ps.mana = ps.maxMana;
    ps.mana = Math.min(ps.mana, ps.maxMana);
  }
  _handleStatsUpdate(session, payload) {
    if (!session || !session.id) return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    const lvl = ps.level || 1;
    const T1_STATS = ["power", "vitality", "endurance", "agility", "mind"];
    const T2_STATS = ["ferocity", "elementalMastery", "fortification", "restoration", "influence"];
    const T2_CAP = 99;
    let statsChanged = false;
    for (const s of T1_STATS) {
      if (typeof payload[s] === "number") {
        const clamped = this._clampStat(payload[s], lvl);
        if (ps[s] !== clamped) {
          ps[s] = clamped;
          statsChanged = true;
        }
      }
    }
    for (const s of T2_STATS) {
      if (typeof payload[s] === "number") {
        const clamped = Math.max(0, Math.min(T2_CAP, Math.floor(payload[s])));
        if (ps[s] !== clamped) {
          ps[s] = clamped;
          statsChanged = true;
        }
      }
    }
    if ("armor" in payload) {
      const incoming = payload.armor;
      let newArmor = null;
      if (incoming && typeof incoming === "object" && incoming.name !== "Leather Armor") {
        newArmor = { ...incoming };
        if (typeof newArmor.tierMult === "number") {
          newArmor.tierMult = Math.max(0, Math.min(8, newArmor.tierMult));
        }
      }
      const oldSig = ps.armor ? JSON.stringify(ps.armor) : "null";
      const newSig = newArmor ? JSON.stringify(newArmor) : "null";
      if (oldSig !== newSig) {
        ps.armor = newArmor;
        statsChanged = true;
      }
    }
    if (statsChanged) {
      this._recomputeMaxes(ps);
    }
    const defCap = lvl * 20 + 100;
    if (typeof payload.def === "number") {
      ps.def = Math.max(0, Math.min(defCap, payload.def));
    }
    if (typeof payload.amuletHpRegen === "number") {
      ps.amuletHpRegen = Math.max(0, Math.min(100, payload.amuletHpRegen));
    }
    if (typeof payload.amuletStaminaRegen === "number") {
      ps.amuletStaminaRegen = Math.max(0, Math.min(100, payload.amuletStaminaRegen));
    }
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
    if (type === "dodge") return Math.ceil((ps.maxStamina || 100) * 0.2);
    if (type === "lunge") return Math.ceil((ps.maxStamina || 100) * 0.25);
    if (type === "retreat") return Math.ceil((ps.maxStamina || 100) * 0.2);
    if (type === "swipe") return Math.floor((ps.maxMana || 100) / 5);
    return 0;
  }
  _abilityPool(type) {
    if (type === "swipe") return "mana";
    return "stamina";
  }
  _handleAbilityUse(session, payload) {
    if (!session || !session.id) return;
    const { type, tier } = payload || {};
    if (type !== "dodge" && type !== "lunge" && type !== "retreat" && type !== "swipe") return;
    const ps = this.playerState[session.id];
    if (!ps) return;
    if (ps.dying || ps.dead || ps.disconnected) return;
    const cost = this._abilityCost(ps, type, tier);
    const pool = this._abilityPool(type);
    const have = pool === "mana" ? ps.mana || 0 : ps.stamina || 0;
    const ws = this._wsBySessionId(session.id);
    if (have < cost) {
      if (ws) {
        try {
          ws.send(JSON.stringify({
            type: "ability_rejected",
            payload: { type, pool, cost, have }
          }));
        } catch (e) {
        }
      }
      return;
    }
    if (pool === "mana") {
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
    ps.respawnAt = Date.now() + 5e3;
    ps.dmgFromMonster = {};
    this._spawnDeathPile(ps, playerId);
    ps.inventory = {};
    this._saveRpg(playerId, ps);
    this._queuePlayerStateFlush(playerId);
    const ws = this._wsBySessionId(playerId);
    if (ws) {
      try {
        ws.send(JSON.stringify({
          type: "player_died",
          payload: { cause: cause || "unknown", respawnInMs: 5e3 }
        }));
      } catch (e) {
      }
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
      ps.z = "town";
      ps.lastDamageAt = 0;
      ps.inventory = {};
      ps.dmgFromMonster = {};
      this._saveRpg(id, ps);
      const ws = this._wsBySessionId(id);
      if (ws) {
        try {
          ws.send(JSON.stringify({
            type: "player_respawned",
            payload: { zone: "town" }
          }));
        } catch (e) {
        }
        this._queuePlayerStateFlush(id);
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
      if (typeof ps.hp !== "number" || typeof ps.maxHp !== "number") continue;
      const ooc = now - (ps.lastDamageAt || 0) > 5e3;
      const oocMana = now - (ps.lastDamageAt || 0) > 2e3;
      let changed = false;
      if ((ps.z === "town" || ps.z === "farm_home") && ps.hp < ps.maxHp) {
        const heal = Math.max(1, Math.ceil(ps.maxHp * 0.1));
        const beforeHp = ps.hp;
        ps.hp = Math.min(ps.maxHp, ps.hp + heal);
        if (ps.hp !== beforeHp) changed = true;
      }
      if (typeof ps.maxStamina === "number" && typeof ps.stamina === "number") {
        if (ps.blocking && ps.stamina > 0) {
          const beforeSt = ps.stamina;
          ps.stamina = Math.max(0, ps.stamina - 5);
          if (ps.stamina !== beforeSt) changed = true;
          if (ps.stamina <= 0) {
            ps.blocking = false;
          }
        } else if (ps.stamina < ps.maxStamina) {
          const stAmuletMult = 1 + (ps.amuletStaminaRegen || 0) / 100;
          const stRestMult = 1 + (ps.restoration || 0) * 1e-3;
          const stEndMult = 1 + (ps.endurance || 0) * 2e-3;
          const stHeal = Math.max(1, Math.ceil(7 * stAmuletMult * stRestMult * stEndMult));
          const beforeSt = ps.stamina;
          ps.stamina = Math.min(ps.maxStamina, ps.stamina + stHeal);
          if (ps.stamina !== beforeSt) changed = true;
        }
      }
      const manaBuffActive = this._buffActive(ps, "mana");
      if (typeof ps.maxMana === "number" && typeof ps.mana === "number" && ps.mana < ps.maxMana) {
        const restMult = 1 + (ps.restoration || 0) * 1e-3;
        const buffMult = manaBuffActive ? 1.3 : 1;
        const mindMult = 1 + (ps.mind || 0) * 1e-3;
        const rate = oocMana ? 0.018 : 7e-3;
        const manaHeal = Math.max(1, Math.ceil(ps.maxMana * rate * restMult * buffMult * mindMult));
        const beforeMn = ps.mana;
        ps.mana = Math.min(ps.maxMana, ps.mana + manaHeal);
        if (ps.mana !== beforeMn) changed = true;
      }
      if (changed) {
        this._saveRpg(id, ps);
        this._queuePlayerStateFlush(id);
      }
    }
  }
  // Pile shape (server-side, full):
  //   { lootId, zone, x, y, coins, skull, shard, recipients,
  //     shares: {pid: number}, killerName, ts, inventoryClaimed,
  //     claimedBy: {pid: true} }
  _spawnLootForKill(zone, monster, killerSessionId, recipients, shares) {
    const lootId = "mk-" + monster.id;
    const skullSource = monster.variant || monster.arch;
    const skull = this._isRemnantSkullArch(skullSource) ? skullSource : null;
    const shard = this._rollShardForKill(zone);
    if (!skull && (!recipients || recipients.length === 0) && monster.gold <= 0 && !shard) {
      return null;
    }
    const killerSession = this._sessionById(killerSessionId);
    const killerName = killerSession && killerSession.name || "Player";
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
      claimedBy: {}
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
      x: p.x,
      y: p.y,
      coins: p.coins,
      skull: p.skull,
      shard: p.shard,
      recipients: p.recipients,
      shares: p.shares,
      killerName: p.killerName,
      ts: p.ts,
      inventoryClaimed: p.inventoryClaimed,
      // Death-drop fields (null for normal monster-kill piles).
      isDeathDrop: p.isDeathDrop || false,
      deathItems: p.deathItems || null,
      expiry: p.expiry || null,
      // Death drops are owner-only until ownerOnlyUntil, then free-
      // for-all until expiry.  Null for monster-kill piles.
      ownerOnlyUntil: p.ownerOnlyUntil || null
    };
  }
  // Spawn a death pile at the dying player's location carrying their
  // entire general inventory (mummy remains, fish, wood, etc.).
  // Equipped loadout + weaponStash are NOT included -- caller wipes
  // ps.inventory after this returns.  Anyone in the zone can pick the
  // pile up (recipients=null bypasses the recipient gate in
  // _handleLootPickup); first picker gets everything.  TTL is the
  // standard LOOT_EXPIRY_MS (60 s) so _tickLoot despawns it on schedule.
  _spawnDeathPile(ps, playerId) {
    if (!ps || !ps.inventory) return null;
    const items = [];
    for (const [k, v] of Object.entries(ps.inventory)) {
      const qty = Math.floor(Number(v) || 0);
      if (qty > 0) items.push({ key: k, qty });
    }
    if (items.length === 0) return null;
    const zone = ps.z;
    if (!zone || zone === "town" || zone === "farm_home") return null;
    const session = this._sessionById(playerId);
    const ownerName = session && session.name || "Player";
    const pile = {
      lootId: "dd-" + playerId + "-" + Date.now(),
      zone,
      x: ps.x || 0,
      y: ps.y || 0,
      coins: 0,
      skull: null,
      shard: null,
      // Recipients = just the dying player for the owner-only window;
      // after DEATH_PILE_OWNER_MS the server-side _handleLootPickup
      // and client-side recipient gate both flip to free-for-all so
      // anyone in zone may claim (driven by ownerOnlyUntil + isDeathDrop).
      recipients: [playerId],
      shares: {},
      killerName: ownerName,
      ts: Date.now(),
      inventoryClaimed: false,
      claimedBy: {},
      isDeathDrop: true,
      deathItems: items,
      ownerOnlyUntil: Date.now() + this.DEATH_PILE_OWNER_MS,
      expiry: Date.now() + this.DEATH_PILE_TOTAL_MS
    };
    if (!this.loot[zone]) this.loot[zone] = [];
    this.loot[zone].push(pile);
    this.eventBuffer.push({
      type: "loot_drop",
      payload: { pile: this._serializePile(pile) }
    });
    return pile;
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
      type: "loot_despawn",
      payload: { lootId, zone }
    });
  }
  _tickLoot() {
    const now = Date.now();
    for (const zoneId of Object.keys(this.loot)) {
      const list = this.loot[zoneId];
      if (!list) continue;
      for (let i = list.length - 1; i >= 0; i--) {
        const p = list[i];
        const expired = p.expiry ? now > p.expiry : now - p.ts > this.LOOT_EXPIRY_MS;
        if (expired) {
          list.splice(i, 1);
          this.eventBuffer.push({
            type: "loot_despawn",
            payload: { lootId: p.lootId, zone: zoneId }
          });
        }
      }
    }
  }
  _handleLootPickup(session, payload) {
    const reject = /* @__PURE__ */ __name((reason, extra) => {
      if (!session || !session.id) return;
      const ws2 = this._wsBySessionId(session.id);
      if (!ws2) return;
      try {
        ws2.send(JSON.stringify({
          type: "loot_pickup_rejected",
          payload: { lootId: payload && payload.lootId || null, zone: payload && payload.zone || null, reason, ...extra || {} }
        }));
      } catch (e) {
      }
    }, "reject");
    if (!session || !session.id) return;
    const { lootId, zone } = payload || {};
    if (!lootId || !zone) return reject("bad-payload");
    const list = this.loot[zone];
    if (!list) return reject("no-loot-zone");
    const pile = list.find((p) => p.lootId === lootId);
    if (!pile) return reject("no-pile");
    if (pile.claimedBy[session.id]) return reject("already-claimed");
    const deathFreeForAll = pile.isDeathDrop && pile.ownerOnlyUntil && Date.now() > pile.ownerOnlyUntil;
    if (!deathFreeForAll && pile.recipients && !pile.recipients.includes(session.id)) {
      return reject("not-recipient", { mySession: session.id, recipients: pile.recipients });
    }
    const ps = this.playerState[session.id];
    if (!ps) return reject("no-ps");
    if (ps.z !== zone) return reject("wrong-zone", { psZ: ps.z });
    if (ps.dead) return reject("dead");
    if (ps.disconnected) return reject("disconnected");
    const dx = ps.x - pile.x;
    const dy = ps.y - pile.y;
    const distSq = dx * dx + dy * dy;
    const rangeSq = this.LOOT_PICKUP_RANGE * this.LOOT_PICKUP_RANGE;
    if (distSq > rangeSq) return reject("out-of-range", { dist: Math.round(Math.sqrt(distSq)), max: this.LOOT_PICKUP_RANGE });
    if (pile.isDeathDrop) {
      if (!ps.inventory) ps.inventory = {};
      const itemsForMe = [];
      for (const it of pile.deathItems || []) {
        const key = it && it.key;
        const qty = Math.floor(Number(it && it.qty) || 0);
        if (!key || qty <= 0) continue;
        ps.inventory[key] = (ps.inventory[key] || 0) + qty;
        itemsForMe.push({ key, qty });
      }
      pile.claimedBy[session.id] = true;
      pile.inventoryClaimed = true;
      this._saveRpg(session.id, ps);
      const ws2 = this._wsBySessionId(session.id);
      if (ws2) {
        try {
          ws2.send(JSON.stringify({
            type: "loot_credit",
            payload: {
              lootId,
              zone,
              coins: 0,
              skull: null,
              shard: null,
              items: itemsForMe,
              isDeathDrop: true
            }
          }));
        } catch (e) {
        }
        this._sendPlayerState(ws2, session.id);
      }
      this.eventBuffer.push({
        type: "loot_claimed",
        payload: { lootId, zone, byPlayer: session.id, inventoryClaimedNow: true }
      });
      this._despawnLoot(zone, lootId);
      return;
    }
    const share = pile.shares[session.id] || 0;
    const coinsForMe = Math.round(pile.coins * share);
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
    const ws = this._wsBySessionId(session.id);
    if (ws) {
      try {
        ws.send(JSON.stringify({
          type: "loot_credit",
          payload: {
            lootId,
            zone,
            coins: coinsForMe,
            skull: skullForMe,
            shard: shardForMe
          }
        }));
      } catch (e) {
      }
      this._sendPlayerState(ws, session.id);
    }
    this.eventBuffer.push({
      type: "loot_claimed",
      payload: { lootId, zone, byPlayer: session.id, inventoryClaimedNow }
    });
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
  _maxWeaponDmg(ps, isSpecial) {
    if (!ps) return 0;
    const candidates = [ps.weapon, ps.rangedWeapon, ps.staffWeapon].filter(Boolean);
    if (candidates.length === 0) return 6.25;
    let max = 0;
    const statBonus = isSpecial ? (ps.mind || 0) * 0.1667 : (ps.power || 0) * 0.1667;
    for (const w of candidates) {
      const base = (this._weaponBase(w.type) + statBonus) * (w.tierMult || 1);
      if (base > max) max = base;
    }
    return max;
  }
  _maxDmgForAttacker(ps, isSpecial) {
    if (!ps) return 21;
    const maxWpn = this._maxWeaponDmg(ps, isSpecial);
    const critMult = 1.5 + (ps.power || 0) * 1e-3 + (ps.ferocity || 0) * 8e-4;
    const comboBoost = 5;
    const specialMult = isSpecial ? 2 : 1;
    return Math.max(21, Math.ceil(maxWpn * critMult * comboBoost * specialMult));
  }
  // Server-authoritative player->monster damage roll.  Mirrors the
  // client's calcWeaponDmg / calcSpecialDmg (src/data/gameSystems.js)
  // plus calcCritChance / calcCritMult, on the baseline-10 (÷4.8) scale.
  // The client now sends an INTENT (which slot, special or not) instead
  // of a damage number -- the server rolls the actual value here so the
  // last client-trusted-damage cheat vector is closed.
  //
  // NOTE (scoped per the server-computed-damage spec): this roll covers
  // weapon base + governing stat + per-type variance + special (2x) +
  // volatile (1.3x) + cooked damage buff (1.2x) + crit.  It deliberately
  // omits amulet elemDmg / elementalMastery / curse / elemental-collision
  // combo damage -- those stay client-side for now and are a follow-up
  // slice (the server has no elemental-status model).  So an elemental
  // combo build's authoritative damage is weapon-only until then.
  _computeAttackDamage(ps, slot, isSpecial) {
    if (!ps) return { dmg: 1, isCrit: false };
    const eff = slot === "melee" || slot === "ranged" || slot === "staff" ? slot : ps.activeSlot || "melee";
    const w = eff === "ranged" ? ps.rangedWeapon : eff === "staff" ? ps.staffWeapon : ps.weapon;
    const type = w && w.type || "greatsword";
    const tierMult = w && w.tierMult || 1;
    const stat = isSpecial ? ps.mind || 0 : type === "bow" ? ps.agility || 0 : type === "staff" ? ps.mind || 0 : ps.power || 0;
    let base = (this._weaponBase(type) + stat * 0.1667) * tierMult;
    const v = type === "staff" ? 0.5 + Math.random() * 1 : type === "bow" ? 0.6 + Math.random() * 0.2 : 0.75 + Math.random() * 0.5;
    base *= v;
    if (isSpecial) base *= 2;
    if (w && w.isVolatile) base *= 1.3;
    if (this._buffActive(ps, "damage")) base *= 1.2;
    const P = ps.power || 0, F = ps.ferocity || 0;
    const critChance = Math.max(0, Math.min(
      1,
      40 * P / (P + 200) / 100 + 30 * F / (F + 250) / 100
    ));
    const isCrit = Math.random() < critChance;
    if (isCrit) base *= 1.5 + P * 1e-3 + F * 8e-4;
    return { dmg: Math.max(1, Math.round(base)), isCrit };
  }
  _handleMonsterDamage(session, payload) {
    const { monsterId, zone, element, slot } = payload;
    if (!monsterId || !zone) return;
    const monsters = this.monsters[zone];
    if (!monsters) return;
    const m = monsters.find((x) => x.id === monsterId);
    if (!m || !m.alive) return;
    const attackerPs = this.playerState[session.id];
    const isSpecial = !!payload.special;
    const rolled = this._computeAttackDamage(attackerPs, slot, isSpecial);
    const dmgCap = this._maxDmgForAttacker(attackerPs, isSpecial);
    const rawDmg = Math.max(1, Math.min(dmgCap, rolled.dmg));
    const actualDmg = Math.min(rawDmg, Math.max(0, m.hp));
    m.hp -= actualDmg;
    if (!m.dmgByPlayer) m.dmgByPlayer = {};
    m.dmgByPlayer[session.id] = (m.dmgByPlayer[session.id] || 0) + actualDmg;
    m._aggroOverrideTarget = session.id;
    m._aggroOverrideUntil = Date.now() + 1e4;
    if (attackerPs) {
      const kbForce = payload.special ? 60 : rolled.isCrit ? 45 : 30;
      const kbAng = Math.atan2(m.y - attackerPs.y, m.x - attackerPs.x);
      m.x += Math.cos(kbAng) * kbForce;
      m.y += Math.sin(kbAng) * kbForce;
      const zoneCfg = this._getZoneConfig(zone);
      if (zoneCfg) {
        const W = zoneCfg.w * this.TILE;
        const H = zoneCfg.h * this.TILE;
        const edgePad = this.TILE;
        m.x = Math.max(edgePad, Math.min(W - edgePad, m.x));
        m.y = Math.max(edgePad, Math.min(H - edgePad, m.y));
      }
    }
    this.dirtyMonsters.add(zone);
    this.eventBuffer.push({
      type: "monster_hit",
      payload: {
        monsterId: m.id,
        zone,
        dmg: actualDmg,
        isCrit: rolled.isCrit,
        attackerId: session.id,
        hpPct: Math.max(0, m.hp / m.maxHp)
      }
    });
    if (m.hp <= 0) {
      m.alive = false;
      m.respawnAt = Date.now() + this.RESPAWN_TIME;
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
        if (share >= 0.05) goldRecipients.push(pid);
      }
      if (xpRecipients.length === 0) {
        xpRecipients.push(session.id);
        goldRecipients.push(session.id);
        shares[session.id] = 1;
      }
      this.eventBuffer.push({
        type: "monster_kill",
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
          shares
        }
      });
      const pile = this._spawnLootForKill(zone, m, session.id, goldRecipients, shares);
      if (pile) {
        this.eventBuffer.push({
          type: "loot_drop",
          payload: { pile: this._serializePile(pile) }
        });
      }
      for (const rid of xpRecipients) {
        const recipPs = this.playerState[rid];
        if (!recipPs) continue;
        const share = shares[rid] || 0;
        const xpForRecipient = Math.round((m.xp || 0) * share);
        if (xpForRecipient <= 0) continue;
        const { leveled, levelsGained, newLevel } = this._addCombatXp(recipPs, xpForRecipient);
        if (leveled) {
          this._recomputeMaxes(recipPs);
          if (typeof recipPs.maxHp === "number") recipPs.hp = recipPs.maxHp;
          if (typeof recipPs.maxStamina === "number") recipPs.stamina = recipPs.maxStamina;
          if (typeof recipPs.maxMana === "number") recipPs.mana = recipPs.maxMana;
        }
        this._saveRpg(rid, recipPs);
        const recipWs = this._wsBySessionId(rid);
        if (recipWs) {
          try {
            recipWs.send(JSON.stringify({
              type: "combat_credit",
              payload: {
                monsterId: m.id,
                zone,
                xpAmt: xpForRecipient,
                leveled,
                levelsGained,
                newLevel
              }
            }));
          } catch (e) {
          }
        }
        this._queuePlayerStateFlush(rid);
      }
      const { refund, reason } = this._applyMeleeLifesteal(attackerPs, m.id, slot);
      const killerWs = this._wsBySessionId(session.id);
      if (killerWs) {
        try {
          killerWs.send(JSON.stringify({
            type: "lifesteal_credit",
            payload: {
              playerId: session.id,
              monsterId: m.id,
              refund,
              reason,
              // Echo the resolved slot + activeSlot so a stale-state
              // debug session has the full picture.
              slot: slot || null,
              activeSlot: attackerPs && attackerPs.activeSlot || null
            }
          }));
        } catch (e) {
        }
        if (refund > 0) {
          this._saveRpg(session.id, attackerPs);
          this._sendPlayerState(killerWs, session.id);
        }
      }
      m.dmgByPlayer = {};
    }
  }
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/_room_count") {
      return new Response(JSON.stringify({ count: this.getPlayerCount() }), {
        headers: { "Content-Type": "application/json" }
      });
    }
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }
    if (this.sessions.size >= this.MAX_PLAYERS) {
      return new Response("Room full", { status: 503 });
    }
    const [client, server] = Object.values(new WebSocketPair());
    this.state.acceptWebSocket(server);
    this.sessions.set(server, { id: null, name: "Anon", data: {}, rtt: 80, lastPing: 0, lastRecv: Date.now() });
    if (!this.tickInterval && this.sessions.size === 1) this.startTickLoop();
    return new Response(null, { status: 101, webSocket: client });
  }
  async webSocketMessage(ws, message) {
    const session = this.sessions.get(ws);
    if (!session) return;
    let msg;
    try {
      msg = JSON.parse(message);
    } catch {
      return;
    }
    if (msg.type !== "pong") session.lastRecv = Date.now();
    switch (msg.type) {
      case "join":
        session.id = msg.id;
        session.name = msg.name || "Anon";
        session.data = msg.data || {};
        this.playerState[msg.id] = {
          x: 0,
          y: 0,
          d: "down",
          z: "town",
          vx: 0,
          vy: 0,
          dodging: false,
          blocking: false,
          dead: false,
          disconnected: false,
          ...msg.data
        };
        this.stateHistory[msg.id] = [];
        {
          const stored = await this._loadRpg(msg.id);
          if (stored) {
            this.playerState[msg.id].coins = stored.coins || 0;
            this.playerState[msg.id].inventory = stored.inventory || {};
            this.playerState[msg.id].lifeSkills = stored.lifeSkills || {};
            this.playerState[msg.id].level = stored.level || 1;
            this.playerState[msg.id].xp = stored.xp || 0;
            this.playerState[msg.id].unspentT2 = stored.unspentT2 || 0;
            this.playerState[msg.id].buildPointsThisLvl = stored.buildPointsThisLvl || 0;
            this.playerState[msg.id].hp = typeof stored.hp === "number" ? stored.hp : 100;
            this.playerState[msg.id].maxHp = typeof stored.maxHp === "number" ? stored.maxHp : 100;
            this.playerState[msg.id].stamina = typeof stored.stamina === "number" ? stored.stamina : 100;
            this.playerState[msg.id].maxStamina = typeof stored.maxStamina === "number" ? stored.maxStamina : 100;
            this.playerState[msg.id].mana = typeof stored.mana === "number" ? stored.mana : 100;
            this.playerState[msg.id].maxMana = typeof stored.maxMana === "number" ? stored.maxMana : 100;
            this.playerState[msg.id]._buffs = stored._buffs && typeof stored._buffs === "object" ? { ...stored._buffs } : {};
            this.playerState[msg.id].weapon = stored.weapon || null;
            this.playerState[msg.id].rangedWeapon = stored.rangedWeapon || null;
            this.playerState[msg.id].staffWeapon = stored.staffWeapon || null;
            this.playerState[msg.id].activeSlot = stored.activeSlot || "melee";
            this.playerState[msg.id].armor = stored.armor && stored.armor.name === "Leather Armor" ? null : stored.armor || null;
            this.playerState[msg.id].shield = stored.shield || null;
            this.playerState[msg.id].amulet = stored.amulet || null;
            this.playerState[msg.id].weaponStash = Array.isArray(stored.weaponStash) ? stored.weaponStash.slice(0, this.WEAPON_STASH_CAP) : [];
            this.playerState[msg.id]._quests = stored._quests && typeof stored._quests === "object" ? { ...stored._quests } : {};
            this.playerState[msg.id]._questFlags = stored._questFlags && typeof stored._questFlags === "object" ? { ...stored._questFlags } : {};
            this.playerState[msg.id]._questKills = stored._questKills && typeof stored._questKills === "object" ? { ...stored._questKills } : {};
            this.playerState[msg.id].achievementPoints = stored.achievementPoints || 0;
            this.playerState[msg.id]._perfectHistory = Array.isArray(stored._perfectHistory) ? stored._perfectHistory : [];
          } else {
            const BOOTSTRAP_LEVEL_CAP = 15;
            const BOOTSTRAP_XP_CAP = 5e4;
            const BOOTSTRAP_UT2_CAP = 75;
            const BOOTSTRAP_COINS_CAP = 2e3;
            const BOOTSTRAP_INV_PER_ITEM_CAP = 50;
            const BOOTSTRAP_INV_KEY_COUNT_CAP = 100;
            const _rawInv = msg.data && msg.data.rpgInventory && typeof msg.data.rpgInventory === "object" ? msg.data.rpgInventory : {};
            const _cappedInv = {};
            let _kc = 0;
            for (const [k, v] of Object.entries(_rawInv)) {
              if (_kc >= BOOTSTRAP_INV_KEY_COUNT_CAP) break;
              const n = Number(v);
              if (!Number.isFinite(n) || n <= 0) continue;
              _cappedInv[k] = Math.min(BOOTSTRAP_INV_PER_ITEM_CAP, Math.floor(n));
              _kc++;
            }
            this.playerState[msg.id].coins = Math.max(0, Math.min(
              BOOTSTRAP_COINS_CAP,
              msg.data && typeof msg.data.rpgCoins === "number" ? Math.floor(msg.data.rpgCoins) : 0
            ));
            this.playerState[msg.id].inventory = _cappedInv;
            this.playerState[msg.id].lifeSkills = msg.data && msg.data.rpgLifeSkills && typeof msg.data.rpgLifeSkills === "object" ? { ...msg.data.rpgLifeSkills } : {};
            this.playerState[msg.id].level = Math.max(1, Math.min(
              BOOTSTRAP_LEVEL_CAP,
              msg.data && typeof msg.data.rpgLevel === "number" ? Math.floor(msg.data.rpgLevel) : 1
            ));
            this.playerState[msg.id].xp = Math.max(0, Math.min(
              BOOTSTRAP_XP_CAP,
              msg.data && typeof msg.data.rpgXp === "number" ? Math.floor(msg.data.rpgXp) : 0
            ));
            this.playerState[msg.id].unspentT2 = Math.max(0, Math.min(
              BOOTSTRAP_UT2_CAP,
              msg.data && typeof msg.data.rpgUnspentT2 === "number" ? Math.floor(msg.data.rpgUnspentT2) : 0
            ));
            this.playerState[msg.id].buildPointsThisLvl = Math.max(0, Math.min(
              4,
              msg.data && typeof msg.data.rpgBuildPointsThisLvl === "number" ? Math.floor(msg.data.rpgBuildPointsThisLvl) : 0
            ));
            this.playerState[msg.id].hp = msg.data && typeof msg.data.rpgHp === "number" ? msg.data.rpgHp : 100;
            this.playerState[msg.id].maxHp = msg.data && typeof msg.data.rpgMaxHp === "number" ? msg.data.rpgMaxHp : 100;
            this.playerState[msg.id].stamina = msg.data && typeof msg.data.rpgStamina === "number" ? msg.data.rpgStamina : 100;
            this.playerState[msg.id].maxStamina = msg.data && typeof msg.data.rpgMaxStamina === "number" ? msg.data.rpgMaxStamina : 100;
            this.playerState[msg.id].mana = msg.data && typeof msg.data.rpgMana === "number" ? msg.data.rpgMana : 100;
            this.playerState[msg.id].maxMana = msg.data && typeof msg.data.rpgMaxMana === "number" ? msg.data.rpgMaxMana : 100;
            this.playerState[msg.id]._buffs = {};
            this.playerState[msg.id].weapon = msg.data && msg.data.rpgWeapon && typeof msg.data.rpgWeapon === "object" ? { ...msg.data.rpgWeapon } : null;
            this.playerState[msg.id].rangedWeapon = msg.data && msg.data.rpgRangedWeapon && typeof msg.data.rpgRangedWeapon === "object" ? { ...msg.data.rpgRangedWeapon } : null;
            this.playerState[msg.id].staffWeapon = msg.data && msg.data.rpgStaffWeapon && typeof msg.data.rpgStaffWeapon === "object" ? { ...msg.data.rpgStaffWeapon } : null;
            this.playerState[msg.id].activeSlot = msg.data && typeof msg.data.rpgActiveSlot === "string" ? msg.data.rpgActiveSlot : "melee";
            {
              const _bootArmor = msg.data && msg.data.rpgArmor && typeof msg.data.rpgArmor === "object" ? msg.data.rpgArmor : null;
              this.playerState[msg.id].armor = _bootArmor && _bootArmor.name === "Leather Armor" ? null : _bootArmor ? { ..._bootArmor } : null;
            }
            this.playerState[msg.id].shield = msg.data && msg.data.rpgShield && typeof msg.data.rpgShield === "object" ? { ...msg.data.rpgShield } : null;
            this.playerState[msg.id].amulet = msg.data && msg.data.rpgAmulet && typeof msg.data.rpgAmulet === "object" ? { ...msg.data.rpgAmulet } : null;
            this.playerState[msg.id].weaponStash = msg.data && Array.isArray(msg.data.rpgWeaponStash) ? msg.data.rpgWeaponStash.slice(0, this.WEAPON_STASH_CAP) : [];
            const _qK = msg.data && msg.data.rpgQuestKills && typeof msg.data.rpgQuestKills === "object" ? msg.data.rpgQuestKills : {};
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
            const _capObjKeys = /* @__PURE__ */ __name((src) => {
              const out = {};
              if (!src || typeof src !== "object") return out;
              let n = 0;
              for (const [k, v] of Object.entries(src)) {
                if (n >= 100) break;
                out[k] = v;
                n++;
              }
              return out;
            }, "_capObjKeys");
            this.playerState[msg.id]._quests = _capObjKeys(msg.data && msg.data.rpgQuests || null);
            this.playerState[msg.id]._questFlags = _capObjKeys(msg.data && msg.data.rpgQuestFlags || null);
            this.playerState[msg.id]._questKills = _qKclean;
            this.playerState[msg.id].achievementPoints = Math.max(0, Math.min(
              99999,
              msg.data && typeof msg.data.rpgAchievementPoints === "number" ? Math.floor(msg.data.rpgAchievementPoints) : 0
            ));
            this.playerState[msg.id]._perfectHistory = [];
            await this._saveRpg(msg.id, this.playerState[msg.id]);
          }
          this.playerState[msg.id].def = msg.data && typeof msg.data.rpgDef === "number" ? Math.max(0, msg.data.rpgDef) : 0;
          this.playerState[msg.id].amuletHpRegen = msg.data && typeof msg.data.rpgAmuletHpRegen === "number" ? Math.max(0, msg.data.rpgAmuletHpRegen) : 0;
          this.playerState[msg.id].amuletStaminaRegen = msg.data && typeof msg.data.rpgAmuletStaminaRegen === "number" ? Math.max(0, msg.data.rpgAmuletStaminaRegen) : 0;
          this.playerState[msg.id].lastDamageAt = 0;
          this.playerState[msg.id].dying = false;
          this.playerState[msg.id].respawnAt = 0;
          {
            const _ps = this.playerState[msg.id];
            const _lvl = _ps.level || 1;
            const RAW_STATS = [
              "power",
              "vitality",
              "endurance",
              "agility",
              "mind",
              "ferocity",
              "elementalMastery",
              "fortification",
              "restoration",
              "influence"
            ];
            const _storedHasStats = stored && typeof stored.vitality === "number";
            for (const s of RAW_STATS) {
              if (_storedHasStats && typeof stored[s] === "number") {
                _ps[s] = stored[s];
              } else {
                const joinKey = "rpg" + s.charAt(0).toUpperCase() + s.slice(1);
                const joinVal = msg.data && typeof msg.data[joinKey] === "number" ? msg.data[joinKey] : 0;
                _ps[s] = this._clampStat(joinVal, _lvl);
              }
            }
            this._recomputeMaxes(_ps);
            this._saveRpg(msg.id, _ps);
          }
        }
        this.broadcastExcept(ws, { type: "player_join", id: msg.id, name: msg.name, data: msg.data });
        const joinZone = msg.data?.z || "town";
        const zoneMonsters = joinZone !== "town" && joinZone !== "farm_home" ? this._ensureZoneMonsters(joinZone) : [];
        const zoneNodes = joinZone !== "town" && joinZone !== "farm_home" ? this._ensureZoneNodes(joinZone) : [];
        const zoneLootForJoin = joinZone !== "town" && joinZone !== "farm_home" ? this._zoneLootForWire(joinZone) : [];
        ws.send(JSON.stringify({
          type: "state_sync",
          players: this.getAllPlayerData(),
          playerCount: this.getPlayerCount(),
          monsters: zoneMonsters.map((m) => ({
            id: m.id,
            arch: m.arch,
            level: m.level,
            element: m.element,
            x: m.x,
            y: m.y,
            hp: m.hp,
            maxHp: m.maxHp,
            dmg: m.dmg,
            xp: m.xp,
            gold: m.gold,
            spd: m.spd,
            emoji: m.emoji,
            color: m.color,
            alive: m.alive
          })),
          nodes: zoneNodes.map((n) => ({
            id: n.id,
            nodeType: n.nodeType,
            x: n.x,
            y: n.y,
            tierLvl: n.tierLvl,
            alive: n.alive,
            respawnAt: n.respawnAt
          })),
          loot: zoneLootForJoin,
          monsterZone: joinZone
        }));
        this._sendPlayerState(ws, msg.id);
        this.broadcastAll({ type: "player_count", count: this.getPlayerCount() });
        this.reportToLeaderboard(session);
        break;
      case "move":
        if (session.id && this.playerState[session.id]) {
          const ps = this.playerState[session.id];
          const oldZone = ps.z;
          const newZone = msg.z || ps.z;
          if (typeof msg.x !== "number" || typeof msg.y !== "number") break;
          const _now = Date.now();
          const zoneChanged = newZone !== oldZone;
          const firstMove = typeof ps.lastMoveAt !== "number";
          let accept = true;
          if (!zoneChanged && !firstMove && typeof ps.x === "number" && typeof ps.y === "number") {
            const dt = Math.max(1e-3, (_now - ps.lastMoveAt) / 1e3);
            const maxDist = 500 * dt + 80;
            const dx = msg.x - ps.x;
            const dy = msg.y - ps.y;
            if (dx * dx + dy * dy > maxDist * maxDist) {
              accept = false;
            }
          }
          ps.lastMoveAt = _now;
          if (accept) {
            ps.x = msg.x;
            ps.y = msg.y;
            ps.d = msg.d || ps.d;
            ps.z = newZone;
            ps.vx = msg.vx || 0;
            ps.vy = msg.vy || 0;
            if (msg.dodging !== void 0) ps.dodging = !!msg.dodging;
            if (msg.blocking !== void 0) ps.blocking = !!msg.blocking;
            if (msg.dead !== void 0) ps.dead = !!msg.dead;
            this.dirtyPlayers.add(session.id);
          }
          if (ps.z !== oldZone) {
            ps.dmgFromMonster = {};
            if (ps.z !== "town" && ps.z !== "farm_home") {
              const newMonsters = this._ensureZoneMonsters(ps.z);
              ps._zoneEntryGraceUntil = Date.now() + this.ZONE_ENTRY_GRACE_MS;
              ws.send(JSON.stringify({
                type: "zone_monsters",
                zone: ps.z,
                monsters: newMonsters.map((m) => ({
                  id: m.id,
                  arch: m.arch,
                  level: m.level,
                  element: m.element,
                  x: m.x,
                  y: m.y,
                  hp: m.hp,
                  maxHp: m.maxHp,
                  dmg: m.dmg,
                  xp: m.xp,
                  gold: m.gold,
                  spd: m.spd,
                  emoji: m.emoji,
                  color: m.color,
                  alive: m.alive
                }))
              }));
              const newNodes = this._ensureZoneNodes(ps.z);
              ws.send(JSON.stringify({
                type: "zone_nodes",
                zone: ps.z,
                nodes: newNodes.map((n) => ({
                  id: n.id,
                  nodeType: n.nodeType,
                  x: n.x,
                  y: n.y,
                  tierLvl: n.tierLvl,
                  alive: n.alive,
                  respawnAt: n.respawnAt
                }))
              }));
              ws.send(JSON.stringify({
                type: "zone_loot",
                zone: ps.z,
                loot: this._zoneLootForWire(ps.z)
              }));
            } else {
              ws.send(JSON.stringify({
                type: "zone_monsters",
                zone: ps.z,
                monsters: []
              }));
              ws.send(JSON.stringify({
                type: "zone_nodes",
                zone: ps.z,
                nodes: []
              }));
              ws.send(JSON.stringify({
                type: "zone_loot",
                zone: ps.z,
                loot: []
              }));
            }
          }
        }
        break;
      case "pong":
        if (session.lastPing > 0) {
          const sample = Date.now() - session.lastPing;
          session.rtt = session.rtt * (1 - this.LAGCOMP_RTT_ALPHA) + sample * this.LAGCOMP_RTT_ALPHA;
          session.rtt = Math.min(session.rtt, this.LAGCOMP_RTT_CAP);
        }
        break;
      case "track":
        if (session.id) {
          session.data = { ...session.data, ...msg.data };
          if (this.playerState[session.id]) Object.assign(this.playerState[session.id], msg.data);
          this.broadcastExcept(ws, { type: "player_update", id: session.id, data: msg.data });
          this.reportToLeaderboard(session);
        }
        break;
      case "player_attack":
        if (session.id) {
          this._resolvePvPAttack(session, msg.payload || msg);
        }
        break;
      case "monster_damage":
        if (session.id) {
          this._handleMonsterDamage(session, msg.payload || msg);
        }
        break;
      case "extraction_start":
        if (session.id) {
          this._handleExtractionStart(session, msg.payload || msg);
        }
        break;
      case "node_strike":
        if (session.id) {
          this._handleNodeStrike(session, msg.payload || msg);
        }
        break;
      case "loot_pickup":
        if (session.id) {
          this._handleLootPickup(session, msg.payload || msg);
        }
        break;
      case "stat_allocate":
        if (session.id) {
          this._handleStatAllocate(session, msg.payload || msg);
        }
        break;
      case "cook_request":
        if (session.id) {
          this._handleCookRequest(session, msg.payload || msg);
        }
        break;
      case "stats_update":
        if (session.id) {
          this._handleStatsUpdate(session, msg.payload || msg);
        }
        break;
      case "ability_use":
        if (session.id) {
          this._handleAbilityUse(session, msg.payload || msg);
        }
        break;
      case "eat_request":
        if (session.id) {
          this._handleEatRequest(session, msg.payload || msg);
        }
        break;
      case "shop_purchase":
        if (session.id) {
          this._handleShopPurchase(session, msg.payload || msg);
        }
        break;
      case "cook_recipe":
        if (session.id) {
          this._handleCookRecipe(session, msg.payload || msg);
        }
        break;
      case "equip_request":
        if (session.id) {
          this._handleEquipRequest(session, msg.payload || msg);
        }
        break;
      case "sell_weapon":
        if (session.id) {
          this._handleSellWeapon(session, msg.payload || msg);
        }
        break;
      case "unequip_request":
        if (session.id) {
          this._handleUnequipRequest(session, msg.payload || msg);
        }
        break;
      case "build_point_earned":
        if (session.id) {
          this._handleBuildPointEarned(session);
        }
        break;
      case "set_active_slot": {
        if (session.id) {
          const ps = this.playerState[session.id];
          if (ps) {
            const slot = msg.payload && msg.payload.slot;
            if (slot === "melee" || slot === "ranged" || slot === "staff") {
              ps.activeSlot = slot;
              this._saveRpg(session.id, ps);
            }
          }
        }
        break;
      }
      case "forge_weapon":
        if (session.id) {
          this._handleForgeWeapon(session, msg.payload || msg);
        }
        break;
      case "quest_accept":
        if (session.id) {
          this._handleQuestAccept(session, msg.payload || msg);
        }
        break;
      case "quest_turn_in":
        if (session.id) {
          this._handleQuestTurnIn(session, msg.payload || msg);
        }
        break;
      default:
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
    const halfRtt = attackerSession.rtt / 2;
    const rewindTicks = Math.min(Math.ceil(halfRtt / this.TICK_RATE), this.LAGCOMP_BUFFER_TICKS);
    if (attackerPs.dying || attackerPs.dead || attackerPs.disconnected) return;
    const range = Math.max(10, Math.min(250, payload.range || 40));
    const arc = Math.max(0.1, Math.min(Math.PI * 1.1, payload.arc || 1.2));
    const angle = payload.angle || 0;
    const dmgCap = this._maxDmgForAttacker(attackerPs, !!payload.special);
    const dmgBase = Math.max(1, Math.min(dmgCap, payload.dmgBase || 10));
    const critChance = Math.max(0, Math.min(100, payload.critChance || 0));
    for (const [targetId, targetPs] of Object.entries(this.playerState)) {
      if (targetId === attackerId) continue;
      if (targetPs.z !== attackerPs.z) continue;
      if (targetPs.dead || targetPs.disconnected) continue;
      const history = this.stateHistory[targetId];
      let checkState = targetPs;
      if (history && history.length > 0) {
        const idx = Math.max(0, history.length - 1 - rewindTicks);
        checkState = history[idx] || targetPs;
      }
      const dx = checkState.x - attackerPs.x;
      const dy = checkState.y - attackerPs.y;
      const dist = Math.sqrt(dx * dx + dy * dy);
      if (dist > range) continue;
      const targetAngle = Math.atan2(dy, dx);
      let angleDiff = targetAngle - angle;
      while (angleDiff > Math.PI) angleDiff -= Math.PI * 2;
      while (angleDiff < -Math.PI) angleDiff += Math.PI * 2;
      if (Math.abs(angleDiff) > arc / 2) continue;
      if (checkState.dodging) continue;
      let blocked = false;
      if (checkState.blocking) blocked = true;
      const isCrit = Math.random() * 100 < critChance;
      const rawDmg = dmgBase * (isCrit ? 1.5 : 1);
      const dmgResult = this._applyDamage(targetPs, rawDmg, blocked);
      const dmgTaken = dmgResult.dmgTaken;
      const hitEvent = {
        type: "pvp_hit",
        payload: {
          attacker: attackerId,
          attackerName: attackerSession.name,
          target: targetId,
          dmgBase,
          dmgTaken,
          isCrit,
          blocked,
          dodged: dmgResult.dodged,
          ts: Date.now(),
          rewindTicks
        }
      };
      this.eventBuffer.push(hitEvent);
      this._saveRpg(targetId, targetPs);
      this._queuePlayerStateFlush(targetId);
      if (targetPs.hp <= 0 && !targetPs.dying) {
        this._handlePlayerDeath(targetPs, targetId, "pvp:" + attackerId);
      }
    }
  }
  async webSocketClose(ws) {
    const session = this.sessions.get(ws);
    if (session?.id) {
      if (this.playerState[session.id]) this.playerState[session.id].disconnected = true;
      delete this.playerState[session.id];
      delete this.stateHistory[session.id];
      delete this.extractions[session.id];
      this.dirtyPlayers.delete(session.id);
      this.broadcastAll({ type: "player_leave", id: session.id });
      this.broadcastAll({ type: "player_count", count: this.getPlayerCount() - 1 });
    }
    this.sessions.delete(ws);
    if (this.sessions.size === 0 && this.tickInterval) {
      clearInterval(this.tickInterval);
      this.tickInterval = null;
    }
  }
  async webSocketError(ws) {
    this.webSocketClose(ws);
  }
  startTickLoop() {
    let pingCounter = 0;
    let regenCounter = 0;
    this.tickInterval = setInterval(() => {
      for (const [id, ps] of Object.entries(this.playerState)) {
        if (!this.stateHistory[id]) this.stateHistory[id] = [];
        this.stateHistory[id].push({
          x: ps.x,
          y: ps.y,
          d: ps.d,
          z: ps.z,
          dodging: ps.dodging || false,
          blocking: ps.blocking || false,
          dead: ps.dead || false,
          tick: this.tickSeq
        });
        if (this.stateHistory[id].length > this.LAGCOMP_BUFFER_TICKS) {
          this.stateHistory[id].shift();
        }
      }
      this._tickMonsters();
      this._tickNodes();
      this._tickLoot();
      this._sweepStaleExtractions(Date.now());
      this._tickPlayerRespawn();
      regenCounter++;
      if (regenCounter >= 30) {
        regenCounter = 0;
        this._tickPlayerRegen();
      }
      this._flushPendingPlayerStates();
      pingCounter++;
      if (pingCounter >= 90) {
        pingCounter = 0;
        const nowMs = Date.now();
        const pingMsg = JSON.stringify({ type: "ping", ts: nowMs });
        for (const [ws, session] of this.sessions) {
          if (nowMs - session.lastRecv > this.IDLE_TIMEOUT_MS) {
            try {
              ws.close(1e3, "idle timeout");
            } catch {
            }
            continue;
          }
          session.lastPing = nowMs;
          try {
            ws.send(pingMsg);
          } catch {
          }
        }
      }
      const hasDirty = this.dirtyPlayers.size > 0;
      const hasEvents = this.eventBuffer.length > 0;
      const hasMonsters = this.dirtyMonsters.size > 0;
      const hasNodes = this.dirtyNodes.size > 0;
      if (!hasDirty && !hasEvents && !hasMonsters && !hasNodes) {
        this.tickSeq++;
        return;
      }
      const delta = { type: "tick", seq: this.tickSeq++, ts: Date.now() };
      if (hasDirty) {
        const players = {};
        for (const id of this.dirtyPlayers) {
          const ps = this.playerState[id];
          if (ps) players[id] = { x: ps.x, y: ps.y, d: ps.d, z: ps.z, vx: ps.vx, vy: ps.vy };
        }
        delta.players = players;
        this.dirtyPlayers.clear();
      }
      if (hasEvents) {
        delta.events = this.eventBuffer.length <= this.EVENTS_PER_TICK_CAP ? this.eventBuffer : this.eventBuffer.slice(0, this.EVENTS_PER_TICK_CAP);
        this.eventBuffer = [];
      }
      if (hasMonsters) {
        const mData = {};
        for (const zoneId of this.dirtyMonsters) {
          const monsters = this.monsters[zoneId];
          if (!monsters) continue;
          mData[zoneId] = monsters.map((m) => ({
            id: m.id,
            x: Math.round(m.x),
            y: Math.round(m.y),
            hp: m.hp,
            alive: m.alive
          }));
        }
        delta.monsters = mData;
        this.dirtyMonsters.clear();
      }
      if (hasNodes) {
        const nData = {};
        for (const zoneId of this.dirtyNodes) {
          const list = this.nodes[zoneId];
          if (!list) continue;
          nData[zoneId] = list.map((n) => ({
            id: n.id,
            alive: n.alive,
            respawnAt: n.respawnAt
          }));
        }
        delta.nodes = nData;
        this.dirtyNodes.clear();
      }
      const msg = JSON.stringify(delta);
      for (const [ws] of this.sessions) {
        try {
          ws.send(msg);
        } catch {
        }
      }
    }, this.TICK_RATE);
  }
  async reportToLeaderboard(session) {
    try {
      const stub = this.env.LEADERBOARD.get(this.env.LEADERBOARD.idFromName("global"));
      await stub.fetch(new Request("https://internal/api/leaderboard/update", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          playerId: session.id,
          name: session.name || session.data?.name || "Anon",
          color: session.data?.color || "#5b52ff",
          level: session.data?.rpgLv || 1,
          rpgData: session.data?.rpgData || {},
          ts: Date.now()
        })
      }));
    } catch {
    }
  }
  broadcastAll(msg) {
    const s = JSON.stringify(msg);
    for (const [ws] of this.sessions) {
      try {
        ws.send(s);
      } catch {
      }
    }
  }
  broadcastExcept(ex, msg) {
    const s = JSON.stringify(msg);
    for (const [ws] of this.sessions) {
      if (ws !== ex) {
        try {
          ws.send(s);
        } catch {
        }
      }
    }
  }
  getAllPlayerData() {
    const r = {};
    for (const [, s] of this.sessions) {
      if (s.id) r[s.id] = { ...this.playerState[s.id], name: s.name, ...s.data };
    }
    return r;
  }
  getPlayerCount() {
    let c = 0;
    for (const [, s] of this.sessions) {
      if (s.id) c++;
    }
    return c;
  }
};
var Marketplace = class {
  static {
    __name(this, "Marketplace");
  }
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.index = null;
    this.playerOrderCounts = null;
    this.SWEEP_INTERVAL = 6e4;
    this.ORDER_EXPIRY = 36e5;
    this.MAX_ORDERS_PER_PLAYER = 10;
  }
  // §39.4 — Composite index key
  _indexKey(o) {
    return `${o.category}:${o.subtype}:${o.tierKey}:${o.element1 || "none"}:${o.element2 || "none"}`;
  }
  // Load full index from storage into memory (once per DO wake)
  async _ensureIndex() {
    if (this.index) return;
    this.index = /* @__PURE__ */ new Map();
    this.playerOrderCounts = /* @__PURE__ */ new Map();
    const now = Date.now();
    const entries = await this.state.storage.list({ prefix: "order:" });
    const expired = [];
    for (const [key, raw] of entries) {
      let o;
      try {
        o = JSON.parse(raw);
      } catch {
        expired.push(key);
        continue;
      }
      if (o.expires <= now) {
        expired.push(key);
        continue;
      }
      this._addToIndex(o);
    }
    if (expired.length) await this.state.storage.delete(expired);
  }
  _addToIndex(o) {
    const key = this._indexKey(o);
    if (!this.index.has(key)) this.index.set(key, { buys: [], sells: [] });
    const bucket = this.index.get(key);
    if (o.type === "buy") {
      bucket.buys.push(o);
      bucket.buys.sort((a, b) => b.price - a.price);
    } else {
      bucket.sells.push(o);
      bucket.sells.sort((a, b) => a.price - b.price);
    }
    this.playerOrderCounts.set(o.playerId, (this.playerOrderCounts.get(o.playerId) || 0) + 1);
  }
  _removeFromIndex(o) {
    const key = this._indexKey(o);
    const bucket = this.index.get(key);
    if (!bucket) return;
    if (o.type === "buy") {
      bucket.buys = bucket.buys.filter((x) => x.id !== o.id);
    } else {
      bucket.sells = bucket.sells.filter((x) => x.id !== o.id);
    }
    if (bucket.buys.length === 0 && bucket.sells.length === 0) this.index.delete(key);
    const count = (this.playerOrderCounts.get(o.playerId) || 1) - 1;
    if (count <= 0) this.playerOrderCounts.delete(o.playerId);
    else this.playerOrderCounts.set(o.playerId, count);
  }
  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace("/api/market", "");
    const H = { "Access-Control-Allow-Origin": "*", "Content-Type": "application/json" };
    try {
      await this._ensureIndex();
      await this._lazySweep();
      if (request.method === "GET" && path.startsWith("/orders")) {
        const category = url.searchParams.get("category");
        const subtype = url.searchParams.get("subtype");
        const tier = url.searchParams.get("tier");
        const orders = this._queryOrders(category, subtype, tier, null, 100);
        return new Response(JSON.stringify({ ok: true, orders }), { headers: H });
      }
      if (request.method === "POST" && path.startsWith("/place")) {
        const body = await request.json();
        const result = await this.placeOrder(body);
        return new Response(JSON.stringify(result), { headers: H });
      }
      if (request.method === "DELETE" && path.startsWith("/cancel")) {
        const orderId = url.searchParams.get("id");
        const playerId = url.searchParams.get("playerId");
        const result = await this.cancelOrder(orderId, playerId);
        return new Response(JSON.stringify(result), { headers: H });
      }
      if (request.method === "GET" && path.startsWith("/my")) {
        const playerId = url.searchParams.get("playerId");
        const orders = this._queryOrders(null, null, null, playerId, 100);
        return new Response(JSON.stringify({ ok: true, orders }), { headers: H });
      }
      return new Response(JSON.stringify({ ok: false, error: "Not found" }), { status: 404, headers: H });
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
    if (!type || !category || !subtype || !tierKey || !price || !playerId) return { ok: false, error: "Missing fields" };
    if (price < 1 || price > 999999) return { ok: false, error: "Invalid price" };
    if (type !== "buy" && type !== "sell") return { ok: false, error: "Invalid type" };
    if (type === "sell" && !item) return { ok: false, error: "Sell needs item" };
    const currentCount = this.playerOrderCounts.get(playerId) || 0;
    if (currentCount >= this.MAX_ORDERS_PER_PLAYER) return { ok: false, error: "Max 10 orders" };
    const order = {
      id: crypto.randomUUID(),
      type,
      category,
      subtype,
      tierKey,
      element1: element1 || null,
      element2: element2 || null,
      price: Math.floor(price),
      item: type === "sell" ? item : null,
      tierLabel: tierLabel || tierKey,
      playerName: playerName || "Unknown",
      playerId,
      ts: Date.now(),
      expires: Date.now() + this.ORDER_EXPIRY
    };
    const key = this._indexKey(order);
    const bucket = this.index.get(key);
    let best = null;
    if (bucket) {
      const oppList = type === "buy" ? bucket.sells : bucket.buys;
      for (let i = 0; i < oppList.length; i++) {
        const o = oppList[i];
        if (o.playerId === playerId) continue;
        if (type === "buy" && o.price <= price) {
          best = o;
          break;
        }
        if (type === "sell" && o.price >= price) {
          best = o;
          break;
        }
      }
    }
    if (best) {
      this._removeFromIndex(best);
      await this.state.storage.delete("order:" + best.id);
      return { ok: true, matched: true, execPrice: best.price, matchedOrder: best, newOrder: order };
    }
    this._addToIndex(order);
    await this.state.storage.put("order:" + order.id, JSON.stringify(order));
    return { ok: true, matched: false, order };
  }
  async cancelOrder(orderId, playerId) {
    if (!orderId || !playerId) return { ok: false, error: "Missing params" };
    const raw = await this.state.storage.get("order:" + orderId);
    if (!raw) return { ok: false, error: "Not found" };
    const order = JSON.parse(raw);
    if (order.playerId !== playerId) return { ok: false, error: "Not yours" };
    this._removeFromIndex(order);
    await this.state.storage.delete("order:" + orderId);
    return { ok: true, cancelled: order };
  }
  // §39.4 — Lazy expiry sweep (once per minute)
  async _lazySweep() {
    const lp = await this.state.storage.get("_lastPurge") || 0;
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
      await this.state.storage.delete("order:" + o.id);
    }
    await this.state.storage.put("_lastPurge", Date.now());
  }
};
var Leaderboard = class {
  static {
    __name(this, "Leaderboard");
  }
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }
  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace("/api/leaderboard", "");
    const H = { "Access-Control-Allow-Origin": "*", "Content-Type": "application/json" };
    try {
      if (request.method === "POST" && path.startsWith("/update")) {
        const body = await request.json();
        await this.updatePlayer(body);
        return new Response(JSON.stringify({ ok: true }), { headers: H });
      }
      if (request.method === "GET" && path.startsWith("/top")) {
        const category = url.searchParams.get("category") || "level";
        const limit = Math.min(100, parseInt(url.searchParams.get("limit")) || 50);
        const results = await this.getTop(category, limit);
        return new Response(JSON.stringify({ ok: true, category, results }), { headers: H });
      }
      return new Response(JSON.stringify({ ok: false, error: "Not found" }), { status: 404, headers: H });
    } catch (err) {
      return new Response(JSON.stringify({ ok: false, error: err.message }), { status: 500, headers: H });
    }
  }
  async updatePlayer(data) {
    const { playerId, name, color, level, rpgData, ts } = data;
    if (!playerId) return;
    await this.state.storage.put("player:" + playerId, JSON.stringify({
      id: playerId,
      name: name || "Anon",
      color: color || "#5b52ff",
      level: level || 1,
      lifeTotal: rpgData?.lifeTotal || 0,
      ap: rpgData?.ap || 0,
      kills: rpgData?.kills || 0,
      dungeons: rpgData?.dungeons || 0,
      goldEarned: rpgData?.goldEarned || 0,
      playtime: rpgData?.playtime || 0,
      clanTag: rpgData?.clanTag || null,
      lastSeen: ts || Date.now()
    }));
  }
  async getTop(category, limit) {
    const entries = await this.state.storage.list({ prefix: "player:" });
    const players = [];
    const now = Date.now();
    const STALE = 7 * 864e5;
    for (const [, raw] of entries) {
      try {
        const p = JSON.parse(raw);
        if (now - (p.lastSeen || 0) < STALE) players.push(p);
      } catch {
      }
    }
    const key = { level: "level", lifeskills: "lifeTotal", ap: "ap", kills: "kills", dungeons: "dungeons", gold: "goldEarned", playtime: "playtime" }[category] || "level";
    players.sort((a, b) => (b[key] || 0) - (a[key] || 0));
    return players.slice(0, limit);
  }
};
var Arena = class {
  static {
    __name(this, "Arena");
  }
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }
  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace("/api/arena", "");
    const H = { "Access-Control-Allow-Origin": "*", "Content-Type": "application/json" };
    try {
      if (request.method === "POST" && path.startsWith("/join")) {
        const body = await request.json();
        return new Response(JSON.stringify(await this.joinQueue(body)), { headers: H });
      }
      if (request.method === "POST" && path.startsWith("/leave")) {
        const body = await request.json();
        return new Response(JSON.stringify(await this.leaveQueue(body.playerId)), { headers: H });
      }
      if (request.method === "GET" && path.startsWith("/status")) {
        const pid = url.searchParams.get("playerId");
        return new Response(JSON.stringify(await this.getStatus(pid)), { headers: H });
      }
      if (request.method === "POST" && path.startsWith("/result")) {
        const body = await request.json();
        return new Response(JSON.stringify(await this.reportResult(body)), { headers: H });
      }
      if (request.method === "GET" && path.startsWith("/tournament")) {
        return new Response(JSON.stringify(await this.getTournament()), { headers: H });
      }
      if (request.method === "GET" && path.startsWith("/history")) {
        return new Response(JSON.stringify(await this.getHistory()), { headers: H });
      }
      return new Response(JSON.stringify({ ok: false, error: "Not found" }), { status: 404, headers: H });
    } catch (err) {
      return new Response(JSON.stringify({ ok: false, error: err.message }), { status: 500, headers: H });
    }
  }
  async joinQueue(data) {
    const { playerId, name, level, color } = data;
    if (!playerId || !name) return { ok: false, error: "Missing fields" };
    const queue = await this.getQueue();
    if (queue.find((p) => p.id === playerId)) return { ok: false, error: "Already in queue" };
    const tournament = await this.getActiveTournament();
    if (tournament) {
      const inTournament = tournament.players.find((p) => p.id === playerId);
      if (inTournament && !inTournament.eliminated) return { ok: false, error: "Already in tournament" };
    }
    const entry = { id: playerId, name, level: level || 1, color: color || "#5b52ff", joinedAt: Date.now() };
    queue.push(entry);
    await this.state.storage.put("queue", JSON.stringify(queue));
    const TOURNAMENT_MIN = 4;
    const TOURNAMENT_IDEAL = 16;
    const QUEUE_TIMEOUT = 12e4;
    const oldestEntry = queue.reduce((min, p) => Math.min(min, p.joinedAt), Infinity);
    const queueAge = Date.now() - oldestEntry;
    if (queue.length >= TOURNAMENT_IDEAL || queue.length >= TOURNAMENT_MIN && queueAge >= QUEUE_TIMEOUT) {
      const players = queue.splice(0, TOURNAMENT_IDEAL).map((p) => ({ ...p, eliminated: false, wins: 0, round: 0 }));
      await this.state.storage.put("queue", JSON.stringify(queue));
      const tournament2 = {
        id: "arena-" + Date.now(),
        players,
        round: 1,
        maxRounds: 10,
        matches: [],
        // {round, p1id, p2id, winnerId, ts}
        currentMatches: [],
        // active matches this round
        startTime: Date.now(),
        status: "active",
        // 'active' | 'complete'
        champion: null,
        spectators: []
      };
      tournament2.currentMatches = this.generateMatchups(tournament2);
      await this.state.storage.put("tournament", JSON.stringify(tournament2));
      return { ok: true, started: true, tournament: this.sanitizeTournament(tournament2), position: null };
    }
    return { ok: true, started: false, queuePosition: queue.length, queueSize: queue.length };
  }
  async leaveQueue(playerId) {
    if (!playerId) return { ok: false, error: "Missing playerId" };
    let queue = await this.getQueue();
    const before = queue.length;
    queue = queue.filter((p) => p.id !== playerId);
    await this.state.storage.put("queue", JSON.stringify(queue));
    return { ok: true, removed: queue.length < before };
  }
  async getStatus(playerId) {
    if (!playerId) return { ok: false, error: "Missing playerId" };
    const queue = await this.getQueue();
    const inQueue = queue.findIndex((p) => p.id === playerId);
    if (inQueue >= 0) {
      return { ok: true, status: "queued", position: inQueue + 1, queueSize: queue.length };
    }
    const tournament = await this.getActiveTournament();
    if (tournament) {
      const player = tournament.players.find((p) => p.id === playerId);
      if (player) {
        const myMatch = tournament.currentMatches.find((m) => m.p1 === playerId || m.p2 === playerId);
        return {
          ok: true,
          status: player.eliminated ? "eliminated" : myMatch ? "fighting" : "waiting",
          tournament: this.sanitizeTournament(tournament),
          currentMatch: myMatch || null,
          round: tournament.round,
          wins: player.wins,
          eliminated: player.eliminated
        };
      }
    }
    return { ok: true, status: "none" };
  }
  async reportResult(data) {
    const { tournamentId, matchId, winnerId, loserId } = data;
    if (!tournamentId || !matchId || !winnerId || !loserId) return { ok: false, error: "Missing fields" };
    const tournament = await this.getActiveTournament();
    if (!tournament || tournament.id !== tournamentId) return { ok: false, error: "Tournament not found" };
    const matchIdx = tournament.currentMatches.findIndex((m) => m.id === matchId);
    if (matchIdx < 0) return { ok: false, error: "Match not found" };
    const match = tournament.currentMatches[matchIdx];
    if (match.resolved) return { ok: false, error: "Already resolved" };
    match.resolved = true;
    match.winnerId = winnerId;
    match.loserId = loserId;
    match.resolvedAt = Date.now();
    const winner = tournament.players.find((p) => p.id === winnerId);
    const loser = tournament.players.find((p) => p.id === loserId);
    if (winner) winner.wins++;
    if (loser) loser.eliminated = true;
    tournament.matches.push({ round: tournament.round, p1: match.p1, p2: match.p2, winnerId, loserId, ts: Date.now() });
    const allResolved = tournament.currentMatches.every((m) => m.resolved);
    if (allResolved) {
      const remaining = tournament.players.filter((p) => !p.eliminated);
      if (remaining.length <= 1 || tournament.round >= tournament.maxRounds) {
        tournament.status = "complete";
        tournament.champion = remaining[0] || null;
        tournament.endTime = Date.now();
        if (tournament.champion) {
          const history = await this.getHistoryData();
          history.push({
            championId: tournament.champion.id,
            championName: tournament.champion.name,
            championLevel: tournament.champion.level,
            wins: tournament.champion.wins,
            totalPlayers: tournament.players.length,
            rounds: tournament.round,
            ts: Date.now()
          });
          if (history.length > 50) history.splice(0, history.length - 50);
          await this.state.storage.put("history", JSON.stringify(history));
        }
      } else {
        tournament.round++;
        tournament.currentMatches = this.generateMatchups(tournament);
      }
    }
    await this.state.storage.put("tournament", JSON.stringify(tournament));
    return {
      ok: true,
      tournament: this.sanitizeTournament(tournament),
      roundComplete: allResolved,
      tournamentComplete: tournament.status === "complete",
      champion: tournament.champion
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
    const active = tournament.players.filter((p) => !p.eliminated);
    for (let i = active.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [active[i], active[j]] = [active[j], active[i]];
    }
    const matches = [];
    for (let i = 0; i < active.length - 1; i += 2) {
      matches.push({
        id: "match-" + tournament.round + "-" + i / 2 + "-" + Date.now(),
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
        loserId: null
      });
    }
    if (active.length % 2 === 1) {
      const bye = active[active.length - 1];
      bye.wins++;
      tournament.matches.push({ round: tournament.round, p1: bye.id, p2: "BYE", winnerId: bye.id, loserId: null, ts: Date.now() });
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
      remaining: t.players.filter((p) => !p.eliminated).length,
      players: t.players.map((p) => ({ id: p.id, name: p.name, level: p.level, color: p.color, eliminated: p.eliminated, wins: p.wins })),
      currentMatches: t.currentMatches,
      recentMatches: t.matches.slice(-10)
    };
  }
  async getQueue() {
    try {
      return JSON.parse(await this.state.storage.get("queue") || "[]");
    } catch {
      return [];
    }
  }
  async getActiveTournament() {
    try {
      const raw = await this.state.storage.get("tournament");
      if (!raw) return null;
      const t = JSON.parse(raw);
      if (Date.now() - t.startTime > 36e5) {
        await this.state.storage.delete("tournament");
        return null;
      }
      return t;
    } catch {
      return null;
    }
  }
  async getHistoryData() {
    try {
      return JSON.parse(await this.state.storage.get("history") || "[]");
    } catch {
      return [];
    }
  }
};
var Feedback = class {
  static {
    __name(this, "Feedback");
  }
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }
  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace("/api/feedback", "");
    const H = { "Access-Control-Allow-Origin": "*", "Content-Type": "application/json" };
    try {
      if (request.method === "POST" && path.startsWith("/submit")) {
        const body = await request.json();
        return new Response(JSON.stringify(await this.submit(body)), { headers: H });
      }
      if (request.method === "GET" && path.startsWith("/list")) {
        const sort = url.searchParams.get("sort") || "top";
        const topic = url.searchParams.get("topic") || null;
        const category = url.searchParams.get("category") || null;
        const limit = Math.min(50, parseInt(url.searchParams.get("limit")) || 20);
        const offset = parseInt(url.searchParams.get("offset")) || 0;
        return new Response(JSON.stringify(await this.list(sort, topic, category, limit, offset)), { headers: H });
      }
      if (request.method === "POST" && path.startsWith("/vote")) {
        const body = await request.json();
        return new Response(JSON.stringify(await this.vote(body)), { headers: H });
      }
      if (request.method === "GET" && path.startsWith("/stats")) {
        return new Response(JSON.stringify(await this.getStats()), { headers: H });
      }
      return new Response(JSON.stringify({ ok: false, error: "Not found" }), { status: 404, headers: H });
    } catch (err) {
      return new Response(JSON.stringify({ ok: false, error: err.message }), { status: 500, headers: H });
    }
  }
  async submit(data) {
    const { playerId, playerName, category, topic, text } = data;
    if (!playerId || !playerName || !category || !topic || !text) return { ok: false, error: "Missing fields" };
    if (text.length > 100) return { ok: false, error: "Max 100 characters" };
    const VALID_CATEGORIES = ["bug", "balance", "remove", "add", "qol", "praise"];
    if (!VALID_CATEGORIES.includes(category)) return { ok: false, error: "Invalid category" };
    const playerKey = "rate:" + playerId;
    const rateData = JSON.parse(await this.state.storage.get(playerKey) || '{"count":0,"resetAt":0}');
    if (Date.now() < rateData.resetAt && rateData.count >= 5) return { ok: false, error: "Rate limited \u2014 max 5/hour" };
    if (Date.now() >= rateData.resetAt) {
      rateData.count = 0;
      rateData.resetAt = Date.now() + 36e5;
    }
    rateData.count++;
    await this.state.storage.put(playerKey, JSON.stringify(rateData));
    const ticket = {
      id: crypto.randomUUID(),
      playerId,
      playerName,
      category,
      topic,
      text: text.slice(0, 100),
      up: 0,
      down: 0,
      voters: {},
      // { playerId: 'up'|'down' }
      ts: Date.now()
    };
    await this.state.storage.put("ticket:" + ticket.id, JSON.stringify(ticket));
    const stats = JSON.parse(await this.state.storage.get("_stats") || "{}");
    const topicKey = topic + ":" + category;
    stats[topicKey] = (stats[topicKey] || 0) + 1;
    stats._total = (stats._total || 0) + 1;
    await this.state.storage.put("_stats", JSON.stringify(stats));
    return { ok: true, ticket: this.sanitize(ticket) };
  }
  async vote(data) {
    const { ticketId, playerId, vote } = data;
    if (!ticketId || !playerId || !["up", "down"].includes(vote)) return { ok: false, error: "Invalid vote" };
    const raw = await this.state.storage.get("ticket:" + ticketId);
    if (!raw) return { ok: false, error: "Ticket not found" };
    const ticket = JSON.parse(raw);
    const prev = ticket.voters[playerId];
    if (prev === "up") ticket.up--;
    if (prev === "down") ticket.down--;
    if (prev === vote) {
      delete ticket.voters[playerId];
    } else {
      ticket.voters[playerId] = vote;
      if (vote === "up") ticket.up++;
      if (vote === "down") ticket.down++;
    }
    await this.state.storage.put("ticket:" + ticketId, JSON.stringify(ticket));
    return { ok: true, up: ticket.up, down: ticket.down, myVote: ticket.voters[playerId] || null };
  }
  async list(sort, topic, category, limit, offset) {
    const entries = await this.state.storage.list({ prefix: "ticket:" });
    let tickets = [];
    for (const [, raw] of entries) {
      try {
        tickets.push(JSON.parse(raw));
      } catch {
      }
    }
    if (topic) tickets = tickets.filter((t) => t.topic === topic);
    if (category) tickets = tickets.filter((t) => t.category === category);
    if (sort === "top") {
      tickets.sort((a, b) => {
        const scoreA = a.up + a.down > 0 ? (a.up - a.down) / (a.up + a.down + 1) + a.up * 0.01 : 0;
        const scoreB = b.up + b.down > 0 ? (b.up - b.down) / (b.up + b.down + 1) + b.up * 0.01 : 0;
        return scoreB - scoreA;
      });
    } else if (sort === "trending") {
      const now = Date.now();
      tickets.sort((a, b) => {
        const ageA = Math.max(1, (now - a.ts) / 36e5);
        const ageB = Math.max(1, (now - b.ts) / 36e5);
        const scoreA = (a.up - a.down * 0.5) / Math.pow(ageA, 0.5);
        const scoreB = (b.up - b.down * 0.5) / Math.pow(ageB, 0.5);
        return scoreB - scoreA;
      });
    } else {
      tickets.sort((a, b) => b.ts - a.ts);
    }
    const total = tickets.length;
    tickets = tickets.slice(offset, offset + limit);
    return { ok: true, tickets: tickets.map((t) => this.sanitize(t)), total, sort, offset, limit };
  }
  async getStats() {
    const stats = JSON.parse(await this.state.storage.get("_stats") || "{}");
    return { ok: true, stats };
  }
  sanitize(t) {
    return { id: t.id, playerName: t.playerName, category: t.category, topic: t.topic, text: t.text, up: t.up, down: t.down, ts: t.ts };
  }
};
export {
  Arena,
  Feedback,
  GameRoom,
  Leaderboard,
  Marketplace,
  index_default as default
};
//# sourceMappingURL=index.js.map
