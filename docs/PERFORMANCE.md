# Server Performance — Measurements & Roadmap

_Last updated: 2026-06-10. Measurements from `npm run loadtest` (40 clients,
3 monster zones, 10Hz input, 20s) against `wrangler dev --local` on the
dev container. Absolute numbers vary by machine; ratios are what matter._

## Architecture recap

One `GameRoom` Durable Object per room: 45Hz tick loop (`TICK_RATE=22`ms),
WebSocket + JSON, server-authoritative state, room-wide `tick` broadcast
serialized once per tick and fanned out to every socket. Singleton DOs for
`Marketplace`, `Leaderboard`, `Arena`, `Feedback`.

## Current numbers (after the optimizations below)

Measured against the post-"sync with deployed source" lineage
(`afd07f0`), wrangler 4, 40 clients:

| Metric (40 clients) | Before | After |
| --- | --- | --- |
| Downstream per client | 45.2 KiB/s | **34.3 KiB/s (−24%)** |
| Tick cadence p50 / p95 (consecutive-seq pairs) | 23 / 24 ms | 23 / 24 ms |

(On the earlier pre-sync lineage the same changes measured −33% from a
higher 85.5 KiB/s baseline; the deployed-source lineage broadcasts less
to begin with.)

Tick CPU is healthy — the loop holds its 22ms cadence at 40 clients with
~34 active monsters. **Bandwidth is the scaling constraint**, and ~99% of
all downstream bytes are `tick` broadcasts. Byte composition of a tick
before optimization: monsters ~49%, players ~40%, events ~8%, envelope ~3%.

Measurement notes: the load test computes cadence only between ticks with
consecutive `seq` — a `seq` jump means the server deliberately sent
nothing that tick (nothing dirty, or monster data deferred by the
broadcast divisor), which would otherwise read as a fake 2× interval.
At the 50-player room cap the *local* harness also starts saturating the
dev container's 4 shared cores; production DO hardware differs, so
re-measure there if rooms run full.

## Done

### Round 1 (CPU + storage; originally PR #1, re-ported after the repo
### was force-synced to the deployed worker source)
- O(1) player→socket reverse index (was a full session scan per lookup,
  ~22 call sites including per-tick paths).
- Write-behind batched RPG persistence: one multi-key `storage.put` per
  tick instead of a full-blob put per mutation; flush-on-disconnect.
- Monster AI: players bucketed by zone once per tick (was O(zones×players)
  full-state spread copies), squared-distance comparisons (adapted to the
  sticky-aggro override and Y-scaled attack ring of the deployed source).
- Lag-comp history ring buffer (was push+shift per player per tick).

### Round 2 (bandwidth + correctness + tooling)
- **Wire-precision rounding** (~14% bandwidth): tick player positions to
  0.1px (raw physics floats were ~18 chars each), `monster_attack` coords
  to ints, `monster_hit.hpPct` and loot `shares` to 4 decimals. Rounding
  happens only at serialization; stored state keeps full precision so
  movement validation never sees drift.
- **22Hz monster broadcasts** (~23% bandwidth): `MONSTER_BROADCAST_DIVISOR=2`
  sends monster state every 2nd tick. Chasing monsters re-dirty their zone
  at 45Hz and clients interpolate, so the halved cadence is invisible.
  AI/combat still run at 45Hz; skipped ticks keep zones dirty. Set the
  divisor to 1 to restore per-tick broadcasts.
- **Duplicate-login fix**: rejoining with the same player id evicts the old
  socket; the stale socket's close no longer wipes the live player's state
  or broadcasts a bogus `player_leave`.
- **Singleton DO fixes**: Leaderboard views were a full `storage.list` +
  parse of every player row per request — now an in-memory table kept in
  sync by `updatePlayer`. Marketplace queries skip whole index buckets on
  a key check; expiry sweep uses batched deletes and no longer reads
  `_lastPurge` from storage per request.
- **Test harness**: `npm run smoke` (13 protocol assertions) and
  `npm run loadtest` (`CLIENTS=… DURATION=…` env overrides). Both need
  `wrangler dev --port 8787 --local` running and Node ≥ 21.

## Roadmap (highest value first)

All remaining big wins change the wire protocol, so they need matching
changes in the client repo (`hemibroscommunity-del/GameDev`, `BroTown.jsx`).
Ship them behind a `protocolVersion: 2` field in the client's `join`
message — the server keeps sending v1 payloads to old clients, so deploys
stay safe in either order (this repo auto-deploys `main` to production).

1. **Zone-scoped tick broadcasts (interest management).** Every client
   currently receives every zone's monster/node/event data; with 3 active
   zones that's ~3× the monster bytes each client needs. Group sessions by
   `ps.z`, serialize one tick variant per zone (players field stays
   room-wide if the client's minimap needs it). The client already
   resyncs on zone entry via `zone_monsters`/`zone_nodes`/`zone_loot`, so
   v2 clients only need to *not expect* other zones' data. Combined with
   item 2 this is most of the remaining ~57 KiB/s.
2. **Per-entity monster deltas.** A dirty zone resends *all* its monsters;
   send only changed ones and have the client merge by `id` instead of
   replacing the zone array. Also lets dead-and-waiting monsters drop out
   of the payload.
3. **Delta `player_state`.** `_sendPlayerState` ships the full snapshot
   (entire inventory, six equipment blobs, stash, quests, buffs) on every
   combat hit / regen / pickup / shop action; most emits change only
   hp/stamina/mana or coins. Send changed fields; client merges. Small
   share of room bandwidth but the largest single messages on the wire.
4. **Merge `zone_monsters`/`zone_nodes`/`zone_loot`** into one `zone_state`
   message on zone change (trivial once 1–2 are done).
5. **Trim `state_sync` peer payloads.** `getAllPlayerData()` spreads each
   player's *entire* server state — inventory, coins, quest flags,
   `_perfectHistory` — into the join snapshot every new client receives.
   That's both join-size bloat (grows with every player's inventory) and
   an information leak: any client can read every other player's
   inventory and coins from the wire. The v2 client should declare what
   it actually renders for peers (position, name, level, hp, equipment
   visuals) and the server should whitelist exactly that. Not done in v1
   because the current client's reads from peer data are unknown.

Not worth it yet: binary encoding (JSON+deltas gets ~10× first; revisit
only if v2 isn't enough), loot pile Maps (user-action frequency), PvP hit
scan pre-filtering (O(50) with cheap early-outs).

## Known issues / watch list

- `_perfectHistory`, `_quests`, inventory etc. ride every `player_state`
  full-snapshot — fixed by roadmap item 3.
- `Arena`/`Feedback` DOs are storage-light and fine; DO storage gets are
  runtime-cached.
- The Cloudflare Git integration builds on every push and deploys `main`
  to production — keep `main` deployable; wire-incompatible work must be
  version-gated as above.
