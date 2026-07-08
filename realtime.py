"""
Trenchers Realtime — Slice 3a: PvP + the token-as-gun mechanic.

Builds on the v0.3.0 netcode (client prediction + reconciliation for movement,
interpolation for others). Adds a SERVER-AUTHORITATIVE token market: players
"ape" a token to arm themselves, their gun tier scales with that token's market
cap, and tokens can rug (you lose the gun until you ape another or it recovers).

Protocol additions
  client -> server : {"t":"ape","sym":"WAGMI"}
  server -> client : state now includes
       "market":[{sym,mcap,tier,rugged}],
       and each player has "tok" (equipped symbol|null) and "tier" (effective)
"""

import asyncio
import json
import math
import random
import time
import uuid
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# --- core tunables (client mirrors SPEED and MOVE_STEP) ---
TICK_HZ = 30
DT = 1.0 / TICK_HZ
SPEED = 230.0
MOVE_STEP = 1.0 / 30.0
MAX_INPUTS_PER_SEC = 45   # legit clients send ~30/sec; cap flooding without throttling honest play
PADX, PADY = 480, 300           # buffer border so the camera can keep you centered to the wall
PLAY_W, PLAY_H = 1600, 1000     # arena matched to Survive mode
W, H = PLAY_W + 2 * PADX, PLAY_H + 2 * PADY   # world incl. buffer (2880 x 1680)
MAX_PLAYERS = 8

MAX_HP = 100
BULLET_SPEED = 540.0
BULLET_LIFE = 2.4
PLAYER_HIT_R = 22.0   # matches the on-screen character body (18x21 sprite @ 3x); was 18 (bullets clipped the visible body but missed)
RESPAWN = 2.0
TARGET_KILLS = 5
RESET_DELAY = 5.0

# --- token market ---
MARKET_SYMS = ["WAGMI", "JEET", "COPE", "APED", "REKT", "BAGS"]
RUG_CHANCE = 0.0016          # per token per tick
MCAP_MIN, MCAP_MAX = 4_000.0, 5_000_000.0

# gun tiers by mcap. (cd seconds, dmg, pellets, spread radians)
def tier_of(mcap: float) -> int:
    if mcap < 25_000:   return 0   # PISTOL
    if mcap < 100_000:  return 1   # SMG
    if mcap < 500_000:  return 2   # RIFLE
    if mcap < 2_000_000:return 3   # LASER
    return 4                       # CANNON

GUNS = [
    {"name": "PISTOL", "cd": 0.42, "dmg": 18, "pellets": 1, "spread": 0.0},
    {"name": "SMG",    "cd": 0.13, "dmg": 10, "pellets": 1, "spread": 0.05},
    {"name": "RIFLE",  "cd": 0.28, "dmg": 26, "pellets": 1, "spread": 0.0},
    {"name": "LASER",  "cd": 0.20, "dmg": 20, "pellets": 1, "spread": 0.0},
    {"name": "CANNON", "cd": 0.70, "dmg": 14, "pellets": 5, "spread": 0.34},
]
DEFAULT_GUN = {"name": "SWORD-PISTOL", "cd": 0.55, "dmg": 12, "pellets": 1, "spread": 0.0}
POWER_GUN   = {"name": "GOLDEN MINIGUN", "cd": 0.07, "dmg": 16, "pellets": 1, "spread": 0.03}

# --- pickups (shield / heal / powerful gun): one of each every 5 minutes, first-come ---
PICKUP_INTERVAL = 300.0   # seconds between spawns of each pickup kind
SHIELD_DURATION = 10.0    # shield lasts this long once picked up
POWER_DURATION  = 25.0    # power gun lasts this long once picked up
HEAL_AMOUNT     = 70      # hp restored by the heal orb
PICKUP_R        = 22.0    # grab radius

# --- bomb ability: aim + press bomb, 15s cooldown, AoE explosion ---
BOMB_CD     = 15.0
BOMB_SPEED  = 300.0
BOMB_FUSE   = 1.1         # seconds until it explodes if it hits nothing
BOMB_RADIUS = 70.0        # blast radius
BOMB_DMG    = 60          # center damage (falls off with distance)

# --- factions (Version A: faction is the player's lobby choice) ---
FACTIONS = ["DIAMONDS", "APES", "SNIPERS", "COPERS"]


def resolve_faction(player) -> str:
    """The ONE swap-point for the NFT hook.

    Version A (now): faction is whatever the player chose in the lobby.
    Version B (later, at mint): replace the body of this function to read the
    player's faction from the Trenchers NFT held in their connected wallet.
    Nothing else in the game needs to change.
    """
    return player.chosen_faction or "—"


def clampx(v): return min(W - PADX, max(PADX, v))
def clampy(v): return min(H - PADY, max(PADY, v))


# --- cover: rectangles (x, y, w, h) in world space, identical for every client ---
# symmetric layout so neither side has an advantage
COVER = [
    (px + PADX, py + PADY, cw, ch) for (px, py, cw, ch) in [
        (250, 230, 130, 40), (1220, 230, 130, 40),
        (250, 730, 130, 40), (1220, 730, 130, 40),
        (140, 425, 40, 150), (1420, 425, 40, 150),
        (700, 150, 200, 40), (700, 810, 200, 40),
        (500, 415, 40, 170), (1060, 415, 40, 170),
        (750, 440, 100, 100),
    ]
]
PLAYER_R = 16


def blocked(px, py):
    for (ox, oy, ow, oh) in COVER:
        if ox - PLAYER_R <= px <= ox + ow + PLAYER_R and oy - PLAYER_R <= py <= oy + oh + PLAYER_R:
            return True
    return False


def in_cover(px, py):
    for (ox, oy, ow, oh) in COVER:
        if ox <= px <= ox + ow and oy <= py <= oy + oh:
            return True
    return False


class Token:
    def __init__(self, sym):
        self.sym = sym
        self.mcap = random.choice([12_000, 60_000, 220_000, 900_000, 3_200_000]) * random.uniform(0.6, 1.4)
        self.rugged = False
        self.recover = 0.0

    def step(self):
        if self.rugged:
            self.recover -= DT
            if self.recover <= 0:
                if random.random() < 0.6:
                    self.rugged = False
                    self.mcap = random.uniform(30_000, 320_000)
                else:
                    self.recover = random.uniform(3.0, 7.0)
            return
        # random walk in log space
        self.mcap *= math.exp(random.uniform(-0.05, 0.052))
        self.mcap = min(MCAP_MAX, max(MCAP_MIN, self.mcap))
        if random.random() < RUG_CHANCE:
            self.rugged = True
            self.mcap = random.uniform(2_000, 6_000)
            self.recover = random.uniform(4.0, 9.0)


class Player:
    def __init__(self, pid, name, ws):
        self.id = pid
        self.name = name
        self.ws = ws
        self.x = clampx(random.uniform(PADX + 60, W - PADX - 60))
        self.y = clampy(random.uniform(PADY + 60, H - PADY - 60))
        self.aim = 0.0
        self.hp = MAX_HP
        self.alive = True
        self.kills = 0
        self.fire_cd = 0.0
        self.respawn = 0.0
        self.last_seq = 0
        self.equipped: Optional[str] = None
        self.chosen_faction: Optional[str] = None
        self.loadout: Optional[int] = None   # starting gun tier (0..2) chosen in loadout screen
        self.input_times = []                 # timestamps for input-rate limiting (anti speed-hack)
        self.shield_until = 0.0               # server-time until which shield is active
        self.power_until = 0.0                # server-time until which the power gun is held
        self.bomb_cd = 0.0                    # bomb cooldown remaining

    def spawn(self):
        for _ in range(30):
            nx = clampx(random.uniform(PADX + 60, W - PADX - 60))
            ny = clampy(random.uniform(PADY + 60, H - PADY - 60))
            if not blocked(nx, ny):
                break
        self.x = nx
        self.y = ny
        self.hp = MAX_HP
        self.alive = True
        self.fire_cd = 0.0
        self.shield_until = 0.0               # lose shield on death
        self.power_until = 0.0                # lose power gun on death


class Room:
    def __init__(self, code):
        self.code = code
        self.players: Dict[str, Player] = {}
        self.bullets: List[dict] = []
        self.phase = "waiting"
        self.winner = None
        self.reset_timer = 0.0
        self.market: Dict[str, Token] = {s: Token(s) for s in MARKET_SYMS}
        self.war: Dict[str, int] = {f: 0 for f in FACTIONS}
        self.pickups: List[dict] = []                      # active {id,kind,x,y}
        self.pickup_timers = {"shield": 30.0, "heal": 60.0, "gun": 90.0}  # staggered first spawns
        self.bombs: List[dict] = []
        self._pid = 0

    def reset_match(self):
        self.bullets.clear()
        self.bombs.clear()
        self.pickups.clear()
        self.pickup_timers = {"shield": 30.0, "heal": 60.0, "gun": 90.0}
        self.phase = "play"
        self.winner = None
        for p in self.players.values():
            p.kills = 0
            p.spawn()


rooms: Dict[str, Room] = {}


def get_room(code):
    code = code.upper()
    if code not in rooms:
        rooms[code] = Room(code)
    return rooms[code]


def player_gun(room: Room, p: Player) -> dict:
    if p.power_until > time.monotonic():
        return POWER_GUN
    if p.equipped:
        tok = room.market.get(p.equipped)
        if tok and not tok.rugged:
            return GUNS[tier_of(tok.mcap)]
    if p.loadout is not None:
        return GUNS[p.loadout]
    return DEFAULT_GUN


def player_tier(room: Room, p: Player) -> int:
    if p.equipped:
        tok = room.market.get(p.equipped)
        if tok and not tok.rugged:
            return tier_of(tok.mcap)
    return -1   # -1 = default/unarmed


def apply_input(room, p, dx, dy, aim, fire, seq, bomb=False):
    # reject non-finite values (NaN/inf) — a hacked client could send these to corrupt state
    if not (math.isfinite(dx) and math.isfinite(dy) and math.isfinite(aim)):
        return
    # rate-limit inputs per player: legit clients send ~30/sec, so a generous cap
    # stops speed-hack input-flooding without ever throttling honest movement.
    now = time.monotonic()
    p.input_times.append(now)
    cutoff = now - 1.0
    while p.input_times and p.input_times[0] < cutoff:
        p.input_times.pop(0)
    flooding = len(p.input_times) > MAX_INPUTS_PER_SEC
    mag = (dx * dx + dy * dy) ** 0.5
    if mag > 1:
        dx /= mag; dy /= mag
    # move one step per input (matches client-side prediction → smooth, no rubber-banding)
    if p.alive and not flooding:
        nx = clampx(p.x + dx * SPEED * MOVE_STEP)
        if not blocked(nx, p.y):
            p.x = nx
        ny = clampy(p.y + dy * SPEED * MOVE_STEP)
        if not blocked(p.x, ny):
            p.y = ny
    p.aim = aim
    if fire and p.alive and room.phase == "play" and p.fire_cd <= 0:
        gun = player_gun(room, p)
        p.fire_cd = gun["cd"]
        for k in range(gun["pellets"]):
            spread = gun["spread"]
            a = aim + (random.uniform(-spread, spread) if spread else 0.0)
            room.bullets.append({
                "o": p.id,
                "fac": resolve_faction(p),
                "x": p.x + math.cos(a) * 18,
                "y": p.y + math.sin(a) * 18,
                "vx": math.cos(a) * BULLET_SPEED,
                "vy": math.sin(a) * BULLET_SPEED,
                "life": BULLET_LIFE,
                "dmg": gun["dmg"],
            })
    # bomb throw: aim + press, 15s cooldown, spawns an AoE projectile
    if bomb and p.alive and room.phase == "play" and p.bomb_cd <= 0:
        p.bomb_cd = BOMB_CD
        room.bombs.append({
            "o": p.id,
            "x": p.x + math.cos(aim) * 18,
            "y": p.y + math.sin(aim) * 18,
            "vx": math.cos(aim) * BOMB_SPEED,
            "vy": math.sin(aim) * BOMB_SPEED,
            "fuse": BOMB_FUSE,
        })
    p.last_seq = seq
    """Squared distance from point (px,py) to the segment (ax,ay)->(bx,by)."""
    dx, dy = bx - ax, by - ay
    seg2 = dx * dx + dy * dy
    if seg2 == 0.0:
        return (px - ax) ** 2 + (py - ay) ** 2
    t = ((px - ax) * dx + (py - ay) * dy) / seg2
    if t < 0.0:
        t = 0.0
    elif t > 1.0:
        t = 1.0
    cx, cy = ax + t * dx, ay + t * dy
    return (px - cx) ** 2 + (py - cy) ** 2


def step_room(room: Room):
    # ---- authoritative waiting gate: match only runs with 2+ ACTIVE players ----
    # count only players who've sent input recently — a lingering/half-closed socket
    # from a reload stops sending input, so it won't falsely trip the gate.
    now_m = time.monotonic()
    live_count = sum(
        1 for p in room.players.values()
        if p.input_times and (now_m - p.input_times[-1]) <= 3.0
    )
    if live_count < 2:
        # not enough players — hold in "waiting", freeze combat/pickups/bombs
        if room.phase != "waiting":
            room.phase = "waiting"
            room.bullets.clear()
            room.bombs.clear()
            room.pickups.clear()
            room.pickup_timers = {"shield": 30.0, "heal": 60.0, "gun": 90.0}
        # keep the lone player idle-alive so they see the arena behind the overlay
        for p in room.players.values():
            if not p.alive:
                p.respawn -= DT
                if p.respawn <= 0:
                    p.spawn()
        return
    if room.phase == "waiting":
        # a second player just arrived — kick off a fresh match
        room.reset_match()

    if room.phase == "over":
        room.reset_timer -= DT
        if room.reset_timer <= 0:
            room.reset_match()

    for tok in room.market.values():
        tok.step()

    for p in room.players.values():
        if p.fire_cd > 0:
            p.fire_cd -= DT
        if p.bomb_cd > 0:
            p.bomb_cd -= DT
        if not p.alive:
            p.respawn -= DT
            if p.respawn <= 0:
                p.spawn()

    # ---- pickups: spawn one of each kind every PICKUP_INTERVAL; first player to touch grabs it ----
    if room.phase == "play":
        for kind in ("shield", "heal", "gun"):
            room.pickup_timers[kind] -= DT
            has_active = any(pk["kind"] == kind for pk in room.pickups)
            if room.pickup_timers[kind] <= 0 and not has_active:
                for _ in range(30):
                    px = clampx(random.uniform(PADX + 60, W - PADX - 60))
                    py = clampy(random.uniform(PADY + 60, H - PADY - 60))
                    if not blocked(px, py):
                        break
                room._pid += 1
                room.pickups.append({"id": room._pid, "kind": kind, "x": px, "y": py})
                room.pickup_timers[kind] = PICKUP_INTERVAL
        # collection — first alive player within range wins it
        now2 = time.monotonic()
        remaining = []
        for pk in room.pickups:
            grabbed = False
            for p in room.players.values():
                if not p.alive:
                    continue
                if (p.x - pk["x"]) ** 2 + (p.y - pk["y"]) ** 2 <= PICKUP_R ** 2:
                    if pk["kind"] == "shield":
                        p.shield_until = now2 + SHIELD_DURATION
                    elif pk["kind"] == "heal":
                        p.hp = min(MAX_HP, p.hp + HEAL_AMOUNT)
                    elif pk["kind"] == "gun":
                        p.power_until = now2 + POWER_DURATION
                    grabbed = True
                    break
            if not grabbed:
                remaining.append(pk)
        room.pickups = remaining

    # ---- bombs: travel, then explode (on player contact or fuse) with AoE ----
    live_bombs = []
    for bomb in room.bombs:
        bomb["x"] += bomb["vx"] * DT
        bomb["y"] += bomb["vy"] * DT
        bomb["fuse"] -= DT
        exploded = bomb["fuse"] <= 0 or in_cover(bomb["x"], bomb["y"]) \
            or bomb["x"] < 0 or bomb["x"] > W or bomb["y"] < 0 or bomb["y"] > H
        if not exploded:
            for p in room.players.values():
                if p.alive and p.id != bomb["o"] and \
                        (p.x - bomb["x"]) ** 2 + (p.y - bomb["y"]) ** 2 <= (PLAYER_HIT_R + 6) ** 2:
                    exploded = True
                    break
        if exploded:
            bomb["boom"] = 0.35   # brief explosion marker for the client
            for p in room.players.values():
                if not p.alive:
                    continue
                d = ((p.x - bomb["x"]) ** 2 + (p.y - bomb["y"]) ** 2) ** 0.5
                if d <= BOMB_RADIUS and p.shield_until <= time.monotonic():
                    dmg = int(BOMB_DMG * (1 - d / BOMB_RADIUS))
                    p.hp -= dmg
                    if p.hp <= 0 and p.alive:
                        p.alive = False
                        p.respawn = RESPAWN
                        shooter = room.players.get(bomb["o"])
                        if shooter and shooter.id != p.id and room.phase == "play":
                            shooter.kills += 1
                            if shooter.kills >= TARGET_KILLS:
                                room.phase = "over"
                                room.winner = shooter.name
                                room.reset_timer = RESET_DELAY
                                fac = resolve_faction(shooter)
                                if fac in room.war:
                                    room.war[fac] += 1
            room.blasts = getattr(room, "blasts", [])
            room.blasts.append({"x": bomb["x"], "y": bomb["y"], "t": 0.35})
        else:
            live_bombs.append(bomb)
    room.bombs = live_bombs
    # age explosion markers
    room.blasts = [dict(bl, t=bl["t"] - DT) for bl in getattr(room, "blasts", []) if bl["t"] - DT > 0]

    alive_bullets = []
    for b in room.bullets:
        ox, oy = b["x"], b["y"]                 # where the bullet was
        nx = ox + b["vx"] * DT
        ny = oy + b["vy"] * DT                  # where it moves to this tick
        b["x"], b["y"] = nx, ny
        b["life"] -= DT
        if b["life"] <= 0 or nx < -20 or nx > W + 20 or ny < -20 or ny > H + 20:
            continue
        if in_cover(nx, ny):
            continue
        hit = False
        for p in room.players.values():
            if not p.alive or p.id == b["o"]:
                continue
            # swept hit-test: distance from the player to the segment the bullet
            # travelled this tick — catches fast bullets that would skip over a player
            if seg_point_dist2(ox, oy, nx, ny, p.x, p.y) <= PLAYER_HIT_R ** 2:
                hit = True
                if p.shield_until > time.monotonic():
                    break   # shielded: bullet is absorbed, no damage
                p.hp -= b["dmg"]
                if p.hp <= 0:
                    p.alive = False
                    p.respawn = RESPAWN
                    shooter = room.players.get(b["o"])
                    if shooter and room.phase == "play":
                        shooter.kills += 1
                        if shooter.kills >= TARGET_KILLS:
                            room.phase = "over"
                            room.winner = shooter.name
                            room.reset_timer = RESET_DELAY
                            fac = resolve_faction(shooter)
                            if fac in room.war:
                                room.war[fac] += 1
                break
        if not hit:
            alive_bullets.append(b)
    room.bullets = alive_bullets


def room_state(room: Room) -> str:
    return json.dumps({
        "t": "state",
        "phase": room.phase,
        "winner": room.winner,
        "target": TARGET_KILLS,
        "market": [
            {"sym": tok.sym, "mcap": round(tok.mcap),
             "tier": (-1 if tok.rugged else tier_of(tok.mcap)), "rugged": tok.rugged}
            for tok in room.market.values()
        ],
        "war": [{"faction": f, "points": room.war[f]} for f in FACTIONS],
        "players": [
            {"id": p.id, "name": p.name, "x": round(p.x, 1), "y": round(p.y, 1),
             "hp": p.hp, "aim": round(p.aim, 3), "alive": p.alive,
             "kills": p.kills, "seq": p.last_seq,
             "tok": p.equipped, "tier": player_tier(room, p),
             "faction": resolve_faction(p),
             "shield": p.shield_until > time.monotonic(),
             "power": p.power_until > time.monotonic(),
             "bombcd": round(max(0.0, p.bomb_cd), 1)}
            for p in room.players.values()
        ],
        "pickups": [{"id": pk["id"], "kind": pk["kind"],
                     "x": round(pk["x"], 1), "y": round(pk["y"], 1)} for pk in room.pickups],
        "bombs": [{"x": round(b["x"], 1), "y": round(b["y"], 1)} for b in room.bombs],
        "blasts": [{"x": round(bl["x"], 1), "y": round(bl["y"], 1)} for bl in getattr(room, "blasts", [])],
        "bullets": [
            {"x": round(b["x"], 1), "y": round(b["y"], 1),
             "vx": round(b["vx"], 1), "vy": round(b["vy"], 1), "fac": b.get("fac"), "o": b["o"]}
            for b in room.bullets
        ],
    })


async def ticker():
    while True:
        await asyncio.sleep(DT)
        for code in list(rooms.keys()):
            room = rooms.get(code)
            if not room or not room.players:
                continue
            step_room(room)
            msg = room_state(room)
            dead = []
            for p in list(room.players.values()):
                try:
                    await p.ws.send_text(msg)
                except Exception:
                    dead.append(p.id)
            for pid in dead:
                room.players.pop(pid, None)
            if not room.players:
                rooms.pop(code, None)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(ticker())
    try:
        yield
    finally:
        task.cancel()


app = FastAPI(title="Trenchers Realtime", version="0.5.0", lifespan=lifespan)


@app.get("/")
def root():
    return {"name": "Trenchers Realtime", "ok": True, "mode": "pvp",
            "netcode": "predict+reconcile", "feature": "token-guns+factions"}


@app.get("/health")
def health():
    return {"ok": True, "rooms": len(rooms),
            "players": sum(len(r.players) for r in rooms.values())}


@app.websocket("/ws/{code}")
async def ws_endpoint(ws: WebSocket, code: str):
    await ws.accept()
    room = get_room(code)
    if len(room.players) >= MAX_PLAYERS:
        await ws.send_text(json.dumps({"t": "full"}))
        await ws.close()
        return
    pid = uuid.uuid4().hex[:8]
    player = Player(pid, "trencher-" + pid[:4], ws)
    room.players[pid] = player
    await ws.send_text(json.dumps({"t": "welcome", "id": pid, "room": room.code, "w": W, "h": H,
                                   "padx": PADX, "pady": PADY,
                                   "cover": [list(c) for c in COVER]}))
    try:
        while True:
            raw = await ws.receive_text()
            try:
                m = json.loads(raw)
            except Exception:
                continue
            kind = m.get("t")
            if kind == "input":
                apply_input(
                    room, player,
                    float(m.get("dx", 0) or 0),
                    float(m.get("dy", 0) or 0),
                    float(m.get("aim", player.aim) or 0),
                    bool(m.get("fire", False)),
                    int(m.get("seq", 0) or 0),
                    bool(m.get("bomb", False)),
                )
            elif kind == "ape":
                sym = str(m.get("sym", "")).upper()
                tok = room.market.get(sym)
                if tok and not tok.rugged:
                    player.equipped = sym
            elif kind == "faction":
                f = str(m.get("f", "")).upper()
                if f in FACTIONS:
                    player.chosen_faction = f
            elif kind == "loadout":
                try:
                    t = int(m.get("tier", 0))
                except (TypeError, ValueError):
                    t = 0
                player.loadout = max(0, min(2, t))   # only PISTOL/SMG/RIFLE allowed as start
            elif kind == "name":
                nm = str(m.get("name", ""))[:16].strip()
                if nm:
                    player.name = nm
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        room.players.pop(pid, None)
        if not room.players:
            rooms.pop(code, None)
