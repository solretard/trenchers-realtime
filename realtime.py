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
import uuid
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# --- core tunables (client mirrors SPEED and MOVE_STEP) ---
TICK_HZ = 30
DT = 1.0 / TICK_HZ
SPEED = 230.0
MOVE_STEP = 1.0 / 30.0
W, H = 960, 540
MAX_PLAYERS = 8

MAX_HP = 100
BULLET_SPEED = 540.0
BULLET_LIFE = 2.4
PLAYER_HIT_R = 18.0
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


def clampx(v): return min(W - 16, max(16, v))
def clampy(v): return min(H - 16, max(16, v))


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
        self.x = clampx(random.uniform(120, W - 120))
        self.y = clampy(random.uniform(120, H - 120))
        self.aim = 0.0
        self.hp = MAX_HP
        self.alive = True
        self.kills = 0
        self.fire_cd = 0.0
        self.respawn = 0.0
        self.last_seq = 0
        self.equipped: Optional[str] = None
        self.chosen_faction: Optional[str] = None

    def spawn(self):
        self.x = clampx(random.uniform(120, W - 120))
        self.y = clampy(random.uniform(120, H - 120))
        self.hp = MAX_HP
        self.alive = True
        self.fire_cd = 0.0


class Room:
    def __init__(self, code):
        self.code = code
        self.players: Dict[str, Player] = {}
        self.bullets: List[dict] = []
        self.phase = "play"
        self.winner = None
        self.reset_timer = 0.0
        self.market: Dict[str, Token] = {s: Token(s) for s in MARKET_SYMS}
        self.war: Dict[str, int] = {f: 0 for f in FACTIONS}

    def reset_match(self):
        self.bullets.clear()
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
    if p.equipped:
        tok = room.market.get(p.equipped)
        if tok and not tok.rugged:
            return GUNS[tier_of(tok.mcap)]
    return DEFAULT_GUN


def player_tier(room: Room, p: Player) -> int:
    if p.equipped:
        tok = room.market.get(p.equipped)
        if tok and not tok.rugged:
            return tier_of(tok.mcap)
    return -1   # -1 = default/unarmed


def apply_input(room, p, dx, dy, aim, fire, seq):
    mag = (dx * dx + dy * dy) ** 0.5
    if mag > 1:
        dx /= mag; dy /= mag
    if p.alive:
        p.x = clampx(p.x + dx * SPEED * MOVE_STEP)
        p.y = clampy(p.y + dy * SPEED * MOVE_STEP)
    p.aim = aim
    if fire and p.alive and room.phase == "play" and p.fire_cd <= 0:
        gun = player_gun(room, p)
        p.fire_cd = gun["cd"]
        for k in range(gun["pellets"]):
            spread = gun["spread"]
            a = aim + (random.uniform(-spread, spread) if spread else 0.0)
            room.bullets.append({
                "o": p.id,
                "x": p.x + math.cos(a) * 18,
                "y": p.y + math.sin(a) * 18,
                "vx": math.cos(a) * BULLET_SPEED,
                "vy": math.sin(a) * BULLET_SPEED,
                "life": BULLET_LIFE,
                "dmg": gun["dmg"],
            })
    p.last_seq = seq


def step_room(room: Room):
    if room.phase == "over":
        room.reset_timer -= DT
        if room.reset_timer <= 0:
            room.reset_match()

    for tok in room.market.values():
        tok.step()

    for p in room.players.values():
        if p.fire_cd > 0:
            p.fire_cd -= DT
        if not p.alive:
            p.respawn -= DT
            if p.respawn <= 0:
                p.spawn()

    alive_bullets = []
    for b in room.bullets:
        b["x"] += b["vx"] * DT
        b["y"] += b["vy"] * DT
        b["life"] -= DT
        if b["life"] <= 0 or b["x"] < -20 or b["x"] > W + 20 or b["y"] < -20 or b["y"] > H + 20:
            continue
        hit = False
        for p in room.players.values():
            if not p.alive or p.id == b["o"]:
                continue
            if (p.x - b["x"]) ** 2 + (p.y - b["y"]) ** 2 <= PLAYER_HIT_R ** 2:
                p.hp -= b["dmg"]
                hit = True
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
             "faction": resolve_faction(p)}
            for p in room.players.values()
        ],
        "bullets": [
            {"x": round(b["x"], 1), "y": round(b["y"], 1),
             "vx": round(b["vx"], 1), "vy": round(b["vy"], 1)}
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
    await ws.send_text(json.dumps({"t": "welcome", "id": pid, "room": room.code, "w": W, "h": H}))
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
