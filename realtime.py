"""
Trenchers Realtime — Slice 2: minimal PvP combat.

Server-authoritative. Clients send only {input: dx,dy, aim, fire}. The server
owns movement, bullets, hit detection, damage, deaths, respawns and the score.
First to TARGET_KILLS wins the round, then it resets.

Protocol
  client -> server : {"t":"input","dx":-1..1,"dy":-1..1,"aim":radians,"fire":bool}
                     {"t":"name","name":"..."}
  server -> client : {"t":"welcome","id","room","w","h"}
                     {"t":"state","phase","winner","target",
                        "players":[{id,name,x,y,hp,aim,alive,kills}],
                        "bullets":[{x,y}]}
"""

import asyncio
import json
import math
import os
import random
import uuid
from contextlib import asynccontextmanager
from typing import Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# --- tunables ---
TICK_HZ = 20
DT = 1.0 / TICK_HZ
SPEED = 230.0
W, H = 960, 540
MAX_PLAYERS = 8

MAX_HP = 100
FIRE_CD = 0.25
BULLET_SPEED = 540.0
BULLET_DMG = 20
BULLET_LIFE = 1.1
PLAYER_HIT_R = 18.0
RESPAWN = 2.0
TARGET_KILLS = 5
RESET_DELAY = 5.0


class Player:
    def __init__(self, pid, name, ws):
        self.id = pid
        self.name = name
        self.ws = ws
        self.x = random.uniform(120, W - 120)
        self.y = random.uniform(120, H - 120)
        self.dx = 0.0
        self.dy = 0.0
        self.aim = 0.0
        self.fire = False
        self.hp = MAX_HP
        self.alive = True
        self.kills = 0
        self.fire_cd = 0.0
        self.respawn = 0.0

    def spawn(self):
        self.x = random.uniform(120, W - 120)
        self.y = random.uniform(120, H - 120)
        self.hp = MAX_HP
        self.alive = True
        self.fire_cd = 0.0


class Room:
    def __init__(self, code):
        self.code = code
        self.players: Dict[str, Player] = {}
        self.bullets: List[dict] = []
        self.phase = "play"      # "play" | "over"
        self.winner = None
        self.reset_timer = 0.0

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


def step_room(room: Room):
    # match reset countdown
    if room.phase == "over":
        room.reset_timer -= DT
        if room.reset_timer <= 0:
            room.reset_match()

    # players: cooldowns, respawns, movement, firing
    for p in room.players.values():
        if p.fire_cd > 0:
            p.fire_cd -= DT
        if not p.alive:
            p.respawn -= DT
            if p.respawn <= 0:
                p.spawn()
            continue
        p.x = min(W - 16, max(16, p.x + p.dx * SPEED * DT))
        p.y = min(H - 16, max(16, p.y + p.dy * SPEED * DT))
        if room.phase == "play" and p.fire and p.fire_cd <= 0:
            p.fire_cd = FIRE_CD
            room.bullets.append({
                "o": p.id,
                "x": p.x + math.cos(p.aim) * 18,
                "y": p.y + math.sin(p.aim) * 18,
                "vx": math.cos(p.aim) * BULLET_SPEED,
                "vy": math.sin(p.aim) * BULLET_SPEED,
                "life": BULLET_LIFE,
            })

    # bullets
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
                p.hp -= BULLET_DMG
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
        "players": [
            {"id": p.id, "name": p.name, "x": round(p.x, 1), "y": round(p.y, 1),
             "hp": p.hp, "aim": round(p.aim, 3), "alive": p.alive, "kills": p.kills}
            for p in room.players.values()
        ],
        "bullets": [{"x": round(b["x"], 1), "y": round(b["y"], 1)} for b in room.bullets],
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


app = FastAPI(title="Trenchers Realtime", version="0.2.0", lifespan=lifespan)


@app.get("/")
def root():
    return {"name": "Trenchers Realtime", "ok": True, "mode": "pvp"}


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
                dx = float(m.get("dx", 0) or 0)
                dy = float(m.get("dy", 0) or 0)
                mag = (dx * dx + dy * dy) ** 0.5
                if mag > 1:
                    dx /= mag
                    dy /= mag
                player.dx = dx
                player.dy = dy
                player.aim = float(m.get("aim", player.aim) or 0)
                player.fire = bool(m.get("fire", False))
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
