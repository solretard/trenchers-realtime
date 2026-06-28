# Trenchers Realtime (Slice 1)

The real-time multiplayer foundation: an **authoritative websocket server** where
players join a room by code, move, and see each other live. This is a **separate
service** from your leaderboard API — both run independently and both deploy to Railway.

Slice 1 proves the hard part (server tick loop + websockets + sync). Combat/PvP
layers on top once this works.

---

## Run it locally

```bash
cd trenchers-realtime
pip3 install -r requirements.txt
python3 -m uvicorn realtime:app --reload
```

You should see `Uvicorn running on http://127.0.0.1:8000`.
Check it's alive: open http://127.0.0.1:8000/health → `{"ok":true,"rooms":0,"players":0}`.

## Test two players

1. Open **`mp-test.html`** in your browser (the `REALTIME_URL` at the top is already
   set to `ws://127.0.0.1:8000` for local testing).
2. Type a callsign + any room code (e.g. `TRENCH1`) and hit **ENTER THE TRENCH**.
3. Open the **same file in a second window** (or a second browser), join the **same
   room code**.
4. Move with WASD or by dragging. **You should see both dots move in real time.**

That's Slice 1 working: two players, one room, live sync.

---

## Deploy to Railway

Same flow as the leaderboard backend — push this folder to its own GitHub repo,
create a new Railway project from it, generate a domain.

Then in `mp-test.html`, change the config to your Railway URL using **wss://** (secure):

```js
const REALTIME_URL = "wss://your-realtime.up.railway.app";
```

> Note: deployed over https you MUST use `wss://` (not `ws://`), or browsers block it.

---

## What's next (not in this slice)

- **PvP combat** — shooting, damage, who-killed-who, all server-authoritative.
- **Token-gun / faction systems** synced across players.
- **Invite links**, then **matchmaking** ("find online players").

Each builds on this foundation. If two dots move in sync here, the path is open.
