from collections import deque
from datetime import datetime, timezone
import hmac
import os
import time
from threading import RLock

from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit, join_room, leave_room
from werkzeug.security import check_password_hash, generate_password_hash

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get(
    "SECRET_KEY", "local-development-only"
)

socketio = SocketIO(app, async_mode="threading")

MASTER_PASSWORD = os.environ.get("MASTER_PASSWORD", "")
MAX_ROOMS = 100
MAX_MESSAGES_PER_ROOM = 100
MAX_MESSAGE_LENGTH = 500
MIN_PASSWORD_LENGTH = 8
MAX_PASSWORD_LENGTH = 128
MIN_SECONDS_BETWEEN_MESSAGES = 0.8

# In-memory only. Rooms and messages reset when the server restarts.
rooms = {}
current_room = {}
silent_join = {}
last_sent = {}
failed_joins = {}
rooms_lock = RLock()


def clean_room_name(value):
    value = str(value or "").strip()
    value = "".join(
        ch for ch in value if ch.isalnum() or ch in " _-"
    )
    return " ".join(value.split())[:30].strip()


def room_key(name):
    return name.casefold()


def channel_name(key):
    return "chat-room:" + key


def public_room_list():
    with rooms_lock:
        result = [
            {
                "name": room["name"],
                "users": sum(
                    1 for joined in current_room.values()
                    if joined == key
                ),
            }
            for key, room in rooms.items()
        ]
    return sorted(result, key=lambda item: item["name"].casefold())


def post_system_message(key, message):
    """Save a system message and send it only to that room."""
    item = {
        "username": "System",
        "message": message,
        "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
        "system": True,
    }

    with rooms_lock:
        room = rooms.get(key)
        if room is None:
            return
        room["history"].append(item)

    socketio.emit("chat_message", item, to=channel_name(key))


def switch_to_room(key, silent=False):
    sid = request.sid
    old_key = current_room.get(sid)

    if old_key == key:
        return

    if old_key:
        post_system_message(old_key, "A user has left")
        leave_room(channel_name(old_key))

    join_room(channel_name(key))
    current_room[sid] = key
    silent_join[sid] = silent


@app.get("/")
def index():
    return render_template("index.html")


@socketio.on("connect")
def on_connect(auth=None):
    emit("room_list", public_room_list())


@socketio.on("create_room")
def on_create_room(data):
    if not isinstance(data, dict):
        emit("room_error", {"text": "Invalid room details."})
        return

    name = clean_room_name(data.get("name", ""))
    password = str(data.get("password", ""))

    if not name:
        emit("room_error", {
            "text": "Use letters, numbers, spaces, _ or - for the room name."
        })
        return

    if not MIN_PASSWORD_LENGTH <= len(password) <= MAX_PASSWORD_LENGTH:
        emit("room_error", {
            "text": f"Passwords must be {MIN_PASSWORD_LENGTH}–{MAX_PASSWORD_LENGTH} characters."
        })
        return

    key = room_key(name)

    with rooms_lock:
        if key in rooms:
            emit("room_error", {
                "text": "That room already exists. Select it to join."
            })
            return

        if len(rooms) >= MAX_ROOMS:
            emit("room_error", {
                "text": "The server room limit has been reached."
            })
            return

        rooms[key] = {
            "name": name,
            "password_hash": generate_password_hash(password),
            "history": deque(maxlen=MAX_MESSAGES_PER_ROOM),
        }

    failed_joins.pop(request.sid, None)
    switch_to_room(key, silent=False)
    emit("room_joined", {"name": name, "history": []})
    post_system_message(key, "A user has joined")
    socketio.emit("room_list", public_room_list())


@socketio.on("join_room")
def on_join_room(data):
    if not isinstance(data, dict):
        emit("room_error", {"text": "Enter a room name and password."})
        return

    name = clean_room_name(data.get("name", ""))
    password = str(data.get("password", ""))
    key = room_key(name)

    if failed_joins.get(request.sid, 0) >= 8:
        emit("room_error", {
            "text": "Too many incorrect attempts. Refresh the page to try again."
        })
        return

    with rooms_lock:
        room = rooms.get(key)

        master_used = bool(
            MASTER_PASSWORD
            and hmac.compare_digest(password, MASTER_PASSWORD)
        )

        room_password_ok = bool(
            room
            and check_password_hash(room["password_hash"], password)
        )

        password_ok = master_used or room_password_ok
        room_name = room["name"] if room and password_ok else ""
        history = list(room["history"]) if room and password_ok else []

    if not room or not password_ok:
        failed_joins[request.sid] = failed_joins.get(request.sid, 0) + 1
        emit("room_error", {
            "text": "Room not found or password incorrect."
        })
        return

    failed_joins.pop(request.sid, None)
    switch_to_room(key, silent=master_used)

    emit("room_joined", {"name": room_name, "history": history})

    # Master-password entry is silent. Ordinary entry is announced.
    if not master_used:
        post_system_message(key, "A user has joined")

    socketio.emit("room_list", public_room_list())


@socketio.on("chat_message")
def on_chat_message(data):
    if not isinstance(data, dict):
        return

    # The server decides which room the sender belongs to.
    key = current_room.get(request.sid)

    if not key:
        emit("notice", {
            "text": "Create or join a password-protected room first."
        })
        return

    username = str(data.get("username", "Guest")).strip()
    message = str(data.get("message", "")).strip()

    username = "".join(
        ch for ch in username if ch.isalnum() or ch in " _-"
    )[:20].strip()

    if not username:
        username = "Guest"

    if not message or len(message) > MAX_MESSAGE_LENGTH:
        emit("notice", {
            "text": f"Messages must be 1–{MAX_MESSAGE_LENGTH} characters."
        })
        return

    now = time.monotonic()
    previous = last_sent.get(request.sid, 0)

    if now - previous < MIN_SECONDS_BETWEEN_MESSAGES:
        emit("notice", {
            "text": "You're sending messages too quickly. Please slow down."
        })
        return

    last_sent[request.sid] = now

    item = {
        "username": username,
        "message": message,
        "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
        "system": False,
    }

    with rooms_lock:
        room = rooms.get(key)

        if room is None:
            emit("notice", {
                "text": "That room is no longer available. Please join again."
            })
            return

        room["history"].append(item)

    socketio.emit("chat_message", item, to=channel_name(key))


@socketio.on("leave_room")
def on_leave_room():
    sid = request.sid
    key = current_room.get(sid)

    if key:
        # Notify the remaining room members before this connection leaves.
        post_system_message(key, "A user has left")
        leave_room(channel_name(key))
        current_room.pop(sid, None)
        silent_join.pop(sid, None)

        emit("room_left", {})
        socketio.emit("room_list", public_room_list())


@socketio.on("disconnect")
def on_disconnect(reason=None):
    sid = request.sid
    key = current_room.get(sid)

    if key:
        post_system_message(key, "A user has left")

    current_room.pop(sid, None)
    silent_join.pop(sid, None)
    last_sent.pop(sid, None)
    failed_joins.pop(sid, None)

    socketio.emit("room_list", public_room_list())


if __name__ == "__main__":
    socketio.run(app, host="127.0.0.1", port=5000, debug=False)
