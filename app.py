import os
import re
import hmac
import time
from datetime import datetime
from threading import RLock

from flask import Flask, render_template
from flask_socketio import SocketIO, join_room as socket_join_room, leave_room as socket_leave_room
from werkzeug.security import generate_password_hash, check_password_hash


app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get(
    "SECRET_KEY", "local-development-only"
)

socketio = SocketIO(app, async_mode="threading")

MASTER_PASSWORD = os.environ.get("MASTER_PASSWORD", "")

MAX_ROOMS = 100
MAX_MESSAGES_PER_ROOM = 100
MAX_MESSAGE_LENGTH = 500
MAX_ROOM_NAME_LENGTH = 40
MIN_PASSWORD_LENGTH = 8
MAX_PASSWORD_LENGTH = 128
MESSAGE_COOLDOWN = 0.8

# All data is temporary and will reset when the server restarts.
rooms = {}
current_room = {}
silent_join = {}
last_sent = {}
failed_joins = {}

lock = RLock()


@app.route("/")
def index():
    return render_template("index.html")


def current_time():
    return datetime.now().strftime("%H:%M")


def room_list_data():
    """Return the public room list and current occupant counts."""
    with lock:
        result = []

        for name in sorted(rooms.keys(), key=str.lower):
            users = sum(
                1 for room_name in current_room.values()
                if room_name == name
            )

            result.append({
                "name": name,
                "users": users,
            })

        return result


def broadcast_room_list():
    socketio.emit("room_list", room_list_data())


def post_chat_message(room_name, username, message, system=False):
    """Store and broadcast a message to people currently in a room."""
    with lock:
        room = rooms.get(room_name)

        if room is None:
            return

        entry = {
            "username": username,
            "message": message,
            "time": current_time(),
            "system": system,
        }

        room["messages"].append(entry)

        if len(room["messages"]) > MAX_MESSAGES_PER_ROOM:
            room["messages"] = room["messages"][-MAX_MESSAGES_PER_ROOM:]

    socketio.emit("chat_message", entry, to=room_name)


def delete_room_if_empty(room_name):
    """Delete a room and its history if nobody remains in it."""
    with lock:
        if room_name not in rooms:
            return False

        occupants = sum(
            1 for name in current_room.values()
            if name == room_name
        )

        if occupants == 0:
            del rooms[room_name]
            print(f"Deleted empty room: {room_name}")
            return True

        return False


def remove_user_from_room(sid, announce=True):
    """Remove a socket from its room and delete the room if it is empty."""
    with lock:
        room_name = current_room.pop(sid, None)
        was_silent = silent_join.pop(sid, False)
        last_sent.pop(sid, None)
        failed_joins.pop(sid, None)

    if room_name is None:
        return

    socket_leave_room(room_name, sid=sid)

    # Notify remaining users unless this user joined silently with the
    # master password.
    if announce and not was_silent:
        post_chat_message(
            room_name,
            "System",
            "A user has left",
            system=True,
        )

    delete_room_if_empty(room_name)
    broadcast_room_list()


@socketio.on("connect")
def handle_connect():
    broadcast_room_list()


@socketio.on("create_room")
def handle_create_room(data):
    if not isinstance(data, dict):
        socketio.emit("room_error", {"text": "Invalid room details."}, to=request_sid())
        return

    sid = request_sid()
    name = str(data.get("name", "")).strip()
    password = str(data.get("password", ""))

    if not name or len(name) > MAX_ROOM_NAME_LENGTH:
        socketio.emit(
            "room_error",
            {"text": f"Room name must be between 1 and {MAX_ROOM_NAME_LENGTH} characters."},
            to=sid,
        )
        return

    if not re.fullmatch(r"[\w -]+", name, flags=re.UNICODE):
        socketio.emit(
            "room_error",
            {"text": "Room names can only contain letters, numbers, spaces, underscores, and hyphens."},
            to=sid,
        )
        return

    if len(password) < MIN_PASSWORD_LENGTH or len(password) > MAX_PASSWORD_LENGTH:
        socketio.emit(
            "room_error",
            {"text": f"Room password must be {MIN_PASSWORD_LENGTH}–{MAX_PASSWORD_LENGTH} characters long."},
            to=sid,
        )
        return

    with lock:
        if name in rooms:
            socketio.emit(
                "room_error",
                {"text": "A room with that name already exists."},
                to=sid,
            )
            return

        if len(rooms) >= MAX_ROOMS:
            socketio.emit(
                "room_error",
                {"text": "The server has reached its room limit."},
                to=sid,
            )
            return

        rooms[name] = {
            "password_hash": generate_password_hash(password),
            "messages": [],
        }

    # If the user was already in a room, leave that room first.
    remove_user_from_room(sid)

    with lock:
        # The new room may have been removed if the user was already
        # connected to it through another action, so verify it still exists.
        if name not in rooms:
            socketio.emit(
                "room_error",
                {"text": "That room no longer exists. Please create it again."},
                to=sid,
            )
            return

        current_room[sid] = name
        silent_join[sid] = False

    socket_join_room(name, sid=sid)
    post_chat_message(name, "System", "A user has joined", system=True)

    with lock:
        history = list(rooms[name]["messages"])

    socketio.emit(
        "room_joined",
        {"name": name, "history": history},
        to=sid,
    )
    broadcast_room_list()


@socketio.on("join_room")
def handle_join_room(data):
    sid = request_sid()

    if not isinstance(data, dict):
        socketio.emit("room_error", {"text": "Invalid room details."}, to=sid)
        return

    name = str(data.get("name", "")).strip()
    password = str(data.get("password", ""))

    with lock:
        room = rooms.get(name)

        if room is None:
            socketio.emit(
                "room_error",
                {"text": "That room does not exist anymore."},
                to=sid,
            )
            return

        password_matches = check_password_hash(
            room["password_hash"], password
        )

        master_matches = bool(MASTER_PASSWORD) and hmac.compare_digest(
            password.encode("utf-8"),
            MASTER_PASSWORD.encode("utf-8"),
        )

        if not password_matches and not master_matches:
            failed_joins[sid] = failed_joins.get(sid, 0) + 1
            socketio.emit(
                "room_error",
                {"text": "Incorrect password."},
                to=sid,
            )
            return

        history = list(room["messages"])

    # Leave the previous room, if there is one.
    remove_user_from_room(sid)

    with lock:
        # Recheck the room in case it was deleted during the transition.
        room = rooms.get(name)

        if room is None:
            socketio.emit(
                "room_error",
                {"text": "That room no longer exists. Please select another."},
                to=sid,
            )
            return

        current_room[sid] = name
        silent_join[sid] = master_matches and not password_matches
        history = list(room["messages"])

    socket_join_room(name, sid=sid)

    if not (master_matches and not password_matches):
        post_chat_message(name, "System", "A user has joined", system=True)

    socketio.emit(
        "room_joined",
        {"name": name, "history": history},
        to=sid,
    )
    broadcast_room_list()


@socketio.on("leave_room")
def handle_leave_room():
    sid = request_sid()
    remove_user_from_room(sid)
    socketio.emit("room_left", {}, to=sid)


@socketio.on("chat_message")
def handle_chat_message(data):
    sid = request_sid()

    if not isinstance(data, dict):
        return

    message = str(data.get("message", "")).strip()

    if not message or len(message) > MAX_MESSAGE_LENGTH:
        socketio.emit(
            "notice",
            {"text": f"Messages must be between 1 and {MAX_MESSAGE_LENGTH} characters."},
            to=sid,
        )
        return

    now = time.monotonic()
    previous = last_sent.get(sid, 0)

    if now - previous < MESSAGE_COOLDOWN:
        socketio.emit(
            "notice",
            {"text": "Please wait a moment before sending another message."},
            to=sid,
        )
        return

    with lock:
        room_name = current_room.get(sid)

        if room_name is None or room_name not in rooms:
            socketio.emit(
                "notice",
                {"text": "Join a room before sending a message."},
                to=sid,
            )
            return

        last_sent[sid] = now

    # The username is intentionally assigned by the server.
    post_chat_message(room_name, "User", message, system=False)


@socketio.on("disconnect")
def handle_disconnect(reason=None):
    sid = request_sid()
    remove_user_from_room(sid)


def request_sid():
    """Return the current Socket.IO connection ID."""
    from flask import request
    return request.sid


if __name__ == "__main__":
    socketio.run(
        app,
        host="127.0.0.1",
        port=5000,
        debug=False,
    )
