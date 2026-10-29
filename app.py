
import os
import re
import hmac
import time
from datetime import datetime
from threading import RLock

from flask import Flask, render_template, request
from flask_socketio import (
    SocketIO,
    join_room as socket_join_room,
    leave_room as socket_leave_room,
)
from werkzeug.security import generate_password_hash, check_password_hash


app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get(
    "SECRET_KEY", "local-development-only"
)

socketio = SocketIO(app, async_mode="threading")

MASTER_PASSWORD = os.environ.get("MASTER_PASSWORD", "")
KILL_KEY = os.environ.get("KILL_KEY", "")

MAX_ROOMS = 100
MAX_MESSAGES_PER_ROOM = 100
MAX_MESSAGE_LENGTH = 500
MAX_ROOM_NAME_LENGTH = 40
MIN_PASSWORD_LENGTH = 8
MAX_PASSWORD_LENGTH = 128
MESSAGE_COOLDOWN = 0.8

# Temporary in-memory storage.
rooms = {}
current_room = {}
silent_join = {}
last_sent = {}
failed_joins = {}

lock = RLock()


@app.route("/")
def index():
    return render_template("index.html")


def request_sid():
    return request.sid


def current_time():
    return datetime.now().strftime("%H:%M")


def room_list_data():
    with lock:
        counts = {}

        for name in current_room.values():
            counts[name] = counts.get(name, 0) + 1

        return [
            {"name": name, "users": counts.get(name, 0)}
            for name in sorted(rooms, key=str.lower)
        ]


def broadcast_room_list():
    socketio.emit("room_list", room_list_data())


def post_chat_message(room_name, username, message, system=False):
    with lock:
        room = rooms.get(room_name)

        if room is None:
            return

        entry = {
            "username": username,
            "message": message,
            "time": current_time(),
            "system": system        }

        room["messages"].append(entry)

        if len(room["messages"]) > MAX_MESSAGES_PER_ROOM:
            room["messages"] = room["messages"][-MAX_MESSAGES_PER_ROOM:]

    socketio.emit("chat_message", entry, to=room_name)


def delete_room_if_empty(room_name):
    with lock:
        if room_name not in rooms:
            return False

        if any(name == room_name for name in current_room.values()):
            return False

        del rooms[room_name]
        print(f"Deleted empty room: {room_name}")
        return True


def remove_user_from_room(sid, announce=True):
    with lock:
        room_name = current_room.pop(sid, None)
        was_silent = silent_join.pop(sid, False)
        last_sent.pop(sid, None)
        failed_joins.pop(sid, None)

    if room_name is None:
        return

    socket_leave_room(room_name, sid=sid)

    if announce and not was_silent:
        post_chat_message(
            room_name, "System", "A user has left", system=True
        )

    delete_room_if_empty(room_name)
    broadcast_room_list()


def terminate_room(room_name, requester_sid):
    """Delete a room and disconnect all of its occupants from that room."""
    with lock:
        if room_name not in rooms:
            return False

        occupants = [
            sid for sid, name in current_room.items()
            if name == room_name
        ]

        # Delete the room, password hash, and message history.
        del rooms[room_name]

        # Remove every occupant from server-side room tracking.
        for sid in occupants:
            current_room.pop(sid, None)
            silent_join.pop(sid, None)
            last_sent.pop(sid, None)
            failed_joins.pop(sid, None)

    # Socket operations and notifications happen outside the lock.
    for sid in occupants:
        socket_leave_room(room_name, sid=sid)

        socketio.emit(
            "room_error",
            {"text": "This room has been terminated."},
            to=sid,
        )
        socketio.emit("room_left", {}, to=sid)

    if requester_sid not in occupants:
        socketio.emit(
            "room_error",
            {"text": "Room terminated successfully."},
            to=requester_sid,
        )

    broadcast_room_list()
    print(f"Room terminated: {room_name}")
    return True


@socketio.on("connect")
def handle_connect():
    broadcast_room_list()


@socketio.on("create_room")
def handle_create_room(data):
    sid = request_sid()

    if not isinstance(data, dict):
        socketio.emit(
            "room_error", {"text": "Invalid room details."}, to=sid
        )
        return

    name = str(data.get("name", "")).strip()
    password = str(data.get("password", ""))

    if not name or len(name) > MAX_ROOM_NAME_LENGTH:
        socketio.emit(
            "room_error",
            {
                "text": (
                    f"Room name must be between 1 and "
                    f"{MAX_ROOM_NAME_LENGTH} characters."
                )
            },
            to=sid,
        )
        return

    if not re.fullmatch(r"[\w -]+", name, flags=re.UNICODE):
        socketio.emit(
            "room_error",
            {
                "text": (
                    "Room names can only contain letters, numbers, "
                    "spaces, underscores, and hyphens."
                )
            },
            to=sid,
        )
        return

    if not MIN_PASSWORD_LENGTH <= len(password) <= MAX_PASSWORD_LENGTH:
        socketio.emit(
            "room_error",
            {
                "text": (
                    f"Room password must be {MIN_PASSWORD_LENGTH}–"
                    f"{MAX_PASSWORD_LENGTH} characters long."
                )
            },
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

    remove_user_from_room(sid)

    with lock:
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
        socketio.emit(
            "room_error", {"text": "Invalid room details."}, to=sid
        )
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

        # The kill key takes priority over ordinary room passwords.
        kill_match = bool(KILL_KEY) and hmac.compare_digest(
            password.encode("utf-8"),
            KILL_KEY.encode("utf-8"),
        )

        if not kill_match:
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

    # Handle termination outside the initial lock.
    if kill_match:
        if not terminate_room(name, sid):
            socketio.emit(
                "room_error",
                {"text": "That room no longer exists."},
                to=sid,
            )
        return

    # Already inside this room? Don't leave and accidentally delete it.
    with lock:
        already_in_room = current_room.get(sid) == name

    if already_in_room:
        socketio.emit(
            "room_joined",
            {"name": name, "history": history},
            to=sid,
        )
        return

    # Leave the previous room before joining the new one.
    remove_user_from_room(sid)

    with lock:
        room = rooms.get(name)

        if room is None:
            socketio.emit(
                "room_error",
                {
                    "text": (
                        "That room no longer exists. Please select another."
                    )
                },
                to=sid,
            )
            return

        current_room[sid] = name
        silent_join[sid] = master_matches and not password_matches
        history = list(room["messages"])

    socket_join_room(name, sid=sid)

    if not (master_matches and not password_matches):
        post_chat_message(
            name, "System", "A user has joined", system=True
        )

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
            {
                "text": (
                    f"Messages must be between 1 and "
                    f"{MAX_MESSAGE_LENGTH} characters."
                )
            },
            to=sid,
        )
        return

    now = time.monotonic()
    previous = last_sent.get(sid, 0)

    if now - previous < MESSAGE_COOLDOWN:
        socketio.emit(
            "notice",
            {
                "text": (
                    "Please wait a moment before sending another message."
                )
            },
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

    post_chat_message(ro
import os
import re
import hmac
import time
from datetime import datetime
from threading import RLock

from flask import Flask, render_template, request
from flask_socketio import (
    SocketIO,
    join_room as socket_join_room,
    leave_room as socket_leave_room,
)
from werkzeug.security import generate_password_hash, check_password_hash
from user_agents import parse


app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get(
    "SECRET_KEY", "local-development-only"
)

socketio = SocketIO(app, async_mode="threading")

MASTER_PASSWORD = os.environ.get("MASTER_PASSWORD", "")
KILL_KEY = os.environ.get("KILL_KEY", "")

MAX_ROOMS = 100
MAX_MESSAGES_PER_ROOM = 100
MAX_MESSAGE_LENGTH = 500
MAX_ROOM_NAME_LENGTH = 40
MIN_PASSWORD_LENGTH = 8
MAX_PASSWORD_LENGTH = 128
MESSAGE_COOLDOWN = 0.8

# Temporary in-memory storage.
rooms = {}
current_room = {}
silent_join = {}
last_sent = {}
failed_joins = {}

lock = RLock()


@app.route("/")
def index():
    return render_template("index.html")


def request_sid():
    return request.sid


def current_time():
    return datetime.now().strftime("%H:%M")


def log_visitor():
    """Print visitor connection details to the application logs."""
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    ip_address = (
        forwarded_for.split(",")[0].strip()
        if forwarded_for
        else request.remote_addr or "Unknown"
    )

    user_agent_string = request.headers.get("User-Agent", "")
    user_agent = parse(user_agent_string)

    browser = (
        f"{user_agent.browser.family} "
        f"{user_agent.browser.version_string}"
    ).strip()

    operating_system = (
        f"{user_agent.os.family} "
        f"{user_agent.os.version_string}"
    ).strip()

    if user_agent.is_mobile:
        device = "Mobile"
    elif user_agent.is_tablet:
        device = "Tablet"
    elif user_agent.is_pc:
        device = "Desktop"
    else:
        device = user_agent.device.family

    print("\n" + "=" * 55, flush=True)
    print("NEW WEBSITE VISITOR", flush=True)
    print(f"TIME: {datetime.now().isoformat(timespec='seconds')}", flush=True)
    print(f"IP: {ip_address}", flush=True)
    print(f"BROWSER: {browser or 'Unknown'}", flush=True)
    print(f"OS: {operating_system or 'Unknown'}", flush=True)
    print(f"DEVICE: {device}", flush=True)
    print(f"PAGE: {request.path}", flush=True)
    print("=" * 55 + "\n", flush=True)


def room_list_data():
    with lock:
        counts = {}

        for name in current_room.values():
            counts[name] = counts.get(name, 0) + 1

        return [
            {"name": name, "users": counts.get(name, 0)}
            for name in sorted(rooms, key=str.lower)
        ]


def broadcast_room_list():
    socketio.emit("room_list", room_list_data())


def post_chat_message(room_name, username, message, system=False):
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
    with lock:
        if room_name not in rooms:
            return False

        if any(name == room_name for name in current_room.values()):
            return False

        del rooms[room_name]
        print(f"Deleted empty room: {room_name}")
        return True


def remove_user_from_room(sid, announce=True):
    with lock:
        room_name = current_room.pop(sid, None)
        was_silent = silent_join.pop(sid, False)
        last_sent.pop(sid, None)
        failed_joins.pop(sid, None)

    if room_name is None:
        return

    socket_leave_room(room_name, sid=sid)

    if announce and not was_silent:
        post_chat_message(
            room_name, "System", "A user has left", system=True
        )

    delete_room_if_empty(room_name)
    broadcast_room_list()


def terminate_room(room_name, requester_sid):
    """Delete a room and disconnect all of its occupants from that room."""
    with lock:
        if room_name not in rooms:
            return False

        occupants = [
            sid for sid, name in current_room.items()
            if name == room_name
        ]

        # Delete the room, password hash, and message history.
        del rooms[room_name]

        # Remove every occupant from server-side room tracking.
        for sid in occupants:
            current_room.pop(sid, None)
            silent_join.pop(sid, None)
            last_sent.pop(sid, None)
            failed_joins.pop(sid, None)

    # Socket operations and notifications happen outside the lock.
    for sid in occupants:
        socket_leave_room(room_name, sid=sid)

        socketio.emit(
            "room_error",
            {"text": "This room has been terminated."},
            to=sid,
        )
        socketio.emit("room_left", {}, to=sid)

    if requester_sid not in occupants:
        socketio.emit(
            "room_error",
            {"text": "Room terminated successfully."},
            to=requester_sid,
        )

    broadcast_room_list()
    print(f"Room terminated: {room_name}")
    return True


@socketio.on("connect")
def handle_connect():
    log_visitor()
    broadcast_room_list()


@socketio.on("create_room")
def handle_create_room(data):
    sid = request_sid()

    if not isinstance(data, dict):
        socketio.emit(
            "room_error", {"text": "Invalid room details."}, to=sid
        )
        return

    name = str(data.get("name", "")).strip()
    password = str(data.get("password", ""))

    if not name or len(name) > MAX_ROOM_NAME_LENGTH:
        socketio.emit(
            "room_error",
            {
                "text": (
                    f"Room name must be between 1 and "
                    f"{MAX_ROOM_NAME_LENGTH} characters."
                )
            },
            to=sid,
        )
        return

    if not re.fullmatch(r"[\w -]+", name, flags=re.UNICODE):
        socketio.emit(
            "room_error",
            {
                "text": (
                    "Room names can only contain letters, numbers, "
                    "spaces, underscores, and hyphens."
                )
            },
            to=sid,
        )
        return

    if not MIN_PASSWORD_LENGTH <= len(password) <= MAX_PASSWORD_LENGTH:
        socketio.emit(
            "room_error",
            {
                "text": (
                    f"Room password must be {MIN_PASSWORD_LENGTH}–"
                    f"{MAX_PASSWORD_LENGTH} characters long."
                )
            },
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

    remove_user_from_room(sid)

    with lock:
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
        socketio.emit(
            "room_error", {"text": "Invalid room details."}, to=sid
        )
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

        # The kill key takes priority over ordinary room passwords.
        kill_match = bool(KILL_KEY) and hmac.compare_digest(
            password.encode("utf-8"),
            KILL_KEY.encode("utf-8"),
        )

        if not kill_match:
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

    # Handle termination outside the initial lock.
    if kill_match:
        if not terminate_room(name, sid):
            socketio.emit(
                "room_error",
                {"text": "That room no longer exists."},
                to=sid,
            )
        return

    # Already inside this room? Don't leave and accidentally delete it.
    with lock:
        already_in_room = current_room.get(sid) == name

    if already_in_room:
        socketio.emit(
            "room_joined",
            {"name": name, "history": history},
            to=sid,
        )
        return

    # Leave the previous room before joining the new one.
    remove_user_from_room(sid)

    with lock:
        room = rooms.get(name)

        if room is None:
            socketio.emit(
                "room_error",
                {
                    "text": (
                        "That room no longer exists. Please select another."
                    )
                },
                to=sid,
            )
            return

        current_room[sid] = name
        silent_join[sid] = master_matches and not password_matches
        history = list(room["messages"])

    socket_join_room(name, sid=sid)

    if not (master_matches and not password_matches):
        post_chat_message(
            name, "System", "A user has joined", system=True
        )

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
            {
                "text": (
                    f"Messages must be between 1 and "
                    f"{MAX_MESSAGE_LENGTH} characters."
                )
            },
            to=sid,
        )
        return

    now = time.monotonic()
    previous = last_sent.get(sid, 0)

    if now - previous < MESSAGE_COOLDOWN:
        socketio.emit(
            "notice",
            {
                "text": (
                    "Please wait a moment before sending another message."
                )
            },
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

    post_chat_message(room_name, "User", message, system=False)


@socketio.on("disconnect")
def handle_disconnect(reason=None):
    remove_user_from_room(request_sid())


if __name__ == "__main__":
    socketio.run(
        app,
        host="127.0.0.1",
        port=5000,
        debug=False,
    )

