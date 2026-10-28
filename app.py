from collections import deque
from datetime import datetime, timezone
import time

from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config["SECRET_KEY"] = "change-this-to-a-long-random-secret-before-deploying"
socketio = SocketIO(app, cors_allowed_origins=[], async_mode="threading")

# In-memory only: messages disappear when the server restarts.
recent_messages = deque(maxlen=100)
last_sent = {}
MAX_MESSAGE_LENGTH = 500
MIN_SECONDS_BETWEEN_MESSAGES = 0.8

@app.get("/")
def index():
    return render_template("index.html")

@socketio.on("connect")
def on_connect():
    emit("history", list(recent_messages))

@socketio.on("chat_message")
def on_chat_message(data):
    if not isinstance(data, dict):
        return

    username = str(data.get("username", "Guest")).strip()
    message = str(data.get("message", "")).strip()

    # Keep usernames simple and limit lengths.
    username = "".join(ch for ch in username if ch.isalnum() or ch in " _-")[:20].strip()
    if not username:
        username = "Guest"

    if not message or len(message) > MAX_MESSAGE_LENGTH:
        emit("notice", {"text": f"Messages must be 1–{MAX_MESSAGE_LENGTH} characters."})
        return

    now = time.monotonic()
    previous = last_sent.get(request.sid, 0)
    if now - previous < MIN_SECONDS_BETWEEN_MESSAGES:
        emit("notice", {"text": "You're sending messages too quickly. Please slow down."})
        return
    last_sent[request.sid] = now

    item = {
        "username": username,
        "message": message,
        "time": datetime.now(timezone.utc).strftime("%H:%M UTC"),
    }
    recent_messages.append(item)
    socketio.emit("chat_message", item)

@socketio.on("disconnect")
def on_disconnect():
    last_sent.pop(request.sid, None)

if __name__ == "__main__":
    # Local development server. Do not expose Flask's debug mode publicly.
    socketio.run(app, host="127.0.0.1", port=5000, debug=False)
