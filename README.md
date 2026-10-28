# Public Python Chat

## 1. Install Python packages (macOS)
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install Flask Flask-SocketIO simple-websocket
```

## 2. Run the chat locally
```bash
python app.py
```
Open http://127.0.0.1:5000 on your computer.

## 3. Publish with a temporary Cloudflare Tunnel
Install `cloudflared` if needed:
```bash
brew install cloudflared
```
Keep the Python server running in Terminal window 1. In Terminal window 2 run:
```bash
cloudflared tunnel --url http://127.0.0.1:5000
```
Cloudflare prints a temporary `https://....trycloudflare.com` address. Share that URL. The tunnel stops when you stop cloudflared, and the URL may change next time.

## Important
- Keep `debug=False`; never expose Flask's debug server.
- This demo keeps the last 100 messages in RAM only. Restarting the app clears them.
- Anyone with the public URL can access the chat. Do not post private details.
- This is a starter demo, not a hardened public service. It has basic message length/rate limits but no accounts, moderation tools, persistent storage, or robust abuse protection.
- Before a long-term public launch, add moderation/reporting, stronger rate limits, persistent storage, and a Cloudflare Access/WAF or other abuse controls as appropriate.
- Change `SECRET_KEY` in `app.py` to a long random value before deployment.
