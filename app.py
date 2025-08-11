from flask import Flask, request, abort, redirect, url_for, Response, render_template_string
import sqlite3
import threading
import time
import secrets
import requests
from urllib.parse import urljoin
import os

DB_PATH = "channels_multi.db"
UPDATE_INTERVAL = 3
TOKEN_EXPIRY_DEFAULT = 86400  # 24 hours

app = Flask(__name__)

latest_playlists = {}
latest_segments = {}
updater_threads = {}

# --- DB helper ---
def db_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = db_conn()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            version TEXT NOT NULL,
            source_url TEXT NOT NULL,
            UNIQUE(name, version)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            token TEXT PRIMARY KEY,
            channel_name TEXT NOT NULL,
            channel_version TEXT NOT NULL,
            expires REAL NOT NULL,
            play_url TEXT UNIQUE NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

# --- Channel CRUD ---
def add_channel(name, version, source_url):
    conn = db_conn()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO channels (name, version, source_url) VALUES (?, ?, ?)", (name, version, source_url))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def delete_channel(name, version):
    conn = db_conn()
    c = conn.cursor()
    c.execute("DELETE FROM channels WHERE name=? AND version=?", (name, version))
    conn.commit()
    conn.close()

def list_channels():
    conn = db_conn()
    c = conn.cursor()
    c.execute("SELECT name, version, source_url FROM channels ORDER BY name, version")
    rows = c.fetchall()
    conn.close()
    return [{"name": r[0], "version": r[1], "source_url": r[2]} for r in rows]

def get_source_url(name, version):
    conn = db_conn()
    c = conn.cursor()
    c.execute("SELECT source_url FROM channels WHERE name=? AND version=?", (name, version))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

# --- Token CRUD ---
def generate_token(channel_name, channel_version, expiry_seconds=TOKEN_EXPIRY_DEFAULT):
    token = secrets.token_urlsafe(16)
    expires = time.time() + expiry_seconds
    play_url = f"/{channel_name}/{channel_version}/index.m3u8?token={token}"
    conn = db_conn()
    c = conn.cursor()
    c.execute("INSERT INTO tokens (token, channel_name, channel_version, expires, play_url) VALUES (?, ?, ?, ?, ?)",
              (token, channel_name, channel_version, expires, play_url))
    conn.commit()
    conn.close()
    return token, expires, play_url

def validate_token(token):
    conn = db_conn()
    c = conn.cursor()
    c.execute("SELECT expires, channel_name, channel_version FROM tokens WHERE token=?", (token,))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
    expires, channel_name, channel_version = row
    if time.time() > expires:
        c.execute("DELETE FROM tokens WHERE token=?", (token,))
        conn.commit()
        conn.close()
        return None
    conn.close()
    return channel_name, channel_version

def cleanup_expired_tokens():
    conn = db_conn()
    c = conn.cursor()
    c.execute("DELETE FROM tokens WHERE expires < ?", (time.time(),))
    conn.commit()
    conn.close()

# --- Playlist updater worker ---
def update_worker(name, version, source_url, stop_event):
    global latest_playlists, latest_segments
    base_url = source_url.rsplit("/",1)[0] + "/"
    while not stop_event.is_set():
        try:
            r = requests.get(source_url, timeout=6)
            r.raise_for_status()
            lines = r.text.splitlines()
            new_segs = []
            new_playlist_lines = []

            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if line.endswith(".ts") or line.endswith(".aac") or ".ts?" in line:
                    full = urljoin(base_url, line)
                    idx = len(new_segs)
                    new_segs.append(full)
                    new_playlist_lines.append(f"/seg/{name}/{version}/{idx}")
                else:
                    new_playlist_lines.append(line)
            
            latest_segments[(name, version)] = new_segs
            latest_playlists[(name, version)] = "\n".join(new_playlist_lines)

        except Exception as e:
            print(f"[{name} v{version}] update error:", e)
        cleanup_expired_tokens()
        stop_event.wait(UPDATE_INTERVAL)

def start_updater(name, version, source_url):
    key = (name, version)
    if key in updater_threads:
        return
    stop_event = threading.Event()
    t = threading.Thread(target=update_worker, args=(name, version, source_url, stop_event), daemon=True)
    updater_threads[key] = (t, stop_event)
    t.start()

def stop_updater(name, version):
    key = (name, version)
    tup = updater_threads.pop(key, None)
    if tup:
        _, stop_event = tup
        stop_event.set()

# --- Flask routes ---

@app.route("/admin", methods=["GET"])
def admin():
    channels = list_channels()
    html = """
    <h1>HLS Relay Admin Panel</h1>
    <h2>Channels</h2>
    <table border="1" cellpadding="5" cellspacing="0">
      <tr><th>Name</th><th>Version</th><th>Source URL</th><th>Actions</th><th>Preview Link</th></tr>
      {% for ch in channels %}
      <tr>
        <td>{{ ch.name }}</td>
        <td>{{ ch.version }}</td>
        <td style="max-width:400px;word-break:break-all;">{{ ch.source_url }}</td>
        <td>
          <form method="POST" action="/admin/delete" onsubmit="return confirm('Delete channel?');">
            <input type="hidden" name="name" value="{{ ch.name }}">
            <input type="hidden" name="version" value="{{ ch.version }}">
            <button type="submit">Delete</button>
          </form>
        </td>
        <td>
          <form method="POST" action="/admin/preview">
            <input type="hidden" name="name" value="{{ ch.name }}">
            <input type="hidden" name="version" value="{{ ch.version }}">
            <button type="submit">Generate Preview Token</button>
          </form>
        </td>
      </tr>
      {% endfor %}
    </table>

    <h2>Add New Channel</h2>
    <form method="POST" action="/admin/add">
      Name: <input name="name" required pattern="[A-Za-z0-9_]+"><br>
      Version: <input name="version" value="v1" required pattern="v[0-9]+"><br>
      Source URL: <input name="source_url" size="80" required><br><br>
      <button type="submit">Add Channel</button>
    </form>
    """
    return render_template_string(html, channels=channels)

@app.route("/admin/add", methods=["POST"])
def admin_add():
    name = request.form.get("name").strip()
    version = request.form.get("version").strip()
    source_url = request.form.get("source_url").strip()
    if not name or not version or not source_url:
        return "Missing data", 400
    ok = add_channel(name, version, source_url)
    if not ok:
        return "Channel already exists", 400
    start_updater(name, version, source_url)
    return redirect(url_for("admin"))

@app.route("/admin/delete", methods=["POST"])
def admin_delete():
    name = request.form.get("name")
    version = request.form.get("version")
    if not name or not version:
        return "Missing data", 400
    stop_updater(name, version)
    latest_playlists.pop((name, version), None)
    latest_segments.pop((name, version), None)
    delete_channel(name, version)
    return redirect(url_for("admin"))

@app.route("/admin/preview", methods=["POST"])
def admin_preview():
    name = request.form.get("name")
    version = request.form.get("version")
    if not name or not version:
        return "Missing data", 400
    src = get_source_url(name, version)
    if not src:
        return "Channel not found", 404
    token, expires, play_url = generate_token(name, version)
    base_url = request.host_url.rstrip("/")
    full_url = f"{base_url}{play_url}"
    return f"""
    <h3>Preview Link</h3>
    <p><b>Play URL:</b> <a href="{full_url}" target="_blank">{full_url}</a></p>
    <p><a href="/admin">Back</a></p>
    """

@app.route("/<name>/<version>/index.m3u8")
def play_index(name, version):
    token = request.args.get("token", "")
    if not token:
        return abort(403, "Missing token")
    valid = validate_token(token)
    if not valid:
        return abort(403, "Invalid or expired token")
    valid_name, valid_version = valid
    if (valid_name != name) or (valid_version != version):
        return abort(403, "Token mismatch")
    playlist = latest_playlists.get((name, version))
    if not playlist:
        return abort(503, "Playlist not ready")
    return Response(playlist, mimetype="application/vnd.apple.mpegurl")

@app.route("/seg/<name>/<version>/<int:seg_id>")
def segment(name, version, seg_id):
    segs = latest_segments.get((name, version), [])
    if seg_id < 0 or seg_id >= len(segs):
        return abort(404, "Segment not found")
    seg_url = segs[seg_id]
    try:
        r = requests.get(seg_url, stream=True, timeout=8)
        r.raise_for_status()
        return Response(r.iter_content(chunk_size=1024), content_type="video/MP2T")
    except Exception as e:
        print(f"[{name} v{version}] segment fetch error:", e)
        return abort(502, "Upstream fetch failed")

@app.route("/")
def home():
    return redirect(url_for("admin"))

# --- Railway entry point ---
init_db()
for ch in list_channels():
    latest_playlists[(ch["name"], ch["version"])] = ""
    latest_segments[(ch["name"], ch["version"])] = []
    start_updater(ch["name"], ch["version"], ch["source_url"])
