import os
import re
import time
import html
import uuid
import socket
import logging
import shutil
import ipaddress
import telebot
import yt_dlp
import sqlite3
import threading
import queue
import glob
from urllib.parse import urlparse
from telebot import types
from telebot.apihelper import ApiTelegramException

TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "123456789"))

TELEGRAM_HARD_CAP_MB = 50
MAX_UPLOAD_MB = 48
MAX_DOWNLOAD_MB = TELEGRAM_HARD_CAP_MB
MAX_CONCURRENT_DOWNLOADS = 3
LINK_TTL_SECONDS = 3600
BROADCAST_PROGRESS_EVERY = 25
MAX_JOBS_PER_USER = 2
MAX_QUEUE_SIZE = 200

ALLOWED_DOMAINS = {
    "instagram.com",
    "facebook.com",
    "fb.watch",
    "twitter.com",
    "x.com",
    "tiktok.com",
    "youtube.com",
    "youtu.be",
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "bot_users.db")

if not TOKEN:
    raise SystemExit("❌ TOKEN set nahi hai. BOT_TOKEN environment variable set kar.")

if shutil.which("ffmpeg") is None:
    print("⚠️ Warning: ffmpeg nahi mila PATH me!")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("aio_bot")

bot = telebot.TeleBot(TOKEN, parse_mode="HTML")

user_links = {}
user_links_lock = threading.Lock()
download_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
active_jobs_per_chat = {}
active_jobs_lock = threading.Lock()
queue_order = []
queue_order_lock = threading.Lock()

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS users (chat_id INTEGER PRIMARY KEY)")
        conn.commit()

def add_user(chat_id):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            c.execute("INSERT OR IGNORE INTO users (chat_id) VALUES (?)", (chat_id,))
            conn.commit()
    except Exception as e:
        log.warning(f"add_user failed for {chat_id}: {e}")

def get_all_users():
    with sqlite3.connect(DB_FILE) as conn:
        c = conn.cursor()
        c.execute("SELECT chat_id FROM users")
        return c.fetchall()

def remove_user(chat_id):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            c = conn.cursor()
            c.execute("DELETE FROM users WHERE chat_id = ?", (chat_id,))
            conn.commit()
    except Exception as e:
        log.warning(f"remove_user failed for {chat_id}: {e}")

init_db()

def h(text: str) -> str:
    if not text:
        return text
    return html.escape(text)

def is_url_safe(url: str) -> tuple[bool, str]:
    try:
        parsed = urlparse(url)
    except Exception:
        return False, "Invalid URL."
    if parsed.scheme not in ("http", "https"):
        return False, "Sirf http/https links allowed hain."
    hostname = (parsed.hostname or "").lower()
    if not hostname:
        return False, "URL me valid hostname nahi hai."
    allowed = any(hostname == d or hostname.endswith("." + d) for d in ALLOWED_DOMAINS)
    if not allowed:
        return False, "Ye site supported nahi hai."
    try:
        infos = socket.getaddrinfo(hostname, None)
        for info in infos:
            ip = info[4][0]
            ip_obj = ipaddress.ip_address(ip)
            if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local or ip_obj.is_reserved:
                return False, "Ye link allowed nahi hai."
    except socket.gaierror:
        return False, "Host resolve nahi ho raha."
    except Exception:
        return False, "URL verify nahi ho paaya."
    return True, ""

def auto_cleaner():
    while True:
        time.sleep(3600)
        now = time.time()
        for file in glob.glob(os.path.join(BASE_DIR, "aio_*")):
            try:
                if os.path.isfile(file) and os.stat(file).st_mtime < now - 1800:
                    os.remove(file)
            except Exception:
                pass

def user_links_cleaner():
    while True:
        time.sleep(600)
        now = time.time()
        with user_links_lock:
            stale = [key for key, (_, ts) in user_links.items() if now - ts > LINK_TTL_SECONDS]
            for key in stale:
                user_links.pop(key, None)

threading.Thread(target=auto_cleaner, daemon=True).start()
threading.Thread(target=user_links_cleaner, daemon=True).start()

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    add_user(message.chat.id)
    bot.reply_to(
        message,
        "👋 <b>AIO Downloader Ready!</b>\n\nKoi bhi Insta, FB, Twitter/X, TikTok ya YouTube ka link bhej aur jadoo dekh! 🚀\n\nCommands:\n/status — apni queue position dekh\n/broadcast — (admin only)"
    )

@bot.message_handler(commands=['status'])
def status_command(message):
    chat_id = message.chat.id
    with active_jobs_lock:
        active = active_jobs_per_chat.get(chat_id, 0)
    with queue_order_lock:
        my_positions = [i + 1 for i, (c, _job_id) in enumerate(queue_order) if c == chat_id]
    if active == 0:
        bot.reply_to(message, "✅ Tera koi active ya queued job nahi hai abhi.")
        return
    with queue_order_lock:
        total_in_queue = len(queue_order)
    if my_positions:
        pos_text = ", ".join(str(p) for p in my_positions)
        bot.reply_to(message, f"⏳ Tere {len(my_positions)} job(s) queue me hain (position: {pos_text} out of {total_in_queue}).")
    else:
        bot.reply_to(message, "🔄 Tera job abhi process ho raha hai (downloading/uploading).")

@bot.message_handler(commands=['broadcast'])
def broadcast_message(message):
    if message.chat.id != ADMIN_ID:
        return bot.reply_to(message, "⛔ Tu admin nahi hai bhai!")
    parts = message.text.split(maxsplit=1)
    text = parts[1].strip() if len(parts) > 1 else ""
    if not text:
        return bot.reply_to(message, "⚠️ Message likh bhai! Aise: <code>/broadcast Hello sabko</code>")
    users = get_all_users()
    progress_msg = bot.reply_to(message, f"📢 Broadcasting to {len(users)} users... 0 done")
    threading.Thread(target=_run_broadcast, args=(text, users, progress_msg), daemon=True).start()

def _run_broadcast(text, users, progress_msg):
    success = 0
    failed = 0
    safe_text = h(text)
    for i, (chat_id,) in enumerate(users, start=1):
        try:
            bot.send_message(chat_id, f"📢 <b>Admin Update:</b>\n\n{safe_text}")
            success += 1
            time.sleep(0.1)
        except ApiTelegramException as e:
            if e.error_code == 429:
                retry_after = e.result_json.get("parameters", {}).get("retry_after", 5) if hasattr(e, 'result_json') else 5
                time.sleep(retry_after + 1)
                try:
                    bot.send_message(chat_id, f"📢 <b>Admin Update:</b>\n\n{safe_text}")
                    success += 1
                except Exception:
                    failed += 1
            elif e.error_code == 403:
                remove_user(chat_id)
                failed += 1
            else:
                failed += 1
        except Exception:
            failed += 1
        if i % BROADCAST_PROGRESS_EVERY == 0:
            try:
                bot.edit_message_text(f"📢 Broadcasting... {i}/{len(users)} processed ({success} sent, {failed} failed)", chat_id=progress_msg.chat.id, message_id=progress_msg.message_id)
            except Exception:
                pass
    try:
        bot.edit_message_text(f"✅ Broadcast finished! Sent to {success} users. Failed: {failed}.", chat_id=progress_msg.chat.id, message_id=progress_msg.message_id)
    except Exception:
        pass

@bot.message_handler(content_types=['text'], func=lambda message: True)
def handle_link(message):
    if message.text.startswith('/'):
        return
    add_user(message.chat.id)
    url = message.text.strip()
    if not url.startswith(('http://', 'https://')):
        bot.reply_to(message, "⚠️ Bhai, please ek valid link bhej jisme 'http' ya 'https' ho.")
        return
    safe, reason = is_url_safe(url)
    if not safe:
        bot.reply_to(message, f"⚠️ {h(reason)}")
        return
    with user_links_lock:
        user_links[(message.chat.id, message.message_id)] = (url, time.time())
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("🎥 Video", callback_data=f"vid_{message.message_id}"),
        types.InlineKeyboardButton("🎵 MP3", callback_data=f"aud_{message.message_id}")
    )
    bot.reply_to(message, "✅ <b>Link mil gaya!</b> Kya download karna hai?", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    chat_id = call.message.chat.id
    try:
        mode_prefix, msg_id_str = call.data.split('_', maxsplit=1)
        msg_id = int(msg_id_str)
        mode = 'audio' if mode_prefix == 'aud' else 'video'
    except Exception:
        return bot.answer_callback_query(call.id, "❌ Error!")
    with user_links_lock:
        entry = user_links.pop((chat_id, msg_id), None)
    url = entry[0] if entry else None
    try:
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
    except Exception:
        pass
    if not url:
        return bot.answer_callback_query(call.id, "⚠️ Ye link purana ho gaya hai ya already process ho chuka hai. Wapas naya link bhej!", show_alert=True)
    with active_jobs_lock:
        current = active_jobs_per_chat.get(chat_id, 0)
        if current >= MAX_JOBS_PER_USER:
            bot.answer_callback_query(call.id, f"⚠️ Tere already {current} jobs chal/queue me hain. Pehle unka wait kar!", show_alert=True)
            return
        active_jobs_per_chat[chat_id] = current + 1
    bot.answer_callback_query(call.id)
    status_msg = bot.send_message(chat_id, "⏳ <b>Queued... tera number aane wala hai!</b>")
    job_id = uuid.uuid4().hex
    try:
        with queue_order_lock:
            queue_order.append((chat_id, job_id))
        download_queue.put_nowait((chat_id, msg_id, mode, url, status_msg, job_id))
    except queue.Full:
        with queue_order_lock:
            queue_order[:] = [item for item in queue_order if item[1] != job_id]
        with active_jobs_lock:
            active_jobs_per_chat[chat_id] = max(0, active_jobs_per_chat.get(chat_id, 1) - 1)
        try:
            bot.edit_message_text("⚠️ Bot abhi bahot busy hai (queue full). Thodi der baad try kar.", chat_id=chat_id, message_id=status_msg.message_id)
        except Exception:
            pass

def download_worker():
    while True:
        chat_id, msg_id, mode, url, status_msg, job_id = download_queue.get()
        with queue_order_lock:
            queue_order[:] = [item for item in queue_order if item[1] != job_id]
        try:
            process_download(chat_id, msg_id, mode, url, status_msg, job_id)
        except Exception:
            pass
        finally:
            download_queue.task_done()
            with active_jobs_lock:
                remaining = active_jobs_per_chat.get(chat_id, 1) - 1
                if remaining <= 0:
                    active_jobs_per_chat.pop(chat_id, None)
                else:
                    active_jobs_per_chat[chat_id] = remaining

for _ in range(MAX_CONCURRENT_DOWNLOADS):
    threading.Thread(target=download_worker, daemon=True).start()

def process_download(chat_id, msg_id, mode, url, status_msg, job_id):
    try:
        bot.edit_message_text("⏳ <b>Processing... Please wait!</b>", chat_id=chat_id, message_id=status_msg.message_id)
    except Exception:
        pass
    last_edit_time = [0]
    file_prefix = os.path.join(BASE_DIR, f"aio_{job_id}")
    def telegram_progress_hook(d):
        if d['status'] == 'downloading':
            current_time = time.time()
            if current_time - last_edit_time[0] >= 5:
                downloaded = d.get('downloaded_bytes', 0)
                total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
                if total > 0:
                    percent = int((downloaded / total) * 100)
                    text = f"⏳ <b>Downloading...</b> {percent}%"
                else:
                    text = "⏳ <b>Downloading chunks...</b>"
                try:
                    bot.edit_message_text(text, chat_id=chat_id, message_id=status_msg.message_id)
                    last_edit_time[0] = current_time
                except Exception:
                    pass
        elif d['status'] == 'finished':
            try:
                bot.edit_message_text("✅ <b>Download complete! Uploading to Telegram...</b>", chat_id=chat_id, message_id=status_msg.message_id)
            except Exception:
                pass
    if mode == 'audio':
        format_str = f'bestaudio[filesize<{MAX_DOWNLOAD_MB}M]/best[filesize<{MAX_DOWNLOAD_MB}M]/bestaudio/best'
        ydl_opts = {
            'format': format_str,
            'outtmpl': f'{file_prefix}.%(ext)s',
            'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}],
            'quiet': True, 'no_warnings': True, 'socket_timeout': 60, 'retries': 5,
            'noprogress': True, 'color': 'no_color',
            'progress_hooks': [telegram_progress_hook],
            'noplaylist': True,
            'max_filesize': MAX_DOWNLOAD_MB * 1024 * 1024,
        }
    else:
        format_str = f'bestvideo[filesize<{MAX_DOWNLOAD_MB}M][ext=mp4]+bestaudio[filesize<{MAX_DOWNLOAD_MB}M][ext=m4a]/best[filesize<{MAX_DOWNLOAD_MB}M][ext=mp4]/best[filesize<{MAX_DOWNLOAD_MB}M]/best'
        ydl_opts = {
            'format': format_str,
            'outtmpl': f'{file_prefix}.%(ext)s',
            'merge_output_format': 'mp4',
            'quiet': True, 'no_warnings': True, 'socket_timeout': 60, 'retries': 5,
            'noprogress': True, 'color': 'no_color',
            'progress_hooks': [telegram_progress_hook],
            'noplaylist': True,
            'max_filesize': MAX_DOWNLOAD_MB * 1024 * 1024,
        }
    filename = None
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if info.get('requested_downloads'):
                filename = info['requested_downloads'][0].get('filepath') or ydl.prepare_filename(info)
            else:
                filename = ydl.prepare_filename(info)
            if mode == 'audio':
                base, _ = os.path.splitext(filename)
                mp3_name = base + '.mp3'
                if os.path.exists(mp3_name):
                    filename = mp3_name
        if not filename or not os.path.exists(filename):
            raise FileNotFoundError("Error")
        file_size = os.path.getsize(filename) / (1024 * 1024)
        if file_size > TELEGRAM_HARD_CAP_MB:
            bot.edit_message_text(f"⚠️ <b>File bahot badi hai</b> ({file_size:.1f} MB)! Max {TELEGRAM_HARD_CAP_MB}MB allowed.", chat_id=chat_id, message_id=status_msg.message_id)
        else:
            with open(filename, 'rb') as f:
                if mode == 'audio':
                    if file_size > MAX_UPLOAD_MB:
                        bot.send_document(chat_id, f, caption="🔥 Badi audio file!")
                    else:
                        bot.send_audio(chat_id, f, caption="🔥 MP3 by AIO Bot!")
                else:
                    if file_size > MAX_UPLOAD_MB:
                        bot.send_document(chat_id, f, caption="🔥 Badi file hone ki wajah se document bheja hai!")
                    else:
                        bot.send_video(chat_id, f, caption="🔥 Downloaded by AIO Bot!")
            try:
                bot.delete_message(chat_id, status_msg.message_id)
            except Exception:
                pass
    except Exception as e:
        raw_msg = str(e).replace('\033', '').replace('\x1b', '')
        error_msg = h(raw_msg[:300])
        try:
            bot.edit_message_text(f"❌ <b>Mission Failed!</b>\n<code>{error_msg}</code>", chat_id=chat_id, message_id=status_msg.message_id)
        except Exception:
            pass
    finally:
        for leftover in glob.glob(f"{file_prefix}.*"):
            try:
                os.remove(leftover)
            except Exception:
                pass

if __name__ == '__main__':
    log.info("🚀 God Mode Bot is running...")
    bot.infinity_polling(skip_pending=True)
