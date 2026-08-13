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

# ==========================================
# CONFIG
# ==========================================
# 🚨 TOKEN ab environment variable se aayega (safer, GitHub par accidentally
# leak nahi hoga). Terminal me chalane se pehle set kar:
#   export BOT_TOKEN="123456:ABC-your-token"
TOKEN = os.getenv("BOT_TOKEN", "8811160391:AAHt2BTdSBZNiGxMgBBpAPZgBuXyp2w61PA")

# 👑 APNA TELEGRAM CHAT ID (Broadcast use karne ke liye).
ADMIN_ID = int(os.getenv("ADMIN_ID", "123456789"))

TELEGRAM_HARD_CAP_MB = 50        # Telegram ka absolute upload ceiling
MAX_UPLOAD_MB = 48               # is se neeche -> video/audio ke roop me bhejo
MAX_DOWNLOAD_MB = TELEGRAM_HARD_CAP_MB
MAX_CONCURRENT_DOWNLOADS = 3     # ek saath max itne downloads chalenge (workers)
LINK_TTL_SECONDS = 3600          # 1 ghante se purane un-clicked links clean honge
BROADCAST_PROGRESS_EVERY = 25

# BUG (unbounded queue) FIX: per-user concurrent job limit + global queue cap,
# taaki ek user saari queue spam na kar sake aur baaki users starve na hon.
MAX_JOBS_PER_USER = 2
MAX_QUEUE_SIZE = 200

# BUG (no domain whitelist / SSRF risk) FIX: sirf in domains (aur unke
# subdomains) ke links accept honge. Zaroorat pade to yahan aur domains
# add kar sakte ho.
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

# BASE_DIR: script jahan se bhi run ho, isi folder ke andar downloads aur
# DB rakhe jaayenge (BUG: relative DB_FILE path FIX).
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "bot_users.db")

if TOKEN == "YOUR_NEW_TOKEN_HERE":
    raise SystemExit("❌ TOKEN set nahi hai. BOT_TOKEN environment variable set kar ya TOKEN variable me daal.")
if ADMIN_ID == 123456789:
    print("⚠️ Warning: ADMIN_ID abhi bhi default placeholder hai. /broadcast kaam nahi karega jab tak apna Chat ID nahi daalte.")

# ffmpeg zaroori hai audio-extraction (mp3 conversion) aur video merge ke
# liye. Agar missing hai to har audio/merge download silently generic error
# ke saath fail hota tha - ab startup pe hi clear warning.
if shutil.which("ffmpeg") is None:
    print("⚠️ Warning: ffmpeg nahi mila PATH me! Audio (MP3) conversion aur "
          "video+audio merge dono FAIL honge jab tak ffmpeg install nahi karte.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("aio_bot")

# NOTE (webhook upgrade): production me infinity_polling ki jagah webhooks
# use karna better scale karta hai (ek hi process baar baar Telegram ko poll
# nahi karta). Iske liye ek public HTTPS endpoint (domain + TLS cert) aur
# ek chhota web-server (Flask/FastAPI) chahiye jo bot.process_new_updates()
# ko call kare - ye deployment-specific hai isliye yahan polling hi default
# rakha hai, lekin bot object same rehta hai chahe polling ho ya webhook.
bot = telebot.TeleBot(TOKEN, parse_mode="HTML")

# key: (chat_id, message_id) -> (url, timestamp)
# (chat_id, message_id) tuple globally unique hai, isliye do users ke
# messages ka id clash nahi karta.
user_links = {}
user_links_lock = threading.Lock()

# BUG (double-click race condition) FIX:
#  - Button dabate hi entry user_links se turant POP (remove) kar dete hain,
#    isse dusra/teesra click "link purana ho gaya" bolega, koi duplicate
#    job queue nahi hogi.
#  - Har job ko ek unique job_id (uuid4 hex) milta hai jo filename me use
#    hota hai (aio_<job_id>.*), isliye do parallel jobs kabhi same file pe
#    collide nahi karenge, chahe race case bach bhi jaaye.
download_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)

# Per-chat active job counter (queued + processing), BUG (no per-user limit) FIX.
active_jobs_per_chat = {}
active_jobs_lock = threading.Lock()

# Queue order tracker sirf /status ke liye position dikhane ke kaam aata hai.
# queue.Queue khud peek/index support nahi karti, isliye ye parallel list
# rakhte hain (same lock ke andar hi mutate hoti hai).
queue_order = []
queue_order_lock = threading.Lock()


# ==========================================
# DATABASE SETUP (For Broadcast)
# ==========================================
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


# ==========================================
# HELPERS
# ==========================================
def h(text: str) -> str:
    """HTML-escape helper. Legacy Markdown me backtick jaise chars escape
    karna error-prone tha (backtick khud escape nahi ho raha tha -> broken
    formatting). HTML parse_mode + html.escape() zyada robust hai: sirf
    &, <, > escape karne padte hain aur wo hamesha sahi render hote hain."""
    if not text:
        return text
    return html.escape(text)


def is_url_safe(url: str) -> tuple[bool, str]:
    """BUG (no domain whitelist / SSRF risk) FIX: sirf whitelisted domains
    allow karte hain, aur hostname ko resolve karke check karte hain ki wo
    kisi private/internal/loopback IP par point na kare (basic SSRF guard).
    Returns (is_safe, reason_if_not)."""
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
        return False, "Ye site supported nahi hai. Sirf Instagram, Facebook, Twitter/X, TikTok, YouTube links allowed hain."

    # Hostname ko resolve karke private/loopback/link-local IPs block karo,
    # taaki koi DNS-rebinding se internal network target na kar sake.
    try:
        infos = socket.getaddrinfo(hostname, None)
        for info in infos:
            ip = info[4][0]
            ip_obj = ipaddress.ip_address(ip)
            if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local or ip_obj.is_reserved:
                return False, "Ye link allowed nahi hai (internal address)."
    except socket.gaierror:
        return False, "Host resolve nahi ho raha."
    except Exception as e:
        log.warning(f"URL safety check failed for {url}: {e}")
        return False, "URL verify nahi ho paaya."

    return True, ""


# ==========================================
# AUTO STORAGE CLEANER
# ==========================================
def auto_cleaner():
    while True:
        time.sleep(3600)
        now = time.time()
        for file in glob.glob(os.path.join(BASE_DIR, "aio_*")):
            try:
                if os.path.isfile(file) and os.stat(file).st_mtime < now - 1800:
                    os.remove(file)
                    log.info(f"Auto-cleaned stale file: {file}")
            except FileNotFoundError:
                pass
            except Exception as e:
                log.warning(f"Could not remove {file}: {e}")


def user_links_cleaner():
    while True:
        time.sleep(600)
        now = time.time()
        with user_links_lock:
            stale = [key for key, (_, ts) in user_links.items() if now - ts > LINK_TTL_SECONDS]
            for key in stale:
                user_links.pop(key, None)
        if stale:
            log.info(f"Cleaned {len(stale)} stale link entries")


threading.Thread(target=auto_cleaner, daemon=True).start()
threading.Thread(target=user_links_cleaner, daemon=True).start()

# ==========================================
# BOT COMMANDS & LOGIC
# ==========================================
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    add_user(message.chat.id)
    bot.reply_to(
        message,
        "👋 <b>AIO Downloader Ready!</b>\n\nKoi bhi Insta, FB, Twitter/X, TikTok ya YouTube ka link bhej aur jadoo dekh! 🚀\n\n"
        "Commands:\n/status — apni queue position dekh\n/broadcast — (admin only)"
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
        bot.reply_to(
            message,
            f"⏳ Tere {len(my_positions)} job(s) queue me hain (position: {pos_text} out of {total_in_queue})."
        )
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
                retry_after = 5
                try:
                    retry_after = e.result_json.get("parameters", {}).get("retry_after", 5)
                except Exception:
                    pass
                log.warning(f"Flood control hit, sleeping {retry_after}s")
                time.sleep(retry_after + 1)
                try:
                    bot.send_message(chat_id, f"📢 <b>Admin Update:</b>\n\n{safe_text}")
                    success += 1
                except Exception as e2:
                    failed += 1
                    log.warning(f"Broadcast retry failed for {chat_id}: {e2}")
            elif e.error_code == 403:
                remove_user(chat_id)
                failed += 1
            else:
                failed += 1
                log.warning(f"Broadcast failed for {chat_id}: {e}")
        except Exception as e:
            failed += 1
            log.warning(f"Broadcast failed for {chat_id}: {e}")

        if i % BROADCAST_PROGRESS_EVERY == 0:
            try:
                bot.edit_message_text(
                    f"📢 Broadcasting... {i}/{len(users)} processed ({success} sent, {failed} failed)",
                    chat_id=progress_msg.chat.id, message_id=progress_msg.message_id
                )
            except ApiTelegramException as e:
                if "message is not modified" not in str(e).lower():
                    log.warning(f"Broadcast progress edit failed: {e}")
            except Exception as e:
                log.warning(f"Broadcast progress edit failed: {e}")

    try:
        bot.edit_message_text(
            f"✅ Broadcast finished! Sent to {success} users. Failed: {failed}.",
            chat_id=progress_msg.chat.id, message_id=progress_msg.message_id
        )
    except Exception as e:
        log.warning(f"Broadcast final edit failed: {e}")


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


# ==========================================
# WORKER POOL (bounded, FIFO, leak-proof)
# ==========================================
@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    chat_id = call.message.chat.id
    try:
        mode_prefix, msg_id_str = call.data.split('_', maxsplit=1)
        msg_id = int(msg_id_str)
        mode = 'audio' if mode_prefix == 'aud' else 'video'
    except Exception:
        return bot.answer_callback_query(call.id, "❌ Error!")

    # BUG (double-click race condition) FIX: entry ko POP karo, na ki sirf
    # get(). Isse dusra click (chahe same button ho ya doosra) is entry ko
    # nahi paayega -> "purana ho gaya" dikhega, duplicate job kabhi queue
    # nahi hogi. Buttons bhi turant hata dete hain taaki UI clear rahe.
    with user_links_lock:
        entry = user_links.pop((chat_id, msg_id), None)
    url = entry[0] if entry else None

    try:
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
    except Exception:
        pass

    if not url:
        return bot.answer_callback_query(call.id, "⚠️ Ye link purana ho gaya hai ya already process ho chuka hai. Wapas naya link bhej!", show_alert=True)

    # BUG (unbounded queue / no per-user limit) FIX
    with active_jobs_lock:
        current = active_jobs_per_chat.get(chat_id, 0)
        if current >= MAX_JOBS_PER_USER:
            bot.answer_callback_query(call.id, f"⚠️ Tere already {current} jobs chal/queue me hain. Pehle unka wait kar!", show_alert=True)
            return
        active_jobs_per_chat[chat_id] = current + 1

    bot.answer_callback_query(call.id)
    status_msg = bot.send_message(chat_id, "⏳ <b>Queued... tera number aane wala hai!</b>")

    job_id = uuid.uuid4().hex  # unique per-job id -> filenames kabhi collide nahi karenge

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
            bot.edit_message_text("⚠️ Bot abhi bahot busy hai (queue full). Thodi der baad try kar.",
                                   chat_id=chat_id, message_id=status_msg.message_id)
        except Exception:
            pass


def download_worker():
    """Fixed-size worker pool. MAX_CONCURRENT_DOWNLOADS threads is loop ko
    run karte hain, queue se job uthate hain, aur job khatam hote hi
    (chahe success ho ya exception) agla job lete hain - koi leak nahi,
    koi FIFO-violation nahi."""
    while True:
        chat_id, msg_id, mode, url, status_msg, job_id = download_queue.get()
        with queue_order_lock:
            queue_order[:] = [item for item in queue_order if item[1] != job_id]
        try:
            process_download(chat_id, msg_id, mode, url, status_msg, job_id)
        except Exception as e:
            log.error(f"Unhandled worker error: {e}")
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
        bot.edit_message_text(
            "⏳ <b>Processing... Please wait!</b>",
            chat_id=chat_id, message_id=status_msg.message_id
        )
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
                except ApiTelegramException as e:
                    if "message is not modified" not in str(e).lower():
                        log.warning(f"Progress edit failed: {e}")
                except Exception as e:
                    log.warning(f"Progress edit failed: {e}")
        elif d['status'] == 'finished':
            try:
                bot.edit_message_text(
                    "✅ <b>Download complete! Uploading to Telegram...</b>",
                    chat_id=chat_id, message_id=status_msg.message_id
                )
            except Exception:
                pass

    if mode == 'audio':
        # Aakhri fallback bina filesize-filter ke hai, taaki agar site
        # filesize report nahi karti (bahot common IG/FB/Twitter par), download
        # at least try ho; final safety net size-check niche hai.
        format_str = (
            f'bestaudio[filesize<{MAX_DOWNLOAD_MB}M]'
            f'/best[filesize<{MAX_DOWNLOAD_MB}M]'
            f'/bestaudio/best'
        )
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
        # Dono video+audio streams filesize-filtered hain (merged file size
        # dono ka sum hoti hai), aur aakhri fallback bina filter ke hai.
        format_str = (
            f'bestvideo[filesize<{MAX_DOWNLOAD_MB}M][ext=mp4]+bestaudio[filesize<{MAX_DOWNLOAD_MB}M][ext=m4a]'
            f'/best[filesize<{MAX_DOWNLOAD_MB}M][ext=mp4]'
            f'/best[filesize<{MAX_DOWNLOAD_MB}M]'
            f'/best'
        )
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
            raise FileNotFoundError("Downloaded file nahi mila (shayad size limit se bada tha ya merge/convert fail hua).")

        file_size = os.path.getsize(filename) / (1024 * 1024)

        if file_size > TELEGRAM_HARD_CAP_MB:
            bot.edit_message_text(
                f"⚠️ <b>File bahot badi hai</b> ({file_size:.1f} MB)! Telegram bots max "
                f"{TELEGRAM_HARD_CAP_MB}MB tak hi bhej sakte hain. Chota clip try kar.",
                chat_id=chat_id, message_id=status_msg.message_id
            )
        else:
            with open(filename, 'rb') as f:
                if mode == 'audio':
                    if file_size > MAX_UPLOAD_MB:
                        bot.send_document(chat_id, f, caption="🔥 Badi audio file hone ki wajah se document bheja hai!")
                    else:
                        bot.send_audio(chat_id, f, caption="🔥 MP3 by AIO Bot!")
                elif file_size > MAX_UPLOAD_MB:
                    bot.send_document(chat_id, f, caption="🔥 Badi file hone ki wajah se document bheja hai!")
                else:
                    bot.send_video(chat_id, f, caption="🔥 Downloaded by AIO Bot!")

            # Isko apne try/except me isolate kiya hai taaki delete-message
            # ka fail hona upload-success ke result ko na todh sake.
            try:
                bot.delete_message(chat_id, status_msg.message_id)
            except Exception as e:
                log.warning(f"Could not delete status message: {e}")

    except Exception as e:
        raw_msg = str(e).replace('\033', '').replace('\x1b', '')
        error_msg = h(raw_msg[:300])
        log.error(f"Download failed for {url}: {raw_msg}")
        try:
            bot.edit_message_text(
                f"❌ <b>Mission Failed!</b>\n<code>{error_msg}</code>",
                chat_id=chat_id, message_id=status_msg.message_id
            )
        except Exception as e2:
            log.warning(f"Failed to report error to user: {e2}")

    finally:
        # Is job ke saare leftover 'aio_<job_id>.*' files (fragments, .part,
        # intermediate merge files agar postprocess beech me fail ho) turant
        # clean ho jaate hain, auto_cleaner ke 30-min wait ka wait nahi karna padta.
        for leftover in glob.glob(f"{file_prefix}.*"):
            try:
                os.remove(leftover)
            except Exception as e:
                log.warning(f"Could not remove {leftover}: {e}")


if __name__ == '__main__':
    log.info("🚀 God Mode Bot is running with SQLite & Worker Pool...")
    bot.infinity_polling(skip_pending=True)