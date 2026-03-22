import asyncio
import io
import logging
import re
import sqlite3
import time
from datetime import datetime, timezone, timedelta

import aiohttp
import qrcode

from telegram import (
    Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update, ChatMember
)
from telegram.ext import (
    ApplicationBuilder, CallbackQueryHandler, CommandHandler,
    MessageHandler, ContextTypes, filters
)
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

# ─── CONFIG ──────────────────────────────────────────────────────────────────
BOT_TOKEN      = "8775850651:AAHBuRzSCAnkDM71Oq3zKz4V_gOcJ6LVHM8"
ADMIN_IDS      = [7745765588]
ADMIN_GROUP_ID = -1003830791866
LOG_CHANNEL_ID = -1003792173111
API_ID         = 30191201
API_HASH       = "5c87a8808e935cc3d97958d0bb24ff1f"
UPI_ID         = "banny143@ptyes"
DB_PATH        = "numberstore7.db"
IST            = timezone(timedelta(hours=5, minutes=30))

OXAPAY_MERCHANT_KEY = "R7GWJN-NPCMVX-H3QYHQ-FL2DJA"
OXAPAY_API_BASE     = "https://api.oxapay.com"
STORE_TAG           = "@OTP_SELLER134_BOT"
STORE_LINK          = "https://t.me/OTP_SELLER134_BOT"
SERVER_NUM          = 1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ─── MARKDOWN ESCAPE ─────────────────────────────────────────────────────────
def mesc(t):
    """Escape Markdown special chars to prevent parse errors."""
    if not t:
        return ""
    for ch in ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']:
        t = str(t).replace(ch, f'\\{ch}')
    return t


# ─── DATABASE ─────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS force_channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id TEXT UNIQUE, channel_link TEXT, channel_name TEXT
    );
    CREATE TABLE IF NOT EXISTS stock_categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE, price_inr REAL DEFAULT 0, price_usd REAL DEFAULT 0,
        enabled INTEGER DEFAULT 1, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_id INTEGER, category_name TEXT,
        phone_number TEXT, session_string TEXT, two_fa_password TEXT,
        is_sold INTEGER DEFAULT 0, sold_to INTEGER, sold_at TIMESTAMP,
        added_by INTEGER, added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, username TEXT, account_id INTEGER,
        category_id INTEGER, category_name TEXT,
        amount_inr REAL, amount_usd REAL,
        payment_method TEXT DEFAULT 'upi',
        payment_screenshot TEXT, crypto_track_id TEXT,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        reviewed_by INTEGER, reviewed_at TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY, username TEXT, first_name TEXT,
        is_banned INTEGER DEFAULT 0, total_purchases INTEGER DEFAULT 0,
        wallet_balance REAL DEFAULT 0,
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS deposits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, amount_inr REAL, amount_usd REAL,
        payment_method TEXT DEFAULT 'upi',
        screenshot TEXT, crypto_track_id TEXT,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        reviewed_by INTEGER, reviewed_at TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY, value TEXT
    );
    """)
    for k, v in [
        ("maintenance",     "0"),
        ("upi_enabled",     "1"),
        ("crypto_enabled",  "1"),
        ("usdt_rate",       "83"),
        ("welcome_message", "🏪 Welcome to NumberStore!\nBuy verified phone numbers instantly.\nFast • Secure • 24/7"),
    ]:
        c.execute("INSERT OR IGNORE INTO settings VALUES (?,?)", (k, v))
    conn.commit()
    conn.close()


# ─── HELPERS ─────────────────────────────────────────────────────────────────
def now_ist():
    return datetime.now(IST)

def fmt_time(ts_str):
    if not ts_str:
        return "N/A"
    try:
        dt = datetime.fromisoformat(str(ts_str))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST).strftime("%d %b %Y %H:%M IST")
    except Exception:
        return str(ts_str)

def get_setting(key, default=""):
    conn = get_db()
    row  = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default

def set_setting(key, value):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, str(value)))
    conn.commit()
    conn.close()

def get_usdt_rate():
    try:
        return float(get_setting("usdt_rate", "83"))
    except Exception:
        return 83.0

def inr_to_usd(inr):
    rate = get_usdt_rate()
    return round(inr / rate, 2) if rate > 0 else 0.0

def register_user(user):
    conn = get_db()
    conn.execute(
        "INSERT OR IGNORE INTO users (id,username,first_name,joined_at) VALUES (?,?,?,?)",
        (user.id, user.username or "", user.first_name or "", now_ist().isoformat())
    )
    conn.execute(
        "UPDATE users SET username=?,first_name=? WHERE id=?",
        (user.username or "", user.first_name or "", user.id)
    )
    conn.commit()
    conn.close()

def is_banned(user_id):
    conn = get_db()
    row  = conn.execute("SELECT is_banned FROM users WHERE id=?", (user_id,)).fetchone()
    conn.close()
    return row and row["is_banned"] == 1

def is_maintenance():
    return get_setting("maintenance", "0") == "1"

def is_admin(user_id):
    return user_id in ADMIN_IDS

def status_emoji(s):
    return {"pending":"⏳","approved":"✅","rejected":"❌","paid":"💚","expired":"⌛"}.get(s,"❓")

def main_menu_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛒 Browse Numbers", callback_data="browse_0"),
         InlineKeyboardButton("💰 My Wallet",       callback_data="wallet")],
        [InlineKeyboardButton("📦 My Orders",       callback_data="my_orders_0"),
         InlineKeyboardButton("❓ Help",             callback_data="help")],
    ])

def generate_upi_qr(amount, note):
    upi_url = f"upi://pay?pa={UPI_ID}&pn=NumberStore&am={amount}&cu=INR&tn={note}"
    qr = qrcode.QRCode(version=1, box_size=10, border=4)
    qr.add_data(upi_url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf

def get_stock_count(cat_id):
    conn = get_db()
    row  = conn.execute("SELECT COUNT(*) as c FROM accounts WHERE category_id=? AND is_sold=0", (cat_id,)).fetchone()
    conn.close()
    return row["c"] if row else 0

def get_cat(cat_id):
    conn = get_db()
    row  = conn.execute("SELECT * FROM stock_categories WHERE id=?", (cat_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


# ─── FORCE-SUB ───────────────────────────────────────────────────────────────
def get_force_channels():
    conn = get_db()
    rows = conn.execute("SELECT * FROM force_channels").fetchall()
    conn.close()
    return [dict(r) for r in rows]

async def check_force_sub(bot, user_id):
    not_joined = []
    for ch in get_force_channels():
        try:
            member = await bot.get_chat_member(chat_id=ch["channel_id"], user_id=user_id)
            if member.status in (ChatMember.LEFT, ChatMember.BANNED):
                not_joined.append(ch)
        except Exception:
            not_joined.append(ch)
    return not_joined

async def send_force_sub_msg(update, not_joined):
    buttons = []
    for i, ch in enumerate(not_joined, 1):
        label = ch["channel_name"] or f"Channel {i}"
        buttons.append([InlineKeyboardButton(f"➕ Join {label}", url=ch["channel_link"])])
    buttons.append([InlineKeyboardButton("✅ I've Joined — Verify", callback_data="verify_sub")])
    lines = ["⚠️ *Access Restricted*\n━━━━━━━━━━━━━━━━━━━━\nJoin these channels to use the bot:\n"]
    for ch in not_joined:
        lines.append(f"• {ch['channel_name'] or ch['channel_id']}")
    lines.append("\n━━━━━━━━━━━━━━━━━━━━\n_Tap Verify after joining._")
    msg = update.message or (update.callback_query.message if update.callback_query else None)
    if msg:
        try:
            await msg.reply_text("\n".join(lines), parse_mode="Markdown",
                                 reply_markup=InlineKeyboardMarkup(buttons))
        except Exception:
            pass


# ─── GUARD ───────────────────────────────────────────────────────────────────
async def guard(update, context):
    user = update.effective_user
    if not user:
        return True
    register_user(user)
    if is_banned(user.id):
        txt = "🚫 You are banned from using this bot."
        if update.callback_query:
            await update.callback_query.answer(txt, show_alert=True)
        else:
            await update.effective_message.reply_text(txt)
        return True
    if is_maintenance() and not is_admin(user.id):
        txt = "🔧 Bot is under maintenance. Please try again later."
        if update.callback_query:
            await update.callback_query.answer(txt, show_alert=True)
        else:
            await update.effective_message.reply_text(txt)
        return True
    if not is_admin(user.id):
        not_joined = await check_force_sub(context.bot, user.id)
        if not_joined:
            await send_force_sub_msg(update, not_joined)
            return True
    return False


# ─── VERIFY SUB ──────────────────────────────────────────────────────────────
async def verify_sub(update, context):
    query = update.callback_query
    await query.answer()
    not_joined = await check_force_sub(context.bot, query.from_user.id)
    if not_joined:
        buttons = []
        for i, ch in enumerate(not_joined, 1):
            buttons.append([InlineKeyboardButton(f"➕ Join {ch['channel_name'] or f'Channel {i}'}", url=ch["channel_link"])])
        buttons.append([InlineKeyboardButton("✅ Verify Again", callback_data="verify_sub")])
        lines = ["❌ Still not joined all channels!\n"]
        for ch in not_joined:
            lines.append(f"• {ch['channel_name'] or ch['channel_id']}")
        await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(buttons))
    else:
        msg = get_setting("welcome_message", "🏪 Welcome to NumberStore!")
        await query.edit_message_text(msg, reply_markup=main_menu_kb())


# ─── /start ──────────────────────────────────────────────────────────────────
async def start(update, context):
    if await guard(update, context):
        return
    msg = get_setting("welcome_message", "🏪 Welcome to NumberStore!")
    await update.message.reply_text(msg, reply_markup=main_menu_kb())


# ─── /addchannel /removechannel ──────────────────────────────────────────────
async def addchannel_cmd(update, context):
    if not is_admin(update.effective_user.id):
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /addchannel <channel_id> <invite_link> [Name]")
        return
    ch_id, ch_link = args[0], args[1]
    ch_name = " ".join(args[2:]) if len(args) > 2 else ch_id
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO force_channels (channel_id,channel_link,channel_name) VALUES (?,?,?)",
                 (ch_id, ch_link, ch_name))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"✅ Channel added: {ch_name}")

async def removechannel_cmd(update, context):
    if not is_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Usage: /removechannel <channel_id>")
        return
    conn = get_db()
    conn.execute("DELETE FROM force_channels WHERE channel_id=?", (context.args[0],))
    conn.commit()
    conn.close()
    await update.message.reply_text("✅ Channel removed.")


# ─── LOG CHANNEL ─────────────────────────────────────────────────────────────
async def send_purchase_log(bot, category_name, price_inr, phone_number, username, user_id):
    ph = str(phone_number)
    masked = f"+{ph[:4]}{'•' * max(0, len(ph)-4)}"
    user_tag = f"@{username}" if username else f"ID:{user_id}"
    # Plain text for log channel — no markdown to avoid parse errors
    text = (
        f"✅ New Number Purchase Successful\n"
        f"➖ Category: {category_name} | ₹{price_inr:.0f}\n"
        f"➕ Number: {masked} 📞\n"
        f"➕ Server: ({SERVER_NUM}) 🥂\n"
        f"• {user_tag} || {STORE_TAG}"
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🛒 Buy Now", url=STORE_LINK)]])
    try:
        await bot.send_message(chat_id=LOG_CHANNEL_ID, text=text, reply_markup=kb)
    except Exception as e:
        logger.error(f"Log channel error: {e}")


# ─── OXAPAY ──────────────────────────────────────────────────────────────────
async def oxapay_create_invoice(amount_usd, desc, order_ref):
    payload = {
        "merchant": OXAPAY_MERCHANT_KEY, "amount": round(float(amount_usd), 2),
        "currency": "USDT", "lifeTime": 30, "feePaidByPayer": 1,
        "description": desc, "orderId": order_ref,
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{OXAPAY_API_BASE}/merchants/request", json=payload) as r:
                data = await r.json()
                if data.get("result") == 100:
                    return {"payLink": data["payLink"], "trackId": data["trackId"]}
                logger.error(f"OxaPay: {data}")
    except Exception as e:
        logger.error(f"OxaPay failed: {e}")
    return None

async def oxapay_check(track_id):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{OXAPAY_API_BASE}/merchants/inquiry",
                              json={"merchant": OXAPAY_MERCHANT_KEY, "trackId": track_id}) as r:
                data = await r.json()
                if data.get("result") == 100:
                    return data.get("status")
    except Exception as e:
        logger.error(f"OxaPay check: {e}")
    return None

async def poll_crypto_order(context, track_id, user_id, order_id):
    for _ in range(60):
        await asyncio.sleep(30)
        status = await oxapay_check(track_id)
        if status == "Paid":
            conn  = get_db()
            order = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
            if order and order["status"] == "pending":
                acc = conn.execute("SELECT * FROM accounts WHERE category_id=? AND is_sold=0 LIMIT 1",
                                   (order["category_id"],)).fetchone()
                now = now_ist().isoformat()
                if acc:
                    conn.execute("UPDATE accounts SET is_sold=1,sold_to=?,sold_at=? WHERE id=?",
                                 (user_id, now, acc["id"]))
                    conn.execute("UPDATE orders SET status='approved',account_id=?,reviewed_at=? WHERE id=?",
                                 (acc["id"], now, order_id))
                    conn.execute("UPDATE users SET total_purchases=total_purchases+1 WHERE id=?", (user_id,))
                    conn.commit()
                    conn.close()
                    await send_purchase_log(context.bot, order["category_name"], order["amount_inr"],
                                            acc["phone_number"], order["username"], user_id)
                    kb = InlineKeyboardMarkup([[InlineKeyboardButton("📱 Reveal Number",
                                               callback_data=f"reveal_{order_id}")]])
                    try:
                        await context.bot.send_message(chat_id=user_id,
                            text=f"✅ Crypto payment confirmed! Order #{order_id} approved.",
                            reply_markup=kb)
                    except Exception:
                        pass
                else:
                    conn.execute("UPDATE orders SET status='rejected',reviewed_at=? WHERE id=?",
                                 (now, order_id))
                    conn.commit()
                    conn.close()
                    try:
                        await context.bot.send_message(chat_id=user_id,
                            text="❌ Payment received but no stock. Refund will be processed.")
                    except Exception:
                        pass
            else:
                conn.close()
            return
        elif status in ("Expired", "Failed"):
            break
    try:
        await context.bot.send_message(chat_id=user_id, text="⌛ Crypto payment expired.")
    except Exception:
        pass

async def poll_crypto_deposit(context, track_id, user_id, dep_id):
    for _ in range(60):
        await asyncio.sleep(30)
        status = await oxapay_check(track_id)
        if status == "Paid":
            conn = get_db()
            dep  = conn.execute("SELECT * FROM deposits WHERE id=?", (dep_id,)).fetchone()
            if dep and dep["status"] == "pending":
                now = now_ist().isoformat()
                conn.execute("UPDATE deposits SET status='approved',reviewed_at=? WHERE id=?", (now, dep_id))
                conn.execute("UPDATE users SET wallet_balance=wallet_balance+? WHERE id=?",
                             (dep["amount_inr"], user_id))
                conn.commit()
                try:
                    await context.bot.send_message(chat_id=user_id,
                        text=f"✅ Crypto deposit of ₹{dep['amount_inr']:.0f} credited to your wallet!",
                        reply_markup=main_menu_kb())
                except Exception:
                    pass
            conn.close()
            return
        elif status in ("Expired", "Failed"):
            break
    try:
        await context.bot.send_message(chat_id=user_id, text="⌛ Crypto deposit expired.")
    except Exception:
        pass


# ─── BROWSE ──────────────────────────────────────────────────────────────────
async def browse_numbers(update, context):
    query = update.callback_query
    await query.answer()
    if await guard(update, context):
        return
    page = int(query.data.split("_")[1])
    conn = get_db()
    cats = conn.execute("""
        SELECT s.*, (SELECT COUNT(*) FROM accounts a WHERE a.category_id=s.id AND a.is_sold=0) as stock_count
        FROM stock_categories s WHERE s.enabled=1 ORDER BY s.name
    """).fetchall()
    cats = [c for c in cats if c["stock_count"] > 0]
    conn.close()
    per_page = 5
    total    = len(cats)
    if total == 0:
        await query.edit_message_text("📦 *No stock available at the moment!*", parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]))
        return
    pages = max(1, (total + per_page - 1) // per_page)
    page  = max(0, min(page, pages - 1))
    chunk = cats[page * per_page:(page + 1) * per_page]
    upi_on    = get_setting("upi_enabled",   "1") == "1"
    crypto_on = get_setting("crypto_enabled","1") == "1"
    pay_icons = ("💳UPI " if upi_on else "") + ("🪙Crypto" if crypto_on else "")
    lines   = ["🛒 *Available Numbers*\n━━━━━━━━━━━━━━━━━━━━"]
    buttons = []
    for c in chunk:
        lines.append(f"📂 *{mesc(c['name'])}*\n   📦 Stock: {c['stock_count']}  |  ₹{c['price_inr']:.0f}  |  ${c['price_usd']:.2f}")
        buttons.append([InlineKeyboardButton(
            f"📂 {c['name']}  •  📦{c['stock_count']}  •  ₹{c['price_inr']:.0f}",
            callback_data=f"cat_{c['id']}"
       