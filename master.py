import os
import sys
import time
import json
import uuid
import shutil
import signal
import socket
import struct
import logging
import sqlite3
import datetime
import platform
import subprocess
import threading
import importlib
import traceback
import collections
import random
import string
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod

# ==========================================
# FIX: Removed psutil dependency for Android
# ==========================================
try:
    import telebot
    from telebot import types, apihelper
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyTelegramBotAPI"])
    import telebot
    from telebot import types, apihelper
# ==========================================

sys.setrecursionlimit(5000)

class Config:
    MASTER_TOKEN = "8408066095:AAG3Lsgv1QlZPDeBjZvcKbrqkJLP4SX6pZI"
    ADMIN_IDS = [6641816009]
    SUPPORT_USER = "@saytama420"
    DB_NAME = "master_ecosystem.db"
    BASE_PATH = os.path.dirname(os.path.abspath(__file__))
    BOTS_PATH = os.path.join(BASE_PATH, "hosted_bots")
    LOGS_PATH = os.path.join(BASE_PATH, "logs")
    BACKUP_PATH = os.path.join(BASE_PATH, "backups")
    TEMP_PATH = os.path.join(BASE_PATH, "temp")

    @staticmethod
    def ensure_dirs():
        for p in [Config.BOTS_PATH, Config.LOGS_PATH, Config.BACKUP_PATH, Config.TEMP_PATH]:
            if not os.path.exists(p):
                os.makedirs(p)

Config.ensure_dirs()

class TextResources:
    WELCOME = "👋 Hello {name}!\n\nWelcome to the Master Bot Ecosystem.\nHost unlimited Python bots with isolated processes.\n\nYour Plan: {plan}\nStatus: {status}\n\nSelect an option:"
    BANNED = "⛔ You are BANNED from this system.\nContact support: " + Config.SUPPORT_USER
    EXPIRED = "⏳ Plan Expired\n\nYour bot hosting service has been paused. Please contact admin to renew."
    BOT_RUNNING = "✅ Bot is currently RUNNING."
    BOT_STOPPED = "⏹ Bot is STOPPED."
    BOT_CRASHED = "⚠️ Bot CRASHED. Check logs."
    NO_BOTS = "You haven't created any bots yet."
    DEPLOY_OPTS = "🛠 Deployment Center\n\nChoose how you want to create your bot:"
    UPLOAD_GUIDE = "📂 Upload Python File\n\nPlease send your .py file now.\n\nRequirements:\n- Must be a valid Python script\n- No malicious code\n- Single file preferred"
    GIT_GUIDE = "🐙 GitHub Clone\n\nPlease send the public repository URL."
    TOKEN_ASK = "🔑 Bot Token Required\n\nPlease send the API Token from @BotFather for this bot."
    SUCCESS_DEPLOY = "🎉 Deployment Successful!\n\nYour bot is ready. Go to 'My Bots' to manage it."
    ADMIN_PANEL = "🔒 Admin Control Panel\n\nManage the entire ecosystem."
    BROADCAST_ASK = "📢 Send the message you want to broadcast to all users."
    BROADCAST_DONE = "✅ Broadcast sent to {count} users."
    SUPPORT_MENU = "🆘 Support Center\n\nCreate a ticket or contact admin."
    TICKET_CREATED = "✅ Ticket #{id} created. Please wait for a reply."
    REFERRAL_INFO = "🤝 Referral System\n\nInvite friends!\n\nYour Link: {link}\nReferrals: {count}\n\n(Rewards are manually assigned by Admin)"
    COUPON_ASK = "🎫 Admin Coupon Application\n\nSend: /apply_coupon [User_ID] [Code]"
    COUPON_SUCCESS = "✅ Coupon redeemed! {days} days added."
    COUPON_INVALID = "❌ Invalid or expired coupon."
    TEMPLATES_MENU = "🧩 Bot Templates\n\nDeploy a pre-built bot instantly."
    PLANS_HIDDEN = "🔒 **Access Locked**\n\nPremium plans are available exclusively via Admin.\nContact: " + Config.SUPPORT_USER
    GIFT_ASK_USER = "🎁 **Gift Plan**\n\nPlease enter the Telegram User ID of the person you want to gift."
    GIFT_ASK_DAYS = "⏳ Enter the number of days you want to gift:"
    GIFT_CONFIRM = "❓ Confirm gift of {days} days to User ID `{uid}`?\n\nType 'yes' to confirm or 'cancel' to stop."
    GIFT_PENDING = "✅ Gift Request Sent!\n\nWaiting for Admin approval."

class Logger:
    def init(self):
        self.log_file = os.path.join(Config.LOGS_PATH, "system.log")

    def log(self, level, message, user_id=None):
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        uid_str = f"[User:{user_id}]" if user_id else "[System]"
        entry = f"[{ts}] [{level.upper()}] {uid_str} {message}\n"
        
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(entry)
            
    def info(self, msg, uid=None): self.log("INFO", msg, uid)
    def warning(self, msg, uid=None): self.log("WARNING", msg, uid)
    def error(self, msg, uid=None): self.log("ERROR", msg, uid)
    def critical(self, msg, uid=None): self.log("CRITICAL", msg, uid)

sys_logger = Logger()
sys_logger.init()

class Database:
    def __init__(self):
        self.conn = None
        self.lock = threading.Lock()
        self.connect()
        self.create_tables()

    def connect(self):
        self.conn = sqlite3.connect(Config.DB_NAME, check_same_thread=False)

    def create_tables(self):
        with self.lock:
            c = self.conn.cursor()
            
            c.execute("""CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_at TIMESTAMP,
                plan_type TEXT DEFAULT 'free',
                plan_expiry TIMESTAMP,
                is_banned INTEGER DEFAULT 0,
                balance REAL DEFAULT 0.0,
                ref_code TEXT UNIQUE,
                referred_by INTEGER,
                total_referrals INTEGER DEFAULT 0
            )""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS bots (
                uuid TEXT PRIMARY KEY,
                owner_id INTEGER,
                name TEXT,
                token TEXT,
                source_type TEXT,
                path TEXT,
                status TEXT,
                pid INTEGER,
                created_at TIMESTAMP,
                runtime INTEGER DEFAULT 0,
                last_error TEXT,
                env_vars TEXT
            )""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS transactions (
                id TEXT PRIMARY KEY,
                user_id INTEGER,
                amount REAL,
                method TEXT,
                status TEXT,
                date TIMESTAMP
            )""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                subject TEXT,
                status TEXT,
                created_at TIMESTAMP
            )""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS ticket_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER,
                sender_id INTEGER,
                message TEXT,
                timestamp TIMESTAMP
            )""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS coupons (
                code TEXT PRIMARY KEY,
                days INTEGER,
                max_uses INTEGER,
                used_count INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_by INTEGER
            )""")

            c.execute("""CREATE TABLE IF NOT EXISTS coupon_usage (
                code TEXT,
                user_id INTEGER,
                timestamp TIMESTAMP,
                applied_by INTEGER,
                PRIMARY KEY (code, user_id)
            )""")

            # New Table: Gifts
            c.execute("""CREATE TABLE IF NOT EXISTS gifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id INTEGER,
                receiver_id INTEGER,
                days INTEGER,
                status TEXT, -- pending, approved, rejected
                created_at TIMESTAMP
            )""")

            # New Table: Audit Logs
            c.execute("""CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT,
                user_id INTEGER,
                details TEXT,
                timestamp TIMESTAMP
            )""")
            
            self.conn.commit()

    def exec(self, query, params=(), fetch_all=False, fetch_one=False):
        with self.lock:
            try:
                c = self.conn.cursor()
                c.execute(query, params)
                if fetch_all:
                    return c.fetchall()
                if fetch_one:
                    return c.fetchone()
                self.conn.commit()
                return True
            except Exception as e:
                sys_logger.error(f"DB Error: {e}\nQuery: {query}")
                return None

    def get_user(self, user_id):
        return self.exec("SELECT * FROM users WHERE user_id=?", (user_id,), fetch_one=True)
    
    def is_banned(self, user_id):
        u = self.get_user(user_id)
        if u and u[6] == 1:
            return True
        return False

    def add_user(self, user_id, username, first_name, ref_by=None):
        if self.get_user(user_id): return
        ref_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        expiry = datetime.datetime.now() + datetime.timedelta(hours=24) # 24h trial
        
        # Handle referral (Record only, no auto reward)
        referrer_id = None
        if ref_by:
            ref_user = self.exec("SELECT user_id FROM users WHERE ref_code=?", (ref_by,), fetch_one=True)
            if ref_user:
                referrer_id = ref_user[0]
                self.exec("UPDATE users SET total_referrals = total_referrals + 1 WHERE user_id=?", (referrer_id,))
                self.log_audit("Referral", user_id, f"Referred by {referrer_id}")

        self.exec("""INSERT INTO users (user_id, username, first_name, joined_at, plan_expiry, ref_code, referred_by)
                     VALUES (?, ?, ?, ?, ?, ?, ?)""", 
                     (user_id, username, first_name, datetime.datetime.now(), expiry, ref_code, referrer_id))

    def update_user_plan(self, user_id, days):
        user = self.get_user(user_id)
        if not user: return
        current_expiry_str = user[5]
        
        try:
            curr = datetime.datetime.strptime(current_expiry_str, "%Y-%m-%d %H:%M:%S.%f")
        except:
            try:
                curr = datetime.datetime.strptime(current_expiry_str, "%Y-%m-%d %H:%M:%S")
            except:
                curr = datetime.datetime.now()
        
        # Allow adding to current time if expired
        if curr < datetime.datetime.now():
            curr = datetime.datetime.now()
            
        new_expiry = curr + datetime.timedelta(days=days)
        
        # If removing time resulted in past date, set to now (expire)
        if new_expiry < datetime.datetime.now():
            new_expiry = datetime.datetime.now()

        self.exec("UPDATE users SET plan_expiry=?, plan_type='paid' WHERE user_id=?", (new_expiry, user_id))
        return new_expiry

    def get_bots(self, user_id):
        return self.exec("SELECT * FROM bots WHERE owner_id=?", (user_id,), fetch_all=True)

    def get_bot(self, bot_uuid):
        return self.exec("SELECT * FROM bots WHERE uuid=?", (bot_uuid,), fetch_one=True)

    def add_bot(self, user_id, name, token, source_type, path):
        uid = str(uuid.uuid4())
        self.exec("""INSERT INTO bots (uuid, owner_id, name, token, source_type, path, status, created_at)
                     VALUES (?, ?, ?, ?, ?, ?, 'stopped', ?)""",
                     (uid, user_id, name, token, source_type, path, datetime.datetime.now()))
        return uid

    def update_bot_status(self, bot_uuid, status, pid=None):
        self.exec("UPDATE bots SET status=?, pid=? WHERE uuid=?", (status, pid, bot_uuid))

    def log_bot_error(self, bot_uuid, error):
        self.exec("UPDATE bots SET last_error=? WHERE uuid=?", (error, bot_uuid))

    def create_ticket(self, user_id, subject):
        self.exec("INSERT INTO tickets (user_id, subject, status, created_at) VALUES (?, ?, 'open', ?)",
                  (user_id, subject, datetime.datetime.now()))
        return self.exec("SELECT last_insert_rowid()", fetch_one=True)[0]

    def add_ticket_message(self, ticket_id, sender_id, message):
        self.exec("INSERT INTO ticket_messages (ticket_id, sender_id, message, timestamp) VALUES (?, ?, ?, ?)",
                  (ticket_id, sender_id, message, datetime.datetime.now()))

    def log_audit(self, action, user_id, details):
        self.exec("INSERT INTO audit_logs (action, user_id, details, timestamp) VALUES (?, ?, ?, ?)",
                  (action, user_id, details, datetime.datetime.now()))
    
    def create_gift(self, sender, receiver, days):
        self.exec("INSERT INTO gifts (sender_id, receiver_id, days, status, created_at) VALUES (?, ?, ?, 'pending', ?)",
                  (sender, receiver, days, datetime.datetime.now()))
        return self.exec("SELECT last_insert_rowid()", fetch_one=True)[0]

db = Database()

class BotEngine:
    def __init__(self):
        self.processes = {}
        self.lock = threading.Lock()

    def start(self, bot_uuid):
        bot = db.get_bot(bot_uuid)
        if not bot: return False, "Bot not found"
        
        path = bot[5]
        token = bot[3]
        
        if bot_uuid in self.processes:
            if self.processes[bot_uuid].poll() is None:
                return False, "Already running"

        try:
            env = os.environ.copy()
            env['BOT_TOKEN'] = token
            
            if platform.system() == "Windows":
                proc = subprocess.Popen([sys.executable, path], env=env, cwd=os.path.dirname(path))
            else:
                proc = subprocess.Popen([sys.executable, path], env=env, cwd=os.path.dirname(path), preexec_fn=os.setsid)
            
            with self.lock:
                self.processes[bot_uuid] = proc
            
            db.update_bot_status(bot_uuid, 'running', proc.pid)
            return True, "Started"
        except Exception as e:
            db.log_bot_error(bot_uuid, str(e))
            return False, str(e)

    def stop(self, bot_uuid):
        with self.lock:
            if bot_uuid in self.processes:
                proc = self.processes[bot_uuid]
                try:
                    if platform.system() == "Windows":
                        proc.terminate()
                    else:
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                except:
                    try:
                        proc.kill()
                    except:
                        pass
                
                del self.processes[bot_uuid]
                db.update_bot_status(bot_uuid, 'stopped', None)
                return True, "Stopped"
            else:
                db.update_bot_status(bot_uuid, 'stopped', None)
                return True, "Already stopped"

    def restart(self, bot_uuid):
        self.stop(bot_uuid)
        time.sleep(1)
        return self.start(bot_uuid)

    def check_health(self):
        crashed = []
        with self.lock:
            for uuid, proc in self.processes.items():
                if proc.poll() is not None:
                    crashed.append(uuid)
        
        for c in crashed:
            self.stop(c)
            db.update_bot_status(c, 'crashed', None)

bot_engine = BotEngine()

class Security:
    @staticmethod
    def is_admin(user_id):
        return user_id in Config.ADMIN_IDS

    @staticmethod
    def validate_file(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
            compile(content, path, 'exec')
            return True, "OK"
        except SyntaxError as e:
            return False, f"Syntax Error: {e}"
        except Exception as e:
            return False, str(e)

class TemplateManager:
    @staticmethod
    def get_echo_bot():
        return """
import telebot
import os

TOKEN = os.environ.get('BOT_TOKEN')
bot = telebot.TeleBot(TOKEN)

@bot.message_handler(func=lambda m: True)
def echo(m):
    bot.reply_to(m, m.text)

bot.infinity_polling()
"""

    @staticmethod
    def get_calc_bot():
        return """
import telebot
import os

TOKEN = os.environ.get('BOT_TOKEN')
bot = telebot.TeleBot(TOKEN)

@bot.message_handler(commands=['start'])
def start(m):
    bot.reply_to(m, "Send me a math expression like 2+2")

@bot.message_handler(func=lambda m: True)
def calc(m):
    try:
        res = eval(m.text)
        bot.reply_to(m, f"Result: {res}")
    except:
        bot.reply_to(m, "Invalid Expression")

bot.infinity_polling()
"""

    @staticmethod
    def save_template(user_id, template_code, name):
        bot_uuid = str(uuid.uuid4())
        user_path = os.path.join(Config.BOTS_PATH, str(user_id), bot_uuid)
        os.makedirs(user_path, exist_ok=True)
        file_path = os.path.join(user_path, "bot.py")
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(template_code)
            
        return file_path

class SystemMonitor(threading.Thread):
    def __init__(self):
        super().__init__()
        self.daemon = True
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set():
            try:
                bot_engine.check_health()
                self.check_expirations()
                time.sleep(10)
            except Exception as e:
                sys_logger.error(f"Monitor Loop Error: {e}")
                
    def check_expirations(self):
        pass # Logic to stop bots of expired users

monitor = SystemMonitor()
monitor.start()

bot = telebot.TeleBot(Config.MASTER_TOKEN)

# ==============================================================================
# KEYBOARDS
# ==============================================================================

class Keyboards:
    @staticmethod
    def main(is_admin):
        mk = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        mk.add("🚀 Deploy Bot", "📂 My Bots")
        mk.add("💳 Plans & Gifts", "👤 Profile")
        mk.add("🧩 Templates", "🆘 Support")
        if is_admin:
            mk.add("🔒 Admin Panel")
        return mk

    @staticmethod
    def admin():
        mk = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        mk.add("👥 Users", "🤖 Bots")
        mk.add("📢 Broadcast", "🎫 Create Coupon")
        mk.add("🎁 Approve Gifts", "📜 Audit Logs")
        mk.add("🚫 Ban User", "✅ Unban User")
        mk.add("➕ Add Time", "➖ Remove Time")
        mk.add("⬅️ Back")
        return mk

    @staticmethod
    def plans_menu():
        mk = types.InlineKeyboardMarkup(row_width=1)
        mk.add(types.InlineKeyboardButton("🎁 Gift a Friend", callback_data="gift_start"))
        return mk

    @staticmethod
    def bot_ctrl(uuid, status):
        mk = types.InlineKeyboardMarkup(row_width=2)
        if status == 'running':
            mk.add(types.InlineKeyboardButton("⏹ Stop", callback_data=f"stop_{uuid}"),
                   types.InlineKeyboardButton("🔄 Restart", callback_data=f"restart_{uuid}"))
        else:
            mk.add(types.InlineKeyboardButton("▶ Start", callback_data=f"start_{uuid}"),
                   types.InlineKeyboardButton("🗑 Delete", callback_data=f"delete_{uuid}"))
        
        mk.add(types.InlineKeyboardButton("📜 Logs", callback_data=f"logs_{uuid}"),
               types.InlineKeyboardButton("✏️ Edit Token", callback_data=f"token_{uuid}"))
        mk.add(types.InlineKeyboardButton("🔙 Back", callback_data="list_bots"))
        return mk

    @staticmethod
    def deploy_method():
        mk = types.InlineKeyboardMarkup(row_width=1)
        mk.add(types.InlineKeyboardButton("📤 Upload File", callback_data="deploy_file"),
               types.InlineKeyboardButton("🐙 GitHub Repo", callback_data="deploy_git"))
        return mk

    @staticmethod
    def templates():
        mk = types.InlineKeyboardMarkup(row_width=1)
        mk.add(types.InlineKeyboardButton("🗣 Echo Bot", callback_data="tpl_echo"),
               types.InlineKeyboardButton("🧮 Calculator Bot", callback_data="tpl_calc"))
        return mk

    @staticmethod
    def support():
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("📩 New Ticket", callback_data="new_ticket"))
        return mk
    
    @staticmethod
    def admin_gift_action(gift_id):
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("✅ Approve", callback_data=f"g_app_{gift_id}"),
               types.InlineKeyboardButton("❌ Reject", callback_data=f"g_rej_{gift_id}"))
        return mk

# ==============================================================================
# HELPERS
# ==============================================================================

def check_ban(func):
    def wrapper(message, *args, **kwargs):
        user_id = message.from_user.id
        if db.is_banned(user_id):
            return
        return func(message, *args, **kwargs)
    return wrapper

def check_ban_cb(func):
    def wrapper(call, *args, **kwargs):
        user_id = call.from_user.id
        if db.is_banned(user_id):
            bot.answer_callback_query(call.id, "⛔ Banned")
            return
        return func(call, *args, **kwargs)
    return wrapper

# New Feature: Auto Delete Message Helper
def delete_later(message, delay=10):
    def _delete():
        time.sleep(delay)
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except:
            pass
    threading.Thread(target=_delete, daemon=True).start()

user_steps = {}

# ==============================================================================
# HANDLERS
# ==============================================================================

@bot.message_handler(commands=['start'])
def cmd_start(m):
    user_id = m.from_user.id
    user_steps.pop(user_id, None) # Clear any existing steps
    
    # Auto delete command message
    delete_later(m, delay=5)
    
    if db.is_banned(user_id): return
    
    username = m.from_user.username
    fname = m.from_user.first_name
    
    ref_by = None
    if len(m.text.split()) > 1:
        try:
            ref_code = m.text.split()[1]
            if ref_code != db.get_user(user_id)[8] if db.get_user(user_id) else True:
                ref_by = ref_code
        except: pass
        
    db.add_user(user_id, username, fname, ref_by)
    
    # Refresh Info
    user = db.get_user(user_id)
    plan_expiry = user[5]
    is_admin = Security.is_admin(user_id)
    
    try:
        exp = datetime.datetime.strptime(plan_expiry, "%Y-%m-%d %H:%M:%S.%f")
    except:
        try:
            exp = datetime.datetime.strptime(plan_expiry, "%Y-%m-%d %H:%M:%S")
        except:
            exp = datetime.datetime.now()
            
    status = "Active" if exp > datetime.datetime.now() else "Expired"
    
    text = TextResources.WELCOME.format(name=fname, plan=user[4].upper(), status=status)
    bot.send_message(user_id, text, reply_markup=Keyboards.main(is_admin))

@bot.message_handler(func=lambda m: m.text == "cancel", content_types=['text'])
def cancel_action(m):
    if db.is_banned(m.from_user.id): return
    user_steps.pop(m.from_user.id, None)
    msg = bot.reply_to(m, "🚫 Action Cancelled.", reply_markup=Keyboards.main(Security.is_admin(m.from_user.id)))
    delete_later(m)
    delete_later(msg)

@bot.message_handler(func=lambda m: m.text == "🔒 Admin Panel")
@check_ban
def admin_panel(m):
    user_steps.pop(m.from_user.id, None) # Auto-Clear stuck steps
    if Security.is_admin(m.from_user.id):
        bot.send_message(m.chat.id, TextResources.ADMIN_PANEL, reply_markup=Keyboards.admin())

@bot.message_handler(func=lambda m: m.text == "⬅️ Back")
@check_ban
def back_home(m):
    user_steps.pop(m.from_user.id, None) # Auto-Clear stuck steps
    is_admin = Security.is_admin(m.from_user.id)
    bot.send_message(m.chat.id, "Main Menu", reply_markup=Keyboards.main(is_admin))

@bot.message_handler(func=lambda m: m.text == "🚀 Deploy Bot")
@check_ban
def deploy_start(m):
    user_steps.pop(m.from_user.id, None) # Auto-Clear stuck steps
    user = db.get_user(m.from_user.id)
    # Check plan
    try:
        exp = datetime.datetime.strptime(user[5], "%Y-%m-%d %H:%M:%S.%f")
        if exp < datetime.datetime.now():
            bot.send_message(m.chat.id, TextResources.EXPIRED)
            return
    except: pass

    bot.send_message(m.chat.id, TextResources.DEPLOY_OPTS, reply_markup=Keyboards.deploy_method())

@bot.message_handler(func=lambda m: m.text == "📂 My Bots")
@check_ban
def my_bots(m):
    user_steps.pop(m.from_user.id, None) # Auto-Clear stuck steps
    bots = db.get_bots(m.from_user.id)
    if not bots:
        bot.send_message(m.chat.id, TextResources.NO_BOTS)
        return

    mk = types.InlineKeyboardMarkup(row_width=1)
    for b in bots:
        status = "🟢" if b[6] == "running" else "🔴"
        mk.add(types.InlineKeyboardButton(f"{status} {b[2]}", callback_data=f"manage_{b[0]}"))

    bot.send_message(m.chat.id, "📂 **Your Bots List:**", reply_markup=mk)

@bot.message_handler(func=lambda m: m.text == "💳 Plans & Gifts")
@check_ban
def plans_menu(m):
    user_steps.pop(m.from_user.id, None) # Auto-Clear stuck steps
    bot.send_message(m.chat.id, TextResources.PLANS_HIDDEN, parse_mode="Markdown", reply_markup=Keyboards.plans_menu())

@bot.message_handler(func=lambda m: m.text == "👤 Profile")
@check_ban
def profile_menu(m):
    user_steps.pop(m.from_user.id, None) # Auto-Clear stuck steps
    u = db.get_user(m.from_user.id)
    bots = db.get_bots(m.from_user.id)
    
    text = (
        f"👤 **User Profile**\n\n"
        f"ID: `{u[0]}`\n"
        f"Name: {u[2]}\n"
        f"Joined: {u[3]}\n"
        f"Plan: {u[4].upper()}\n"
        f"Expiry: {u[5]}\n"
        f"Bots: {len(bots)}\n\n"
        f"🔗 **Referral Link:**\n"
        f"https://t.me/{bot.get_me().username}?start={u[8]}\n"
        f"Total Referrals: {u[10]}"
    )

    bot.send_message(m.chat.id, text, parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "🧩 Templates")
@check_ban
def templates_menu(m):
    user_steps.pop(m.from_user.id, None) # Auto-Clear stuck steps
    bot.send_message(m.chat.id, TextResources.TEMPLATES_MENU, reply_markup=Keyboards.templates())

@bot.message_handler(func=lambda m: m.text == "🆘 Support")
@check_ban
def support_menu(m):
    user_steps.pop(m.from_user.id, None) # Auto-Clear stuck steps
    bot.send_message(m.chat.id, TextResources.SUPPORT_MENU, reply_markup=Keyboards.support())

# ==============================================================================
# DEPLOYMENT FLOW
# ==============================================================================

@bot.callback_query_handler(func=lambda c: c.data == "deploy_file")
@check_ban_cb
def deploy_file_cb(c):
    user_steps[c.from_user.id] = "upload_file"
    bot.send_message(c.message.chat.id, TextResources.UPLOAD_GUIDE)
    bot.answer_callback_query(c.id)

@bot.message_handler(content_types=['document'])
@check_ban
def file_handler(m):
    uid = m.from_user.id
    if user_steps.get(uid) != "upload_file": return

    if not m.document.file_name.endswith(".py"):
        msg = bot.reply_to(m, "❌ Only .py files are allowed.")
        delete_later(msg)
        return

    try:
        file_info = bot.get_file(m.document.file_id)
        downloaded = bot.download_file(file_info.file_path)
        
        bot_uuid = str(uuid.uuid4())
        user_path = os.path.join(Config.BOTS_PATH, str(uid), bot_uuid)
        os.makedirs(user_path, exist_ok=True)
        
        save_path = os.path.join(user_path, "bot.py")
        with open(save_path, "wb") as f:
            f.write(downloaded)
            
        valid, msg = Security.validate_file(save_path)
        if not valid:
            bot.reply_to(m, f"❌ Validation Failed:\n{msg}")
            shutil.rmtree(user_path)
            return

        user_steps[uid] = f"token_{bot_uuid}_{m.document.file_name}"
        bot.reply_to(m, TextResources.TOKEN_ASK)
        
    except Exception as e:
        bot.reply_to(m, f"Error: {e}")
        user_steps.pop(uid, None)

@bot.callback_query_handler(func=lambda c: c.data == "deploy_git")
@check_ban_cb
def deploy_git_cb(c):
    user_steps[c.from_user.id] = "git_url"
    bot.send_message(c.message.chat.id, TextResources.GIT_GUIDE)
    bot.answer_callback_query(c.id)

@bot.message_handler(func=lambda m: user_steps.get(m.from_user.id) == "git_url")
@check_ban
def git_url_handler(m):
    url = m.text.strip()
    if "github.com" not in url:
        msg = bot.reply_to(m, "❌ Invalid GitHub URL")
        delete_later(msg)
        return

    uid = m.from_user.id
    bot_uuid = str(uuid.uuid4())
    user_path = os.path.join(Config.BOTS_PATH, str(uid), bot_uuid)
    os.makedirs(user_path, exist_ok=True)

    msg = bot.reply_to(m, "⏳ Cloning repository...")

    try:
        subprocess.check_call(['git', 'clone', url, '.'], cwd=user_path)
        
        entry_file = None
        for f in ['main.py', 'bot.py', 'app.py']:
            if os.path.exists(os.path.join(user_path, f)):
                entry_file = f
                break
        
        if not entry_file:
            py_files = [f for f in os.listdir(user_path) if f.endswith(".py")]
            if len(py_files) == 1:
                entry_file = py_files[0]
            else:
                shutil.rmtree(user_path)
                bot.edit_message_text("❌ Could not detect entry point (main.py/bot.py).", m.chat.id, msg.message_id)
                user_steps.pop(uid, None)
                return
        
        if os.path.exists(os.path.join(user_path, "requirements.txt")):
            bot.edit_message_text("⏳ Installing requirements...", m.chat.id, msg.message_id)
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "--target", user_path])
            
        full_path = os.path.join(user_path, entry_file)
        
        user_steps[uid] = {
            "type": "git_finish",
            "uuid": bot_uuid,
            "path": full_path,
            "name": url.split('/')[-1]
        }
        
        bot.edit_message_text(TextResources.TOKEN_ASK, m.chat.id, msg.message_id)
        
    except Exception as e:
        bot.edit_message_text(f"Error: {e}", m.chat.id, msg.message_id)
        shutil.rmtree(user_path, ignore_errors=True)
        user_steps.pop(uid, None)

@bot.message_handler(func=lambda m: isinstance(user_steps.get(m.from_user.id), dict) or str(user_steps.get(m.from_user.id)).startswith("token_"))
@check_ban
# FIX: Strict check so it doesn't conflict with admin commands
def is_token_step(m):
    step = user_steps.get(m.from_user.id)
    if isinstance(step, str) and step.startswith("token_"): return True
    if isinstance(step, dict) and step.get('type') == 'git_finish': return True
    return False

@bot.message_handler(func=is_token_step)
def token_handler(m):
    uid = m.from_user.id
    step = user_steps.get(uid)
    token = m.text.strip()

    if ":" not in token:
        msg = bot.reply_to(m, "❌ Invalid Token Format")
        delete_later(msg)
        return

    name = ""
    path = ""
    uuid_str = ""
    source = ""

    if isinstance(step, dict) and step['type'] == 'git_finish':
        uuid_str = step['uuid']
        path = step['path']
        name = step['name']
        source = 'git'
    elif isinstance(step, str) and step.startswith("token_"):
        parts = step.split("_")
        uuid_str = parts[1]
        name = parts[2]
        user_path = os.path.join(Config.BOTS_PATH, str(uid), uuid_str)
        path = os.path.join(user_path, "bot.py")
        source = 'file'
    else:
        return
        
    db.exec("""INSERT INTO bots (uuid, owner_id, name, token, source_type, path, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, 'stopped', ?)""",
               (uuid_str, uid, name, token, source, path, datetime.datetime.now()))
               
    user_steps.pop(uid, None)
    bot.reply_to(m, TextResources.SUCCESS_DEPLOY, reply_markup=Keyboards.main(Security.is_admin(uid)))

    name = ""
    path = ""
    uuid_str = ""
    source = ""

    if isinstance(step, dict) and step['type'] == 'git_finish':
        uuid_str = step['uuid']
        path = step['path']
        name = step['name']
        source = 'git'
    elif isinstance(step, str) and step.startswith("token_"):
        parts = step.split("_")
        uuid_str = parts[1]
        name = parts[2]
        user_path = os.path.join(Config.BOTS_PATH, str(uid), uuid_str)
        path = os.path.join(user_path, "bot.py")
        source = 'file'
    else:
        return
        
    db.exec("""INSERT INTO bots (uuid, owner_id, name, token, source_type, path, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, 'stopped', ?)""",
               (uuid_str, uid, name, token, source, path, datetime.datetime.now()))
               
    user_steps.pop(uid, None)
    bot.reply_to(m, TextResources.SUCCESS_DEPLOY, reply_markup=Keyboards.main(Security.is_admin(uid)))

# ==============================================================================
# BOT MANAGEMENT
# ==============================================================================

@bot.callback_query_handler(func=lambda c: c.data.startswith("manage_"))
@check_ban_cb
def manage_bot(c):
    uuid_str = c.data.split("_")[1]
    bot_info = db.get_bot(uuid_str)

    if not bot_info:
        bot.answer_callback_query(c.id, "Bot not found")
        return
        
    text = (
        f"🤖 **Bot Manager**\n\n"
        f"Name: {bot_info[2]}\n"
        f"Status: {bot_info[6].upper()}\n"
        f"Created: {bot_info[8]}\n"
    )

    bot.edit_message_text(text, c.message.chat.id, c.message.message_id, reply_markup=Keyboards.bot_ctrl(uuid_str, bot_info[6]))

@bot.callback_query_handler(func=lambda c: c.data == "list_bots")
@check_ban_cb
def list_bots_back(c):
    bot.delete_message(c.message.chat.id, c.message.message_id)
    my_bots(c.message)

@bot.callback_query_handler(func=lambda c: c.data.startswith(("start_", "stop_", "restart_", "delete_", "logs_")))
@check_ban_cb
def bot_actions(c):
    action, uuid_str = c.data.split("_")

    b = db.get_bot(uuid_str)
    if not b or b[1] != c.from_user.id:
        bot.answer_callback_query(c.id, "Access Denied")
        return

    if action == "start":
        ok, msg = bot_engine.start(uuid_str)
        bot.answer_callback_query(c.id, msg, show_alert=not ok)
        
    elif action == "stop":
        ok, msg = bot_engine.stop(uuid_str)
        bot.answer_callback_query(c.id, msg)
        
    elif action == "restart":
        ok, msg = bot_engine.restart(uuid_str)
        bot.answer_callback_query(c.id, msg)
        
    elif action == "delete":
        bot_engine.stop(uuid_str)
        db.exec("DELETE FROM bots WHERE uuid=?", (uuid_str,))
        path = os.path.join(Config.BOTS_PATH, str(c.from_user.id), uuid_str)
        shutil.rmtree(path, ignore_errors=True)
        bot.answer_callback_query(c.id, "Deleted")
        list_bots_back(c)
        bot.send_message(c.message.chat.id, "Bot Deleted.", reply_markup=Keyboards.main(Security.is_admin(c.from_user.id)))
        return
        
    elif action == "logs":
        err = b[10] if b[10] else "No errors logged."
        bot.send_message(c.message.chat.id, f"📜 **Logs:**\n\n`{err}`", parse_mode="Markdown")
        bot.answer_callback_query(c.id)
        return

    manage_bot(c)

# ==============================================================================
# ADMIN ACTIONS
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == "📢 Broadcast" and Security.is_admin(m.from_user.id))
def broadcast_step1(m):
    user_steps[m.from_user.id] = "broadcast"
    bot.send_message(m.chat.id, TextResources.BROADCAST_ASK)

@bot.message_handler(func=lambda m: user_steps.get(m.from_user.id) == "broadcast")
def broadcast_send(m):
    users = db.exec("SELECT user_id FROM users", fetch_all=True)
    count = 0
    for u in users:
        try:
            bot.send_message(u[0], f"📢 ANNOUNCEMENT\n\n{m.text}", parse_mode="Markdown")
            count += 1
            time.sleep(0.1)
        except: pass

    bot.reply_to(m, TextResources.BROADCAST_DONE.format(count=count))
    user_steps.pop(m.from_user.id, None)

@bot.message_handler(func=lambda m: m.text == "🎫 Create Coupon" and Security.is_admin(m.from_user.id))
def create_coupon_step1(m):
    user_steps.pop(m.from_user.id, None) 
    user_steps[m.from_user.id] = "create_coupon"
    bot.send_message(m.chat.id, "Format: CODE DAYS MAX_USES\nExample: SALE20 30 100")

@bot.message_handler(func=lambda m: user_steps.get(m.from_user.id) == "create_coupon")
def create_coupon_step2(m):
    try:
        parts = m.text.split()
        code, days, limit = parts[0], int(parts[1]), int(parts[2])
        db.exec("INSERT INTO coupons (code, days, max_uses, created_by) VALUES (?, ?, ?, ?)", (code, days, limit, m.from_user.id))
        bot.reply_to(m, f"✅ Coupon Created.\nTo apply: /apply_coupon [USER_ID] {code}")
        db.log_audit("CreateCoupon", m.from_user.id, f"Code: {code}, Days: {days}")
    except Exception as e:
        bot.reply_to(m, f"Error: {e}")
    user_steps.pop(m.from_user.id, None)

@bot.message_handler(commands=['apply_coupon'])
def admin_apply_coupon(m):
    if not Security.is_admin(m.from_user.id): return
    delete_later(m) # Auto delete command
    try:
        parts = m.text.split()
        if len(parts) != 3:
            msg = bot.reply_to(m, "Usage: /apply_coupon [USER_ID] [CODE]")
            delete_later(msg)
            return
        
        target_id = int(parts[1])
        code = parts[2]
        
        coupon = db.exec("SELECT * FROM coupons WHERE code=?", (code,), fetch_one=True)
        if not coupon:
            msg = bot.reply_to(m, "Invalid Code.")
            delete_later(msg)
            return
            
        db.update_user_plan(target_id, coupon[1])
        db.exec("INSERT INTO coupon_usage VALUES (?, ?, ?, ?)", (code, target_id, datetime.datetime.now(), m.from_user.id))
        db.exec("UPDATE coupons SET used_count=used_count+1 WHERE code=?", (code,))
        
        bot.reply_to(m, f"✅ Coupon Applied to {target_id}.")
        try:
            bot.send_message(target_id, f"🎁 Admin applied coupon '{code}'! {coupon[1]} days added.")
        except: pass
        db.log_audit("ApplyCoupon", m.from_user.id, f"Applied {code} to {target_id}")
        
    except Exception as e:
        bot.reply_to(m, f"Error: {e}")

# ==============================================================================
# NEW ADMIN FEATURES: ADD/REMOVE TIME
# ==============================================================================

@bot.message_handler(func=lambda m: m.text == "➕ Add Time" and Security.is_admin(m.from_user.id))
def add_time_step1(m):
    user_steps[m.from_user.id] = "add_time_id"
    bot.send_message(m.chat.id, "Enter User ID to Add Time:")

@bot.message_handler(func=lambda m: user_steps.get(m.from_user.id) == "add_time_id")
def add_time_step2(m):
    try:
        uid = int(m.text)
        user_steps[m.from_user.id] = {"type": "add_time", "uid": uid}
        bot.reply_to(m, "Enter Days to Add:")
    except:
        bot.reply_to(m, "Invalid ID.")
        user_steps.pop(m.from_user.id, None)

@bot.message_handler(func=lambda m: isinstance(user_steps.get(m.from_user.id), dict) and user_steps[m.from_user.id].get("type") == "add_time")
def add_time_step3(m):
    try:
        days = int(m.text)
        uid = user_steps[m.from_user.id]["uid"]
        
        new_exp = db.update_user_plan(uid, days)
        bot.reply_to(m, f"✅ Added {days} days to {uid}.\nNew Expiry: {new_exp}")
        db.log_audit("AddTime", m.from_user.id, f"Added {days} days to {uid}")
        
        try:
            bot.send_message(uid, f"🎉 Admin added {days} days to your plan!")
        except: pass
        
    except Exception as e:
        bot.reply_to(m, f"Error: {e}")
    
    user_steps.pop(m.from_user.id, None)

@bot.message_handler(func=lambda m: m.text == "➖ Remove Time" and Security.is_admin(m.from_user.id))
def rem_time_step1(m):
    user_steps[m.from_user.id] = "rem_time_id"
    bot.send_message(m.chat.id, "Enter User ID to Remove Time:")

@bot.message_handler(func=lambda m: user_steps.get(m.from_user.id) == "rem_time_id")
def rem_time_step2(m):
    try:
        uid = int(m.text)
        user_steps[m.from_user.id] = {"type": "rem_time", "uid": uid}
        bot.reply_to(m, "Enter Days to Remove:")
    except:
        bot.reply_to(m, "Invalid ID.")
        user_steps.pop(m.from_user.id, None)

@bot.message_handler(func=lambda m: isinstance(user_steps.get(m.from_user.id), dict) and user_steps[m.from_user.id].get("type") == "rem_time")
def rem_time_step3(m):
    try:
        days = int(m.text)
        uid = user_steps[m.from_user.id]["uid"]
        
        # Passing negative days to update_user_plan reduces time
        new_exp = db.update_user_plan(uid, -days)
        bot.reply_to(m, f"✅ Removed {days} days from {uid}.\nNew Expiry: {new_exp}")
        db.log_audit("RemoveTime", m.from_user.id, f"Removed {days} days from {uid}")
        
    except Exception as e:
        bot.reply_to(m, f"Error: {e}")
    
    user_steps.pop(m.from_user.id, None)


@bot.message_handler(func=lambda m: m.text == "🎁 Approve Gifts" and Security.is_admin(m.from_user.id))
def list_pending_gifts(m):
    gifts = db.exec("SELECT * FROM gifts WHERE status='pending'", fetch_all=True)
    if not gifts:
        bot.send_message(m.chat.id, "No pending gifts.")
        return
        
    for g in gifts:
        text = f"🎁 **Gift Request #{g[0]}**\nFrom: `{g[1]}`\nTo: `{g[2]}`\nDays: {g[3]}\nDate: {g[5]}"
        bot.send_message(m.chat.id, text, parse_mode="Markdown", reply_markup=Keyboards.admin_gift_action(g[0]))

@bot.callback_query_handler(func=lambda c: c.data.startswith("g_app_") or c.data.startswith("g_rej_"))
def admin_gift_decision(c):
    if not Security.is_admin(c.from_user.id): return
    action, gid = c.data.split("_")[1], c.data.split("_")[2]
    
    gift = db.exec("SELECT * FROM gifts WHERE id=?", (gid,), fetch_one=True)
    if not gift:
        bot.answer_callback_query(c.id, "Gift not found")
        return

    if action == "app":
        db.update_user_plan(gift[2], gift[3])
        db.exec("UPDATE gifts SET status='approved' WHERE id=?", (gid,))
        bot.edit_message_text(f"✅ Gift #{gid} Approved.", c.message.chat.id, c.message.message_id)
        db.log_audit("ApproveGift", c.from_user.id, f"Gift ID {gid} Approved")
        try:
            bot.send_message(gift[1], f"✅ Your gift to {gift[2]} was approved!")
            bot.send_message(gift[2], f"🎁 You received {gift[3]} days gift from {gift[1]}!")
        except: pass
    else:
        db.exec("UPDATE gifts SET status='rejected' WHERE id=?", (gid,))
        bot.edit_message_text(f"❌ Gift #{gid} Rejected.", c.message.chat.id, c.message.message_id)
        db.log_audit("RejectGift", c.from_user.id, f"Gift ID {gid} Rejected")
        try:
            bot.send_message(gift[1], f"❌ Your gift to {gift[2]} was rejected by admin.")
        except: pass

@bot.message_handler(func=lambda m: m.text == "📜 Audit Logs" and Security.is_admin(m.from_user.id))
def show_audit_logs(m):
    logs = db.exec("SELECT * FROM audit_logs ORDER BY id DESC LIMIT 15", fetch_all=True)
    text = "📜 **Recent Audit Logs**\n\n"
    for l in logs:
        text += f"ID:{l[0]} | {l[1]} | {l[4]}\nDetails: {l[3]}\n\n"
    bot.send_message(m.chat.id, text)

@bot.message_handler(func=lambda m: m.text == "👥 Users" and Security.is_admin(m.from_user.id))
def list_users(m):
    cnt = db.exec("SELECT COUNT(*) FROM users", fetch_one=True)[0]
    recent = db.exec("SELECT user_id, first_name FROM users ORDER BY joined_at DESC LIMIT 10", fetch_all=True)
    msg = f"👥 Total Users: {cnt}\n\nLast 10:\n"
    for r in recent:
        msg += f"{r[0]} - {r[1]}\n"
    bot.send_message(m.chat.id, msg, parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "🚫 Ban User" and Security.is_admin(m.from_user.id))
def ban_user_start(m):
    user_steps[m.from_user.id] = "ban_user"
    bot.send_message(m.chat.id, "Enter User ID to Ban:")

@bot.message_handler(func=lambda m: user_steps.get(m.from_user.id) == "ban_user")
def ban_user_exec(m):
    try:
        uid = int(m.text)
        db.exec("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
        bot.reply_to(m, f"✅ User {uid} Banned.")
        db.log_audit("BanUser", m.from_user.id, f"Banned {uid}")
    except:
        bot.reply_to(m, "Invalid ID")
    user_steps.pop(m.from_user.id, None)

@bot.message_handler(func=lambda m: m.text == "✅ Unban User" and Security.is_admin(m.from_user.id))
def unban_user_start(m):
    user_steps[m.from_user.id] = "unban_user"
    bot.send_message(m.chat.id, "Enter User ID to Unban:")

@bot.message_handler(func=lambda m: user_steps.get(m.from_user.id) == "unban_user")
def unban_user_exec(m):
    try:
        uid = int(m.text)
        db.exec("UPDATE users SET is_banned=0 WHERE user_id=?", (uid,))
        bot.reply_to(m, f"✅ User {uid} Unbanned.")
        db.log_audit("UnbanUser", m.from_user.id, f"Unbanned {uid}")
    except:
        bot.reply_to(m, "Invalid ID")
    user_steps.pop(m.from_user.id, None)

# ==============================================================================
# GIFT SYSTEM (User Side)
# ==============================================================================

@bot.callback_query_handler(func=lambda c: c.data == "gift_start")
@check_ban_cb
def gift_step1(c):
    user_steps[c.from_user.id] = "gift_ask_user"
    bot.send_message(c.message.chat.id, TextResources.GIFT_ASK_USER)
    bot.answer_callback_query(c.id)

@bot.message_handler(func=lambda m: user_steps.get(m.from_user.id) == "gift_ask_user")
@check_ban
def gift_step2(m):
    try:
        target_id = int(m.text.strip())
        if not db.get_user(target_id):
            bot.reply_to(m, "❌ User not found in system.")
            return
        user_steps[m.from_user.id] = {"type": "gift", "target": target_id}
        bot.reply_to(m, TextResources.GIFT_ASK_DAYS)
        user_steps[m.from_user.id]["step"] = "days"
    except ValueError:
        bot.reply_to(m, "❌ Invalid ID. Enter numbers only.")

@bot.message_handler(func=lambda m: isinstance(user_steps.get(m.from_user.id), dict) and user_steps[m.from_user.id].get("type") == "gift" and user_steps[m.from_user.id].get("step") == "days")
@check_ban
def gift_step3(m):
    try:
        days = int(m.text.strip())
        if days <= 0: raise ValueError
        
        data = user_steps[m.from_user.id]
        data["days"] = days
        data["step"] = "confirm"
        user_steps[m.from_user.id] = data
        
        bot.reply_to(m, TextResources.GIFT_CONFIRM.format(days=days, uid=data["target"]), parse_mode="Markdown")
    except:
        bot.reply_to(m, "❌ Invalid days.")

@bot.message_handler(func=lambda m: isinstance(user_steps.get(m.from_user.id), dict) and user_steps[m.from_user.id].get("type") == "gift" and user_steps[m.from_user.id].get("step") == "confirm")
@check_ban
def gift_step4(m):
    if m.text.lower() == "yes":
        data = user_steps[m.from_user.id]
        gid = db.create_gift(m.from_user.id, data["target"], data["days"])
        bot.reply_to(m, TextResources.GIFT_PENDING)
        
        # Notify Admin
        for admin in Config.ADMIN_IDS:
            try:
                bot.send_message(admin, f"🎁 **New Gift Request #{gid}**\nFrom: {m.from_user.id}\nTo: {data['target']}\nDays: {data['days']}\n\nCheck /start -> Admin Panel -> Approve Gifts", parse_mode="Markdown")
            except: pass
            
    else:
        bot.reply_to(m, "❌ Gift Cancelled.")
        
    user_steps.pop(m.from_user.id, None)

# ==============================================================================
# TEMPLATE SYSTEM
# ==============================================================================

@bot.callback_query_handler(func=lambda c: c.data.startswith("tpl_"))
@check_ban_cb
def tpl_handler(c):
    tpl_type = c.data.split("_")[1]

    code = ""
    name = ""

    if tpl_type == "echo":
        code = TemplateManager.get_echo_bot()
        name = "EchoBot_Template"
    elif tpl_type == "calc":
        code = TemplateManager.get_calc_bot()
        name = "CalcBot_Template"
    
    user_path = TemplateManager.save_template(c.from_user.id, code, name)
    bot_uuid = os.path.basename(os.path.dirname(user_path))
    user_steps[c.from_user.id] = f"token_{bot_uuid}_{name}"

    bot.send_message(c.message.chat.id, f"✅ **Template Selected: {name}**\n\nCode generated internally.\n\n" + TextResources.TOKEN_ASK, parse_mode="Markdown")
    bot.answer_callback_query(c.id)

# ==============================================================================
# SUPPORT SYSTEM
# ==============================================================================

@bot.callback_query_handler(func=lambda c: c.data == "new_ticket")
@check_ban_cb
def ticket_step1(c):
    user_steps[c.from_user.id] = "ticket_subject"
    bot.send_message(c.message.chat.id, "📝 Please write the subject of your issue.")
    bot.answer_callback_query(c.id)

@bot.message_handler(func=lambda m: user_steps.get(m.from_user.id) == "ticket_subject")
@check_ban
def ticket_create(m):
    tid = db.create_ticket(m.from_user.id, m.text)
    bot.reply_to(m, TextResources.TICKET_CREATED.format(id=tid))
    user_steps.pop(m.from_user.id, None)

    for a in Config.ADMIN_IDS:
        try:
            bot.send_message(a, f"🔔 **New Ticket #{tid}**\nUser: {m.from_user.id}\nSubject: {m.text}")
        except: pass

# ==============================================================================
# MAIN LOOP
# ==============================================================================

def main():
    print("------------------------------------------")
    print(" MASTER BOT ECOSYSTEM STARTED")
    print(" Features: Auto-Del, Plan Manage, Android Fix")
    print(f" Time: {datetime.datetime.now()}")
    print("------------------------------------------")

    while True:
        try:
            bot.infinity_polling(timeout=25, long_polling_timeout=25)
        except Exception as e:
            sys_logger.critical(f"Main Loop Crash: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()