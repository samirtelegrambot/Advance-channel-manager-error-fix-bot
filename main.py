import logging
import json
import os
import re
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Set, List, Optional, Any
from uuid import uuid4
from dotenv import load_dotenv
from filelock import FileLock

# Correct imports from telegram
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineQueryResultArticle,
    InputTextMessageContent,
    CallbackQuery,
    Message,
    Bot,
)
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    InlineQueryHandler,
    ContextTypes,
    filters,
    ConversationHandler,
)

# ==================== Configuration ====================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("advanced_channel_manager.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

BOT_TOKEN: Optional[str] = os.getenv("BOT_TOKEN")
OWNER_ID: Optional[str] = os.getenv("OWNER_ID")

# Validate environment variables
if not BOT_TOKEN:
    logger.error("BOT_TOKEN environment variable is not set")
    raise ValueError("BOT_TOKEN environment variable is not set")
if not OWNER_ID or not OWNER_ID.isdigit():
    logger.error("OWNER_ID environment variable is not set or invalid")
    raise ValueError("OWNER_ID environment variable is not set or invalid")
OWNER_ID: int = int(OWNER_ID)

CONFIG_FILE: str = "channel_manager_pro_config.json"
CONFIG_LOCK: str = "channel_manager_pro_config.lock"
MAX_BATCH_MESSAGES: int = 100
BATCH_EXPIRY_HOURS: int = 6
SCHEDULE_EXPIRY_DAYS: int = 7
POST_DELAY_SECONDS: float = 0.1
MAX_RETRIES: int = 3
MAX_FOOTER_LENGTH: int = 200
TEXT_FILE_SIZE_LIMIT: int = 1024000  # 1MB
TEXT_FILE_DELIMITER: str = "\n\n"
MAX_MESSAGE_LENGTH: int = 4096  # Telegram's message length limit
MAX_CAPTION_LENGTH: int = 1024  # Telegram's caption length limit

EMOJI: Dict[str, str] = {
    "admin": "👑", "channel": "📢", "stats": "📊", "success": "✅", "error": "❌",
    "warning": "⚠️", "info": "ℹ️", "add": "➕", "remove": "🗑️", "list": "📋",
    "clear": "🧹", "post": "📤", "back": "🔙", "select": "☑️", "selected": "✅",
    "cancel": "❌", "time": "⏰", "batch": "📦", "fixed": "🔒", "progress": "🔄",
    "settings": "⚙️", "help": "❓", "search": "🔍", "loading": "⏳", "done": "🎉",
    "schedule": "⏖", "users": "👥", "analytics": "📈", "broadcast": "📢", "meta": "📱",
    "next": "➡️", "prev": "⬅️", "edit": "✏️", "confirm": "✔️"
}

FIXED_CHANNELS: Dict[str, Dict[str, Any]] = {
    "-1002504723776": {
        "name": "Official Announcements",
        "description": "Primary announcement channel",
        "emoji": "📢",
        "protected": True
    },
    "-1002489624380": {
        "name": "Secondary Channel",
        "description": "Backup channel",
        "emoji": "📣",
        "protected": True
    }
}

# Conversation states
ADMIN_MANAGEMENT, CHANNEL_MANAGEMENT, POST_SETTINGS, SCHEDULE_BATCH = range(4)

# ==================== Config Manager ====================
class ConfigManager:
    _instance: Optional['ConfigManager'] = None
    _config: Dict[str, Any] = {}
    _last_loaded: Optional[datetime] = None
    
    def __new__(cls) -> 'ConfigManager':
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._load()
        return cls._instance
    
    def _load(self) -> None:
        lock = FileLock(CONFIG_LOCK)
        try:
            with lock:
                if os.path.exists(CONFIG_FILE):
                    with open(CONFIG_FILE, "r") as f:
                        self._config = json.load(f)
                        self._last_loaded = datetime.now()
                else:
                    self._initialize_default_config()
                    self._save()
        except (IOError, json.JSONDecodeError, PermissionError) as e:
            logger.error(f"Error loading config: {e}", exc_info=True)
            self._initialize_default_config()
            self._save()
    
    def _initialize_default_config(self) -> None:
        self._config = {
            "admins": [str(OWNER_ID)],
            "channels": {},
            "stats": {
                "posts": 0,
                "batches": 0,
                "last_post": None,
                "last_post_channels": []
            },
            "settings": {
                "default_delay": POST_DELAY_SECONDS,
                "max_retries": MAX_RETRIES,
                "notifications": True,
                "footer": ""
            },
            "admin_stats": {},
            "scheduled_posts": {},
            "post_analytics": {}
        }
    
    def _save(self) -> None:
        lock = FileLock(CONFIG_LOCK)
        try:
            with lock:
                with open(CONFIG_FILE, "w") as f:
                    json.dump(self._config, f, indent=4)
                    self._last_loaded = datetime.now()
        except (IOError, PermissionError) as e:
            logger.error(f"Error saving config: {e}", exc_info=True)
    
    def get_config(self) -> Dict[str, Any]:
        if not self._last_loaded or (datetime.now() - self._last_loaded).seconds > 60:
            self._load()
        return self._config
    
    def save_config(self) -> None:
        self._save()
    
    def is_admin(self, user_id: int) -> bool:
        return str(user_id) in self.get_config().get("admins", [])
    
    def get_all_channels(self) -> Dict[str, Dict[str, Any]]:
        config = self.get_config()
        all_ch: Dict[str, Dict[str, Any]] = {}
        
        for cid, data in FIXED_CHANNELS.items():
            all_ch[cid] = {
                "name": f"{data.get('emoji', '📢')} {data['name']}",
                "fixed": True,
                "description": data["description"],
                "stats": config["channels"].get(cid, {}).get("stats", {"post_count": 0})
            }
        
        for cid, data in config.get("channels", {}).items():
            if cid not in FIXED_CHANNELS:
                all_ch[cid] = {
                    "name": f"📢 {data['name'] if isinstance(data, dict) else data}",
                    "fixed": False,
                    "description": data.get("description", "User-added channel") if isinstance(data, dict) else "User-added channel",
                    "stats": data.get("stats", {"post_count": 0}) if isinstance(data, dict) else {"post_count": 0}
                }
        
        return all_ch
    
    def update_stats(self, posts: int = 0, batches: int = 0, channels: Optional[List[str]] = None, admin_id: Optional[str] = None, post_id: Optional[str] = None) -> None:
        config = self.get_config()
        stats = config.setdefault("stats", {})
        stats["posts"] = stats.get("posts", 0) + posts
        stats["batches"] = stats.get("batches", 0) + batches
        stats["last_post"] = datetime.now().isoformat()
        
        if channels:
            stats["last_post_channels"] = channels
            for cid in channels:
                if cid not in config["channels"] and cid not in FIXED_CHANNELS:
                    continue
                if cid not in config["channels"]:
                    config["channels"][cid] = {"name": f"Channel {cid}", "stats": {"post_count": 0}}
                if "stats" not in config["channels"][cid]:
                    config["channels"][cid]["stats"] = {"post_count": 0}
                config["channels"][cid]["stats"]["post_count"] += 1
        
        if admin_id:
            config["admin_stats"][admin_id] = config["admin_stats"].get(admin_id, {"posts": 0, "last_action": None})
            config["admin_stats"][admin_id]["posts"] += posts
            config["admin_stats"][admin_id]["last_action"] = datetime.now().isoformat()
        
        if post_id:
            config["post_analytics"][post_id] = {
                "timestamp": datetime.now().isoformat(),
                "channels": channels or [],
                "post_count": posts,
                "admin_id": admin_id,
                "batch_size": posts // len(channels) if channels else 0
            }
        
        self.save_config()
    
    def _cleanup_expired_jobs(self) -> None:
        config = self.get_config()
        scheduled_posts = config.get("scheduled_posts", {})
        now = datetime.now()
        expired_jobs = []
        
        for job_id, job_data in scheduled_posts.items():
            try:
                job_time = datetime.fromisoformat(job_data["time"])
                if now - job_time > timedelta(days=SCHEDULE_EXPIRY_DAYS):
                    expired_jobs.append(job_id)
            except ValueError:
                expired_jobs.append(job_id)
        
        for job_id in expired_jobs:
            scheduled_posts.pop(job_id)
            logger.info(f"Removed expired scheduled job: {job_id}")
        
        if expired_jobs:
            self.save_config()

config_manager = ConfigManager()

# ==================== Utility Functions ====================
def sanitize_markdown(text: str) -> str:
    """Sanitize text to escape Markdown special characters."""
    special_chars = ['*', '_', '`', '[', ']', '(', ')', '#', '+', '-', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

def style_text(text: str, style: str = None, emoji: str = None, parse_mode: str = "Markdown") -> str:
    styles = {
        "header": "✨ *{}* ✨", "title": "📌 *{}*", "subheader": "• *{}* •",
        "success": f"{EMOJI['success']} *{{}}*", "error": f"{EMOJI['error']} *{{}}*",
        "warning": f"{EMOJI['warning']} *{{}}*", "info": f"{EMOJI['info']} _{{}}_",
        "bold": "*{}*", "italic": "_{}_", "code": "`{}`", "pre": "```\n{}\n```",
        "list": "• {}", "highlight": "⬇️ *{}* ⬇️", "quote": "> {}", "alert": "❗ *{}* ❗"
    }
    
    formatted_text = styles.get(style, "{}").format(sanitize_markdown(text))
    if emoji:
        formatted_text = f"{emoji} {formatted_text}"
    if parse_mode == "HTML":
        formatted_text = formatted_text.replace("*", "<b>").replace("_", "<i>").replace("`", "<code>")
    
    return formatted_text

def format_timestamp(timestamp: Optional[str] = None, relative: bool = False) -> str:
    if not timestamp:
        return "Never"
    try:
        dt = datetime.fromisoformat(timestamp)
        if relative:
            now = datetime.now()
            delta = now - dt
            if delta.days > 365:
                return f"{delta.days // 365} years ago"
            elif delta.days > 30:
                return f"{delta.days // 30} months ago"
            elif delta.days > 0:
                return f"{delta.days} days ago"
            elif delta.seconds > 3600:
                return f"{delta.seconds // 3600} hours ago"
            elif delta.seconds > 60:
                return f"{delta.seconds // 60} minutes ago"
            else:
                return "Just now"
        return dt.strftime("%d %b %Y, %H:%M:%S")
    except ValueError:
        return "Invalid date"

def validate_channel_id(channel_id: str) -> bool:
    return bool(re.match(r"^-100\d{10,}$", channel_id))

def validate_user_id(user_id: str) -> bool:
    return user_id.isdigit() and len(user_id) <= 20

def validate_schedule_time(time_str: str) -> Optional[datetime]:
    try:
        dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M")
        if dt < datetime.now() + timedelta(minutes=5):
            return None
        return dt
    except ValueError:
        return None

def validate_message_content(content: str, is_caption: bool = False) -> bool:
    max_length = MAX_CAPTION_LENGTH if is_caption else MAX_MESSAGE_LENGTH
    return bool(content.strip()) and len(content) <= max_length

def check_schedule_conflict(config: Dict[str, Any], schedule_dt: datetime, channels: Set[str]) -> bool:
    scheduled_posts = config.get("scheduled_posts", {})
    for job_data in scheduled_posts.values():
        try:
            job_time = datetime.fromisoformat(job_data["time"])
            job_channels = set(job_data.get("channels", []))
            if abs((job_time - schedule_dt).total_seconds()) < 300 and job_channels & channels:
                return True
        except ValueError:
            continue
    return False

# ==================== Keyboards ====================
def create_main_menu() -> ReplyKeyboardMarkup:
    keyboard = [
        [f"{EMOJI['admin']} Admin Panel", f"{EMOJI['channel']} Channels"],
        [f"{EMOJI['stats']} Analytics", f"{EMOJI['batch']} Post"],
        [f"{EMOJI['schedule']} Schedules", f"{EMOJI['settings']} Settings"],
        [f"{EMOJI['help']} Help"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def create_admin_management_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(f"{EMOJI['add']} Add Admin", callback_data="admin_add"),
            InlineKeyboardButton(f"{EMOJI['remove']} Remove Admin", callback_data="admin_remove")
        ],
        [
            InlineKeyboardButton(f"{EMOJI['list']} Admins", callback_data="admin_list"),
            InlineKeyboardButton(f"{EMOJI['users']} Admin Stats", callback_data="admin_stats")
        ],
        [
            InlineKeyboardButton(f"{EMOJI['back']} Back", callback_data="back_to_categories")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_channel_management_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(f"{EMOJI['add']} Add Channel", callback_data="channel_add"),
            InlineKeyboardButton(f"{EMOJI['remove']} Remove Channel", callback_data="channel_remove")
        ],
        [
            InlineKeyboardButton(f"{EMOJI['list']} Channels", callback_data="channel_list"),
            InlineKeyboardButton(f"{EMOJI['analytics']} Channel Stats", callback_data="channel_stats")
        ],
        [
            InlineKeyboardButton(f"{EMOJI['back']} Back", callback_data="back_to_categories")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_batch_management_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(f"{EMOJI['clear']} Clear Batch", callback_data="batch_clear"),
            InlineKeyboardButton(f"{EMOJI['list']} Show Batch", callback_data="batch_list")
        ],
        [
            InlineKeyboardButton(f"{EMOJI['post']} Post Options", callback_data="post_options"),
            InlineKeyboardButton(f"{EMOJI['schedule']} Schedule", callback_data="batch_schedule_menu")
        ],
        [
            InlineKeyboardButton(f"{EMOJI['back']} Back", callback_data="back_to_categories")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_schedule_management_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(f"{EMOJI['list']} List Schedules", callback_data="schedule_list"),
            InlineKeyboardButton(f"{EMOJI['remove']} Cancel Schedule", callback_data="schedule_delete")
        ],
        [
            InlineKeyboardButton(f"{EMOJI['edit']} Reschedule", callback_data="schedule_reschedule"),
            InlineKeyboardButton(f"{EMOJI['back']} Back", callback_data="back_to_categories")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_post_settings_keyboard() -> InlineKeyboardMarkup:
    config = config_manager.get_config()
    settings = config.get("settings", {})
    keyboard = [
        [
            InlineKeyboardButton(
                f"⏱️ Delay: {settings.get('default_delay', POST_DELAY_SECONDS)}s",
                callback_data="set_delay"
            ),
            InlineKeyboardButton(
                f"🔄 Retries: {settings.get('max_retries', MAX_RETRIES)}",
                callback_data="set_retries"
            )
        ],
        [
            InlineKeyboardButton(
                f"📝 Footer: {'Set' if settings.get('footer') else 'None'}",
                callback_data="set_footer"
            ),
            InlineKeyboardButton(
                f"🔔 Notifications: {'On' if settings.get('notifications', True) else 'Off'}",
                callback_data="toggle_notifications"
            )
        ],
        [
            InlineKeyboardButton(f"{EMOJI['success']} Save", callback_data="save_settings"),
            InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_settings")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_post_confirmation_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(f"{EMOJI['confirm']} Confirm Post", callback_data="confirm_post"),
            InlineKeyboardButton(f"{EMOJI['edit']} Edit Settings", callback_data="edit_settings")
        ],
        [
            InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_post")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def build_channel_selection_keyboard(selected_channels: Set[str], channels: Dict[str, Dict[str, Any]], page: int = 0, per_page: int = 10) -> tuple[InlineKeyboardMarkup, int]:
    keyboard: List[List[InlineKeyboardButton]] = []
    start_idx = page * per_page
    end_idx = start_idx + per_page
    
    fixed_channels = {cid: data for cid, data in channels.items() if data.get("fixed", False)}
    regular_channels = {cid: data for cid, data in channels.items() if not data.get("fixed", False)}
    
    all_channels = list(fixed_channels.items()) + list(regular_channels.items())
    total_pages = (len(all_channels) + per_page - 1) // per_page
    
    keyboard.append([InlineKeyboardButton(f"{EMOJI['search']} Search Channels", switch_inline_query_current_chat="")])
    
    if fixed_channels and start_idx < len(fixed_channels):
        keyboard.append([InlineKeyboardButton("🔒 Fixed Channels", callback_data="header_fixed_channels")])
        for cid, data in list(fixed_channels.items())[start_idx:min(end_idx, len(fixed_channels))]:
            emoji = EMOJI["selected"] if cid in selected_channels else EMOJI["select"]
            keyboard.append([InlineKeyboardButton(f"{emoji} {data['name']}", callback_data=f"toggle_{cid}")])
    
    if regular_channels and end_idx > len(fixed_channels):
        keyboard.append([InlineKeyboardButton("📢 Your Channels", callback_data="header_regular_channels")])
        regular_start = max(0, start_idx - len(fixed_channels))
        regular_end = min(len(regular_channels), end_idx - len(fixed_channels))
        for cid, data in list(regular_channels.items())[regular_start:regular_end]:
            emoji = EMOJI["selected"] if cid in selected_channels else EMOJI["select"]
            keyboard.append([InlineKeyboardButton(f"{emoji} {data['name']}", callback_data=f"toggle_{cid}")])
    
    navigation: List[InlineKeyboardButton] = []
    if page > 0:
        navigation.append(InlineKeyboardButton(f"{EMOJI['prev']} Previous", callback_data=f"page_{page-1}"))
    if page < total_pages - 1:
        navigation.append(InlineKeyboardButton(f"{EMOJI['next']} Next", callback_data=f"page_{page+1}"))
    if navigation:
        keyboard.append(navigation)
    
    control_buttons: List[InlineKeyboardButton] = []
    if len(selected_channels) < len(channels):
        control_buttons.append(InlineKeyboardButton(f"{EMOJI['select']} Select All", callback_data="select_all"))
    if selected_channels:
        control_buttons.append(InlineKeyboardButton(f"{EMOJI['clear']} Clear Selection", callback_data="unselect_all"))
    if control_buttons:
        keyboard.append(control_buttons)
    
    action_buttons: List[InlineKeyboardButton] = []
    if selected_channels:
        action_buttons.append(InlineKeyboardButton(
            f"{EMOJI['post']} Preview Post to {len(selected_channels)} Channels",
            callback_data="post_selected"
        ))
    action_buttons.append(InlineKeyboardButton(f"{EMOJI['settings']} Settings", callback_data="post_settings"))
    action_buttons.append(InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_post"))
    keyboard.append(action_buttons)
    
    return InlineKeyboardMarkup(keyboard), total_pages

def create_schedule_list_keyboard(scheduled_jobs: Dict[str, Any]) -> InlineKeyboardMarkup:
    keyboard = []
    for job_id, job_data in scheduled_jobs.items():
        time_str = format_timestamp(job_data["time"], relative=False)
        channels = len(job_data.get("channels", []))
        batch_size = job_data.get("batch_size", 0)
        keyboard.append([
            InlineKeyboardButton(
                f"⏖ Job {job_id[:8]}: {time_str} ({batch_size} msgs, {channels} ch)",
                callback_data=f"schedule_view_{job_id}"
            )
        ])
    keyboard.append([
        InlineKeyboardButton(f"{EMOJI['back']} Back", callback_data="back_to_schedule_menu")
    ])
    return InlineKeyboardMarkup(keyboard)

# ==================== Command Handlers ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id: int = update.effective_user.id
    if not config_manager.is_admin(user_id):
        await update.message.reply_text(
            style_text("You are not authorized to use this bot.", "error"),
            reply_markup=ReplyKeyboardRemove(),
            parse_mode="Markdown"
        )
        logger.warning(f"Unauthorized access attempt by user {user_id}")
        return
    
    welcome_text = """
✨ *Welcome to Advanced Channel Manager Pro!* ✨

🚀 *A powerful tool to manage your Telegram channels efficiently*

🔖 *Main Features:*
• 📢 Multi-channel broadcasting
• 📦 Smart message batching
• ⏖ Flexible scheduling with editing
• 📊 Advanced analytics with trends
• 🔒 Robust role-based access control
• 🔍 Channel search with inline query
• 📝 Custom footers with previews
• 📄 Text file parsing
• 📷 Photo, video, and document support
• 📈 Detailed posting summaries

🛠 *How to Use:*
1. Forward messages, send media, or text files
2. Select target channels with search
3. Configure settings with preview
4. Post immediately or schedule with confirmation!

📱 Use the menu below or type /help for commands
"""
    await update.message.reply_text(
        welcome_text,
        reply_markup=create_main_menu(),
        parse_mode="Markdown"
    )
    logger.info(f"User {user_id} started the bot")

async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_text = """
📚 *Advanced Channel Manager Pro Help*

*🔧 Commands:*
• /start - Open main menu
• /help - Display this help
• /cancel - Cancel operation
• /status - View batch and scheduler status

*📖 Usage Guide:*
1. *Add messages* via text, media, or .txt files
2. Use *Batch Manager* to review messages
3. Select channels with inline search
4. Set *Post settings* (delay, retries, footer) with previews
5. Post with confirmation or schedule with rescheduling

*📄 Text Files:*
• Forward .txt file to split by double newlines
• Max size: 1MB
• Each segment becomes a post
*Media:*
• Text, photos, videos, documents supported
• Forward or send directly to batch

*💡 Tips:*
• Use keyboard for quick navigation
• Preview settings before posting
• Schedule during low-traffic
• Monitor analytics
• Use footer for branding
"""
    await update.message.reply_text(
        help_text,
        reply_markup=create_main_menu(),
        parse_mode="Markdown"
    )
    logger.debug(f"Help command issued by user {update.effective_user.id}")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id: int = update.effective_user.id
    context.user_data.clear()
    await update.message.reply_text(
        style_text("All operations canceled.", "success"),
        reply_markup=create_main_menu(),
        parse_mode="Markdown"
    )
    logger.info(f"User {user_id} canceled all operations")
    return ConversationHandler.END

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id: int = update.effective_user.id
    if not config_manager.is_admin(user_id):
        await update.message.reply_text(
            style_text("Permission denied.", "error"),
            reply_markup=create_main_menu(),
            parse_mode="Markdown"
        )
        return
    
    batch = context.user_data.get("batch", [])
    config = config_manager.get_config()
    
    text = f"""
📊 *Bot Status*

*Batch Info:*
• Size: {len(batch)}/{MAX_BATCH_MESSAGES}
• Created: {format_timestamp(context.user_data.get('batch_created'), relative=True)}
• Scheduled: {format_timestamp(context.user_data.get('schedule_time'), relative=False) if 'schedule_time' in context.user_data else 'Not scheduled'}

*Scheduler Info:*
• Active Jobs: {len(config.get('scheduled_posts', {}))}
"""
    if config.get("scheduled_posts"):
        text += "\n*Scheduled Posts:*\n"
        for job_id, job_data in config["scheduled_posts"].items():
            text += f"• Job `{job_id[:8]}...`: {format_timestamp(job_data['time'])} ({job_data.get('batch_size', 0)} messages, {len(job_data.get('channels', []))} channels)\n"
    
    await update.message.reply_text(
        text,
        reply_markup=create_main_menu(),
        parse_mode="Markdown"
    )
    logger.debug(f"Status checked by user {user_id}")

# ==================== Main Menu Handlers ====================
async def handle_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text: str = update.message.text
    user_id: int = update.effective_user.id
    
    if not config_manager.is_admin(user_id):
        await update.message.reply_text(
            style_text("Permission denied.", "error"),
            reply_markup=create_main_menu(),
            parse_mode="Markdown"
        )
        logger.warning(f"Unauthorized menu access attempt by user {user_id}")
        return
    
    if text == f"{EMOJI['admin']} Admin Panel":
        await update.message.reply_text(
            style_text("Admin Management Panel", "header"),
            reply_markup=create_admin_management_keyboard(),
            parse_mode="Markdown"
        )
    elif text == f"{EMOJI['channel']} Channels":
        await update.message.reply_text(
            style_text("Channel Management Panel", "header"),
            reply_markup=create_channel_management_keyboard(),
            parse_mode="Markdown"
        )
    elif text == f"{EMOJI['stats']} Analytics":
        await show_advanced_stats(update, context)
    elif text == f"{EMOJI['batch']} Post":
        await post_batch_menu(update, context)
    elif text == f"{EMOJI['schedule']} Schedules":
        await schedule_management_menu(update, context)
    elif text == f"{EMOJI['settings']} Settings":
        await post_settings(update, context)
    elif text == f"{EMOJI['help']} Help":
        await show_help(update, context)
    logger.debug(f"User {user_id} selected menu option: {text}")

async def show_advanced_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config = config_manager.get_config()
    stats = config.get("stats", {})
    channels = config_manager.get_all_channels()
    post_analytics = config.get("post_analytics", {})
    
    monthly_counts = {}
    for post_id, data in post_analytics.items():
        try:
            post_time = datetime.fromisoformat(data["timestamp"])
            month_key = post_time.strftime("%Y-%m")
            monthly_counts[month_key] = monthly_counts.get(month_key, 0) + data["post_count"]
        except ValueError:
            continue
    
    text = f"""
📊 *Advanced Analytics Dashboard*

*📖 Summary:*
• Total Posts: `{stats.get('posts', 0)}`
• Total Batches: `{stats.get('batches', 0)}`
• Last Post: `{format_timestamp(stats.get('last_post'), relative=True)}`

*📈 Channel Activity:*
"""
    last_channels = stats.get("last_post_channels", [])
    for cid in last_channels:
        if cid in channels:
            post_count = channels[cid]["stats"].get("post_count", 0)
            text += f"• {channels[cid]['name']}: `{post_count}` posts\n"
    
    text += "\n*📅 Monthly Post Counts:*\n"
    for month_key, count in sorted(monthly_counts.items(), reverse=True)[:3]:
        text += f"• {month_key}: `{count}` posts\n"
    
    text += "\n*👥 Admin Activity:*\n"
    for admin_id, admin_data in config.get("admin_stats", {}).items():
        text += f"• Admin `{admin_id[:8]}...`: `{admin_data.get('posts', 0)}` posts\n"
        if admin_data.get("last_action"):
            text += f"  Last Action: {format_timestamp(admin_data['last_action'], relative=True)}\n"
    
    text += "\n*📑 Recent Posts:*\n"
    for i, (pid, data) in enumerate(sorted(post_analytics.items(), key=lambda x: x[1]["timestamp"], reverse=True)[:3], 1):
        text += f"{i}. Post `{pid[:8]}...` at {format_timestamp(data['timestamp'], relative=True)} "
        text += f"({data['post_count']} posts to {len(data['channels'])} channels)\n"
    
    await update.message.reply_text(
        text,
        reply_markup=create_main_menu(),
        parse_mode="Markdown"
    )
    logger.debug(f"Analytics viewed by user {update.effective_user.id}")

# ==================== Batch Management ====================
async def post_batch_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0) -> None:
    batch: List[int] = context.user_data.get("batch", [])
    user_id: int = update.effective_user.id
    
    if not batch:
        await update.message.reply_text(
            style_text("Your batch is empty. Please add messages first.", "warning"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.info(f"User {user_id} attempted to post empty batch")
        return
    
    channels = config_manager.get_all_channels()
    if not channels:
        await update.message.reply_text(
            style_text("No channels available to post to. Add channels first.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.info(f"User {user_id} attempted to post with no channels")
        return
    
    if "selected_channels" not in context.user_data:
        context.user_data["selected_channels"] = set()
    context.user_data["page"] = page
    
    keyboard, total_pages = build_channel_selection_keyboard(
        context.user_data["selected_channels"],
        channels,
        page
    )
    
    await update.message.reply_text(
        f"📤 *Select Channels for Batch Posting*\n\n*Batch Size:* `{len(batch)}` messages | *Page:* `{page+1}/{total_pages}`\nSelect target channels:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    logger.debug(f"User {user_id} opened channel selection (page {page+1})")

async def add_to_batch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id: int = update.effective_user.id
    if not config_manager.is_admin(user_id):
        await update.message.reply_text(
            style_text("Permission denied.", "error"),
            reply_markup=create_main_menu(),
            parse_mode="Markdown"
        )
        logger.warning(f"Unauthorized batch addition attempt by user {user_id}")
        return
    
    if "settings_input" in context.user_data or "channel_input" in context.user_data or "schedule_input" in context.user_data:
        logger.debug(f"User {user_id} is in input mode, ignoring batch addition")
        return
    
    if "batch" not in context.user_data:
        context.user_data["batch"] = []
        context.user_data["batch_created"] = datetime.now().isoformat()
        logger.debug(f"New batch created for user {user_id}")
    
    batch: List[int] = context.user_data["batch"]
    
    if "batch_created" in context.user_data:
        created = datetime.fromisoformat(context.user_data["batch_created"])
        if (datetime.now() - created).total_seconds() > BATCH_EXPIRY_HOURS * 3600:
            context.user_data["batch"] = []
            context.user_data["batch_created"] = datetime.now().isoformat()
            await update.message.reply_text(
                style_text("Your batch has expired. A new batch has been created.", "warning"),
                reply_markup=create_batch_management_keyboard(),
                parse_mode="Markdown"
            )
            batch = context.user_data["batch"]
            logger.info(f"Batch expired for user {user_id}, new batch created")
    
    if len(batch) >= MAX_BATCH_MESSAGES:
        await update.message.reply_text(
            style_text(f"Batch limit reached ({len(batch)}/{MAX_BATCH_MESSAGES}). Post messages or clear batch.", "warning"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.warning(f"User {user_id} reached batch limit")
        return
    
    if update.message.document and update.message.document.mime_type == "text/plain":
        if update.message.document.file_size > TEXT_FILE_SIZE_LIMIT:
            await update.message.reply_text(
                style_text(f"File too large. Max {TEXT_FILE_SIZE_LIMIT // 1024}KB.", "error"),
                reply_markup=create_batch_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.error(f"User {user_id} uploaded file too large: {update.message.document.file_size} bytes")
            return
        
        try:
            file = await update.message.document.get_file()
            content = await file.download_as_bytearray()
            text_content = content.decode("utf-8")
            messages = [
                msg.strip() for msg in text_content.split(TEXT_FILE_DELIMITER)
                if validate_message_content(msg.strip()) and len(msg.strip()) <= MAX_MESSAGE_LENGTH
            ]
            
            if not messages:
                await update.message.reply_text(
                    style_text("No valid messages found in file. Ensure messages are separated by double newlines and are non-empty.", "warning"),
                    reply_markup=create_batch_management_keyboard(),
                    parse_mode="Markdown"
                )
                logger.warning(f"User {user_id} uploaded empty or invalid text file")
                return
            
            added_count = 0
            for msg in messages:
                if len(batch) >= MAX_BATCH_MESSAGES:
                    break
                sent_msg = await context.bot.send_message(
                    chat_id=update.message.chat_id,
                    text=msg[:MAX_MESSAGE_LENGTH],
                    parse_mode="Markdown"
                )
                batch.append(sent_msg.message_id)
                added_count += 1
            
            await update.message.reply_text(
                style_text(f"Added {added_count} messages from file to batch ({len(batch)}/{MAX_BATCH_MESSAGES}).", "success"),
                reply_markup=create_batch_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} added {added_count} messages from text file")
        except (IOError, UnicodeDecodeError) as e:
            logger.error(f"Error processing text file for user {user_id}: {e}", exc_info=True)
            await update.message.reply_text(
                style_text(f"Failed to process file: {str(e)}. Try another file.", "error"),
                reply_markup=create_batch_management_keyboard(),
                parse_mode="Markdown"
            )
            return
    
    if not (update.message.text or update.message.photo or update.message.video or update.message.document):
        await update.message.reply_text(
            style_text("Invalid message content. Messages must be non-empty or valid media.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.warning(f"User {user_id} sent invalid message content")
        return
    
    if update.message.text and not validate_message_content(update.message.text):
        await update.message.reply_text(
            style_text(f"Message is empty or too long. Max {MAX_MESSAGE_LENGTH} characters.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.warning(f"User {user_id} sent invalid text message")
        return
    
    if (update.message.photo or update.message.video) and update.message.caption and not validate_message_content(update.message.caption, is_caption=True):
        await update.message.reply_text(
            style_text(f"Caption is too long. Max {MAX_CAPTION_LENGTH} characters.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.warning(f"User {user_id} sent invalid caption")
        return
    
    batch.append(update.message.message_id)
    msg_type = "text" if update.message.text else \
               "photo" if update.message.photo else \
               "video" if update.message.video else "document"
    await update.message.reply_text(
        style_text(f"Added {msg_type} message to batch ({len(batch)}/{MAX_MESSAGE_COUNT}).", "success"),
        reply_markup=create_batch_management_keyboard(),
        parse_mode="Markdown"
    )
    logger.debug(f"User {user_id} added {msg_type} message ID {update.message.message_id} to batch")

async def show_batch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    batch: List[int] = context.user_data.get("batch", [])
    if not batch:
        await query.message.edit_text(
            style_text("Your batch is empty.", "info"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        logger.info(f"User {query.from_user.id} viewed empty batch")
        return
    
    text = f"""
📦 *Current Batch* ({len(batch)}/{MAX_MESSAGE_COUNT})
"""
    for i, msg_id in enumerate(batch, 1):
        text += f"{i}. Message ID: `{msg_id}`\n"
    if "batch_created" in context.user_data:
        text += f"Created at: `{format_timestamp(context.user_data['batch_created'], relative=True)}`\n"
    if "schedule_time" in context.user_data:
        text += f"Scheduled: `{format_timestamp(context.user_data['schedule_time'])}` at: `{format_timestamp(context.user_data['schedule_time'], relative=False)}`"
    
    await query.message.edit_text(
        text,
        reply_markup=create_batch_management_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()
    logger.info(f"User {query.from_user.id} viewed batch with {len(batch)} messages")

async def clear_batch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    context.user_data["batch"] = []
    context.user_data.pop("batch_created", None)
    context.user_data.pop("schedule_time", None)
    await query.message.edit_text(
        style_text("Batch cleared successfully.", "success"),
        reply_markup=create_batch_management_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()
    logger.info(f"User {query.from_user.id} cleared batch")

# ==================== Schedule Management ====================
async def schedule_management_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config = config_manager.get_config()
    scheduled_jobs = config.get("scheduled_posts", {})
    
    text = f"""
⏖ *Schedule Management Dashboard*

Active scheduled jobs: `{len(scheduled_jobs)}`
"""
    if scheduled_jobs:
        text += "\n*Recent Scheduled Jobs:*\n"
        for i, (job_id, job_data) in enumerate(sorted(scheduled_jobs.items(), key=lambda x: x[1]["time"])[:3], 1):
            text += f"{i}. Job `{job_id[:8]}...`: {format_timestamp(job_data['time'], relative=True)} "
            text += f"({job_data.get('batch_size', 0)} messages, {len(job_data.get('channels', []))} channels)\n"
    
    await update.message.reply_text(
        text,
        reply_markup=create_schedule_management_keyboard(),
        parse_mode="Markdown"
    )
    logger.debug(f"Schedule dashboard accessed by user {update.effective_user.id}")

async def list_schedules(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    config = config_manager.get_config()
    scheduled_jobs = config.get("scheduled_posts", {})
    
    if not scheduled_jobs:
        await query.message.edit_text(
            style_text("No scheduled jobs found.", "info"),
            reply_markup=create_schedule_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        logger.info(f"User {query.from_user.id} viewed empty schedule")
        return
    
    text = "⏖ *Scheduled Jobs*\n\n"
    await query.message.edit_text(
        text,
        reply_markup=create_schedule_list_keyboard(scheduled_jobs),
        parse_mode="Markdown"
    )
    await query.answer()
    logger.debug(f"User {query.from_user.id} listed schedules")

async def view_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    job_id = query.data.split("_")[2]
    config = config_manager.get_config()
    job_data = config.get("scheduled_posts", {}).get(job_id)
    
    if not job_data:
        await query.message.edit_text(
            style_text("Scheduled job not found.", "error"),
            reply_markup=create_schedule_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        logger.warning(f"User {query.from_user.id} attempted to view invalid job {job_id}")
        return
    
    channels = config_manager.get_all_channels()
    text = f"""
⏖ *Scheduled Job Details: {job_id[:8]}...*

• Time: `{format_timestamp(job_data['time'])}`
• Batch Size: `{job_data.get('batch_size', 0)}` messages
• Channels: `{len(job_data.get('channels', []))}`
"""
    for i, cid in enumerate(job_data.get("channels", []), 1):
        if cid in channels:
            text += f"{i}. {channels[cid]['name']}\n"
    
    keyboard = [
    [
        InlineKeyboardButton(f"{EMOJI['edit']} Reschedule", callback_data=f"reschedule_{job_id}"),
        InlineKeyboardButton(f"{EMOJI['remove']} Cancel Job", callback_data=f"delete_job_{job_id}")
    ],
    ],
        [
            InlineKeyboardButton(f"{EMOJI['back']} Back", callback_data="back_to_schedule_list")
        ]
    
    await query.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )
    await query.answer()
    logger.debug(f"User {query.from_user.id} viewed schedule job {job_id}")

async def schedule_batch_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    batch: List[int] = context.user_data.get("batch", [])
    if not batch:
        await query.message.edit_text(
            style_text("Your batch is empty. Add messages first.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        logger.warning(f"User {query.from_user.id} attempted to schedule without batch")
        return None
    
    await query.message.edit_text(
        style_text("Enter schedule time (YYYY-MM-DD HH:MM), e.g., 2025-01-01 14:00", "info"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_schedule")]
        ]),
        parse_mode="Markdown"
    )
    context.user_data["schedule_input"] = "schedule_batch"
    await query.answer()
    return SCHEDULE_BATCH

async def schedule_batch_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id: int = update.effective_user.id
    text: str = update.message.text.strip()
    schedule_dt = validate_schedule_time(text)
    
    if not schedule_dt:
        await update.message.reply_text(
            style_text("Invalid or past date format. Use YYYY-MM-DD HH:MM.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.warning(f"User {user_id} provided invalid schedule time: {text}")
        context.user_data.pop("schedule_input", None)
        return ConversationHandler.END
    
    batch = context.user_data.get("batch", [])
    selected_channels = context.user_data.get("selected_channels", set())
    config
    if not batch or not selected_channels:
        await update.message.reply_text(
            style_text("No messages or no channels selected. Start over.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.warning(f"User {user_id} attempted to schedule without batch or channels")
        context.user_data.pop("schedule_input", None),
        return ConversationHandler.END
    
    if check_schedule_conflict(config, schedule_dt, selected_channels)):
        await update.message.reply_text(
            style_text("Schedule conflict detected. Choose a different time.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.warning(f"User {user_id} attempted to schedule with conflict at {text}")
        return SCHEDULE_BATCH
    
    job_id = str(uuid.uuid4())
    config["scheduled_jobs]["posts"][job_id] = {
        "time": schedule_dt.isoformat(),
        "batch_ids": len(batch),
        "batch_size": int,
        "channels": list(selected_channels),
        "user_id": user_id
    }
    config_manager.save_config()
    
    context.user_data["schedule_time"] = schedule_dt.isoformat()
    
    await update.message.reply_text(
        style_text(f"Batch scheduled successfully for {text} to {len(selected_channels)} channels.", "success"),
        reply_markup=create_batch_management_keyboard(),
        parse_mode="Markdown"
    )
    logger.info(f"User {user_id} scheduled batch job {job_id} at {text}")
    
    context.user_data.pop("schedule_input", None)
    return ConversationHandler.END

# ==================== Posting Functionality ====================
async def preview_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    batch: List[int] = context.user_data.get("batch", []))
    selected_channels: Set[str] = context.user_data.get("selected_channels", set())
    config = config_manager.get_config()
    settings = config.get("settings", {})
    
    if not batch or not selected_channels:
        await query.message.edit_text(
            style_text("No messages or channels selected. Add messages and select channels.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        else await query.answer()
        else logger.warning(f"User {query.from_user.id} from {user} attempted to preview without batch or channels")
        return
    
    delay = settings.get("default_delay", POST_DELAY_SECONDS)
    retries = settings.get("max_retries", MAX_RETRIES)
    footer = settings.get("footer", "")
    
    text = f"""
🎥 *Post Preview and Confirmation*

*Batch Details:*
• Messages: `{len(batch)}`
• Channels: `{len(selected_channels)}`
• Estimated Time: `{len(batch) * len(selected_channels) * delay:.1f}s`

*Settings:*
• Delay: `{delay}s` per post
• Retries: `{retries}` per post
• Footer: `{footer[:50]}{'...' if len(footer) > 50 else ''}`

*Target Channels:*
"""
    channels = config_manager.get_all_channels()
    for i, cid in enumerate(selected_channels, 1):
        text += f"{i}. {channels[cid]['name']}\n"
    
    await query.message.edit_text(
        text,
        reply_markup=create_post_confirmation_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()
    logger.debug(f"User {query.from_user.id} viewed post preview")

async def execute_post(update: Update, context: ContextTypes.DEFAULT_TYPE, is_scheduled: bool = False, job_id: Optional[str] = None) -> bool:
    query = update.callback_query
    batch_ids: List[int] = context.user_data.get("batch", [])[:]
    selected_channels: Set[str] = context.user_data.get("selected_channels", set()).copy()
    user_id: int = query.from_user.id
    chat_id: int = query.message.chat_id
    
    if not batch_ids or not selected_channels:
        await query.message.edit_text(
            style_text("No messages or channels selected for posting.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        logger.warning(f"User {user_id} attempted to post without batch or channels")
        return False
    
    config = config_manager.get_config()
    settings = config.get("settings", {})
    delay = settings.get("default_delay", POST_DELAY_SECONDS)
    max_retries = settings.get("max_retries", MAX_RETRIES)
    footer = settings.get("footer", "")
    
    await query.message.edit_text(
        style_text(f"Starting {'scheduled' if is_scheduled else ''} post...", "info"),
        parse_mode="Markdown"
    )
    await query.answer()
    
    success_count = 0
    failed_posts: Dict[str, List[Dict[str, Any]]] = {}
    total_operations = len(batch_ids) * len(selected_channels)
    completed_operations = 0
    post_id = str(uuid4())
    
    progress_message: Optional[Message] = None
    
    try:
        await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
        progress_text = f"""
🔄 *{'Scheduled' if is_scheduled else ''} Post Progress*

• Completed: `0/{total_operations}`
• Posted: `0`
• Failed: `0`
• Remaining: `{total_operations}`
```
Progress: [          0%]
```
"""
        progress_message = await context.bot.send_message(
            chat_id=chat_id,
            text=progress_text,
            parse_mode="Markdown"
        )
        
        update_interval = max(1, total_operations // 10)  # Update every 10%
        for batch_id in batch_ids:
            for ch_id in selected_channels:
                if not validate_channel_id(ch_id):
                    failed_posts.setdefault(ch_id, []).append({"error": "Invalid channel ID", "message_id": batch_id})
                    logger.warning(f"Invalid channel ID {ch_id} for message {batch_id}")
                    continue
                
                for attempt in range(max_retries + 1):
                    try:
                        await context.bot.send_chat_action(
                            chat_id=int(ch_id),
                            action=ChatAction.UPLOAD_DOCUMENT
                        )
                        copied_msg = await context.bot.copy_message(
                            chat_id=int(ch_id),
                            from_chat_id=chat_id,
                            message_id=batch_id
                        )
                        if footer:
                            await context.bot.send_message(
                                chat_id=int(ch_id),
                                text=footer,
                                reply_to_message_id=copied_msg.message_id,
                                parse_mode="Markdown"
                            )
                        success_count += 1
                        break
                    except Exception as e:
                        if attempt == max_retries:
                            failed_posts.setdefault(ch_id, []).append({"error": str(e)[:500], "message_id": batch_id})
                            logger.warning(f"Failed to post message {batch_id} to {ch_id} after {max_retries} attempts: {e}")
                        await asyncio.sleep(1)
                
                completed_operations += 1
                percent = min(100.0, (completed_operations / total_operations) * 100)
                progress_bar = "█" * int(percent // 10) + " " * (10 - int(total_operations // 10))
                
                if completed_operations % update_interval == 0 or completed_operations == total_operations:
                    progress_text = f"""
🔄 *{'Scheduled' if is_scheduled else ''} Post Progress*

• Completed: `{completed_operations}/{total_operations}`
• Posted: `{success_count}`
• Failed: `{sum(len(errors) for errors in failed_posts.values())}`
• Remaining: `{total_operations - completed_operations}`
```
Progress: [{progress_bar} {percent:.1f}%]
```
"""
                    try:
                        await context.bot.edit_message_text(
                            chat_id=progress_message.chat_id,
                            message_id=progress_message.message_id,
                            text=progress_text,
                            parse_mode="Markdown"
                        )
                    except Exception as e:
                        logger.warning(f"Failed to update progress message: {e}", exc_info=True)
                
                await asyncio.sleep(delay)
        
        config_manager.update_stats(
            posts=success_count,
            batches=1,
            channels=list(selected_channels),
            admin_id=str(user_id),
            post_id=post_id
        )
        
        summary_text = f"""
🎉 *{'Scheduled' if is_scheduled else ''} Posting Summary*

• Post ID: `{post_id[:8]}...`
• Total Messages: `{len(batch_ids)}`
• Total Channels: `{len(selected_channels)}`
• Successful Posts: `{success_count}`
• Failed Posts: `{sum(len(errors) for errors in failed_posts.values())}`
"""
        if failed_posts:
            summary_text += "\n*❌ Failed Posts:*"
            channels = config_manager.get_all_channels()
            for ch_id, errors in failed_posts.items():
                summary_text += f"• {channels.get(ch_id, {'name': ch_id})['name']}:"
                for error in errors:
                    summary_text += f"  - Msg ID `{error['message_id']}`: `{error['error'][:50]}...`\n"
        
        context.user_data["batch"] = []
        context.user_data["selected_channels"] = set()
        context.user_data.pop("batch_created", None)
        context.user_data.pop("schedule_time", None)
        context.user_data.pop("page", None)
        
        if is_scheduled and job_id:
            config["scheduled_posts"].pop(job_id, None)
            config_manager.save_config()
            logger.info(f"Completed scheduled job {job_id}")
        
        if settings.get("notifications", True):
            await context.bot.send_message(
                chat_id=chat_id,
                text=summary_text,
                reply_markup=create_main_menu(),
                parse_mode="Markdown"
            )
        
        if progress_message:
            try:
                await context.bot.delete_message(
                    chat_id=progress_message.chat_id,
                    message_id=progress_message.message_id
                )
            except Exception as e:
                logger.warning(f"Failed to delete progress message: {e}", exc_info=True)
        
        return True
    
    except Exception as e:
        logger.error(f"Posting error for user {user_id}: {e}", exc_info=True)
        await context.bot.send_message(
            chat_id=chat_id,
            text=style_text(f"Posting failed: {str(e)[:500]}. Please try again.", "error"),
            reply_markup=create_main_menu(),
            parse_mode="Markdown"
        )
        return False

# ==================== Admin Management ====================
async def list_admins(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    config = config_manager.get_config()
    admins = config.get("admins", [])
    
    if not admins:
        await query.message.edit_text(
            style_text("No admins found.", "info"),
            reply_markup=create_admin_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        logger.info(f"User {query.from_user.id} viewed empty admin list")
        return True
    
    text = "👑 *Admin List*\n\n"
    for i, admin_id in enumerate(admins, 1):
        text += f"{i}. `{admin_id}`\n"
    
    await query.message.edit_text(
        text,
        reply_markup=create_admin_management_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()
    logger.debug(f"User {query.from_user.id} viewed admin list")

async def show_admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    config = config_manager.get_config()
    admin_stats = config.get("admin_stats", {})
    
    text = "👑 *Admin Statistics*\n\n"
    if not admin_stats:
        text += "No admin statistics available."
    else:
        for admin_id, stats in admin_stats.items():
            text += f"• Admin `{admin_id[:8]}...`: `{stats.get('posts', 0)}` posts\n"
            if stats.get("last_action"):
                text += f"  Last Action: {format_timestamp(stats['last_action'], relative=True)}\n"
            else:
                text += f"\n"
    
    await query.message.edit_text(
        text,
        reply_markup=create_admin_management_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()
    logger.debug(f"User {query.from_user.id} viewed admin stats")

async def add_admin_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.message.edit_text(
        style_text("Enter the User ID to add as admin.", "info"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_admin")]
        ]),
        parse_mode="Markdown"
    )
    context.user_data["settings_input"] = "add_admin"
    await query.answer()
    logger.debug(f"User {query.from_user.id} prompted to add admin")

async def remove_admin_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.message.edit_text(
        style_text("Enter the User ID to remove from admins.", "info"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_admin")]
        ])),
        parse_mode="Markdown"
    )
    context.user_data["settings_input"] = "delete_admin"
    await query.answer()
    logger.debug(f"User {query.from_user.id} prompted to remove admin")

# ==================== Channel Management ====================
async def list_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    channels = config_manager.get_all_channels()
    
    if not channels:
        await query.message.edit_text(
            style_text("No channels available.", "info"),
            reply_markup=create_channel_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        logger.info(f"User {query.from_user.id} viewed empty channel list")
        return
    
    text = "📢 *Channel List*\n\n"
    text += "*🔒 Fixed Channels*\n"
    for i, (cid, data) in enumerate(FIXED_CHANNELS.items(), 1):
        text += f"{i}. `{cid}`: {data['emoji']} {data['name']}\n"
        text += f"  _{data['description']}_\n"
        text += f"  Posts: `{channels[cid]['stats']['post_count']}`\n\n"
    
    config = config_manager.get_config()
    if config.get("channels"):
        text += "*📖 Your Channels*\n"
        for i, (cid, data) in enumerate(config["channels"].items(), len(FIXED_CHANNELS) + 1):
            if cid not in FIXED_CHANNELS:
                text += f"{i}. `{cid}`: `{data['name']}`\n"
                text += f"  Posts: `{channels[cid]['stats']['post_count']}`\n\n"
    
    await query.message.edit_text(
        text,
        reply_markup=create_channel_management_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()
    logger.debug(f"User {query.from_user.id} viewed channel list")

async def show_channel_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    channels = config_manager.get_all_channels()
    
    text = "📊 *Channel Statistics*\n\n"
    for i, (cid, data) in enumerate(channels.items(), 1):
        post_count = data["stats"].get("post_count", 0)
        text += f"{i}. `{data['name']}` (`{cid}`):\n"
        text += f"  Posts: `{post_count}`\n"
        text += f"  Type: `{'Fixed' if data.get('fixed', False) else 'User-added'}`\n"
        text += f"  Description: `{data.get('description', 'No description')}`\n\n"
    
    await query.message.edit_text(
        text,
        reply_markup=create_channel_management_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()
    logger.debug(f"User {query.from_user.id} viewed channel stats")

async def add_channel_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.message.edit_text(
        style_text("Enter channel ID and name (e.g., -1001234567890 Channel Name).", "info"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_channel")]
        ]),
        parse_mode="Markdown"
    )
    context.user_data["channel_input"] = "add_channel"
    await query.answer()
    logger.debug(f"User {query.from_user.id} prompted to add channel")

async def remove_channel_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.message.edit_text(
        style_text("Enter the channel ID to remove.", "info"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_channel")]
        ])),
        parse_mode="Markdown"
    )
    context.user_data["channel_input"] = "delete_channel"
    await query.answer()
    logger.debug(f"User {query.from_user.id} prompted to remove channel")

# ==================== Post Settings ====================
async def post_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        style_text("Configure Posting Settings", "header"),
        reply_markup=create_post_settings_keyboard(),
        parse_mode="Markdown"
    )
    logger.debug(f"User {update.effective_user.id} opened settings")

async def set_delay_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.message.edit_text(
        style_text("Enter post delay in seconds (0-5).", "info"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_settings")]
        ])),
        parse_mode="Markdown"
    )
    context.user_data["settings_input"] = "set_delay"
    await query.answer()
    logger.debug(f"User {query.from_user.id} prompted to set delay")

async def set_retries_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.message.edit_text(
        style_text("Enter max retries (1-10).", "info"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_settings")]
        ])),
        parse_mode="Markdown"
    )
    context.user_data["settings_input"] = "set_retries"
    await query.answer()
    logger.debug(f"User {query.from_user.id} prompted to set retries")

async def set_footer_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.message.edit_text(
        style_text(f"Enter footer text (max {MAX_FOOTER_LENGTH} chars) or 'clear' to remove.", "info"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_settings")]
        ])),
        parse_mode="Markdown"
    )
    context.user_data["settings_input"] = "set_footer"
    await query.answer()
    logger.debug(f"User {query.from_user.id} prompted to set footer")

async def toggle_notifications(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    config = config_manager.get_config()
    config["settings"]["notifications"] = not config["settings"].get("notifications", True)
    config_manager.save_config()
    
    await query.message.edit_text(
        style_text(f"Notifications {'enabled' if config['settings']['notifications'] else 'disabled'}.", "success"),
        reply_markup=create_post_settings_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()
    logger.info(f"User {query.from_user.id} toggled notifications to {config['settings']['notifications']}")

async def save_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.message.edit_text(
        style_text("Settings saved successfully.", "success"),
        reply_markup=create_main_menu(),
        parse_mode="Markdown"
    )
    await query.answer()
    context.user_data.pop("settings_input", None)
    logger.info(f"User {query.from_user.id} saved settings")

async def cancel_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.message.edit_text(
        style_text("Settings update canceled.", "info"),
        reply_markup=create_main_menu(),
        parse_mode="Markdown"
    )
    await query.answer()
    context.user_data.pop("settings_input", None)
    logger.info(f"User {query.from_user.id} canceled settings update")

# ==================== Search Handler ====================

async def inline_search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query: str = update.inline_query.query.lower()
    user_id: int = update.inline_query.from_user.id
    
    if not config_manager.is_admin(user_id):
        logger.warning(f"Unauthorized inline query attempt by user {user_id}")
        return
    
    channels = config_manager.get_all_channels()
    results = []
    
    for i, (cid, data) in enumerate(channels.items(), 1):
        if not query or query.lower() in data["name"].lower():
            results.append(
                InlineQueryResultArticle(
                    id=str(uuid4()),
                    title=data["name"],
                    description=data["description"],
                    input_message_content=InputTextMessageContent(
                        f"Selected channel: {data['name']} (`{cid}`)"
                    ),
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(f"{EMOJI['channel']} Select Channel", callback_data=f"toggle_channel_{cid}")]
                    ])
                )
            )
    
    await update.inline_query.answer(results[:50])
    logger.debug(f"Inline search by user {user_id} for query: {query}")

# ==================== Button Handlers ====================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id
    
    if not config_manager.is_admin(user_id):
        await query.message.edit_text(
            style_text("Permission denied.", "error"),
            reply_markup=create_main_menu(),
            parse_mode="Markdown"
        )
        await query.answer()
        logger.warning(f"Unauthorized button query by user {user_id}")
        return
    
    if data == "back_to_categories":
        await query.message.edit_text(
            style_text("Select an option:", "header"),
            reply_markup=create_main_menu(),
            parse_mode="Markdown"
        )
        await query.answer()
        logger.debug(f"User {user_id} returned to main menu")
        return
    
    # Admin Management
    if data == "admin_list":
        await list_admins(update, context)
    elif data == "admin_stats":
        await show_admin_stats(update, context)
    elif data == "admin_add":
        await add_admin_prompt(update, context)
    elif data == "admin_remove":
        await remove_admin_prompt(update, context)
    elif data == "cancel_admin":
        await query.message.edit_text(
            style_text("Admin operation canceled.", "info"),
            reply_markup=create_admin_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        context.user_data.pop("settings_input", None)
        logger.debug(f"User {user_id} canceled admin operation")
    
    # Channel Management
    elif data == "channel_list":
        await list_channels(update, context)
    elif data == "channel_stats":
        await show_channel_stats(update, context)
    elif data == "channel_add":
        await add_channel_prompt(update, context)
    elif data == "channel_remove":
        await remove_channel_prompt(update, context)
    elif data == "cancel_channel":
        await query.message.edit_text(
            style_text("Channel operation canceled.", "info"),
            reply_markup=create_channel_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        context.user_data.pop("channel_input", None)
        logger.debug(f"User {user_id} canceled channel operation")

    # Batch Management
    elif data == "batch_clear":
        await clear_batch(update, context)
    elif data == "batch_list":
        await show_batch(update, context)
    elif data == "batch_schedule_menu":
        await schedule_batch_menu(update, context)
    elif data == "post_options":
        await post_batch_menu(update, context)
    elif data.startswith("page_"):
        page = int(data.split("_")[1])
        await post_batch_menu(update, context, page=page)
    elif data.startswith("toggle_"):
        channel_id = data.split("_")[1]
        if "selected_channels" not in context.user_data:
            context.user_data["selected_channels"] = set()
        if channel_id in context.user_data["selected_channels"]:
            context.user_data["selected_channels"].remove(channel_id)
        else:
            context.user_data["selected_channels"].add(channel_id)
        page = context.user_data.get("page", 0)
        await post_batch_menu(update, context, page=page)
    elif data == "select_all":
        context.user_data["selected_channels"] = set(config_manager.get_all_channels().keys())
        page = context.user_data.get("page", 0)
        await post_batch_menu(update, context, page=page)
    elif data == "unselect_all":
        context.user_data["selected_channels"] = set()
        page = context.user_data.get("page", 0)
        await post_batch_menu(update, context, page=page)
    elif data == "post_selected":
        await preview_post(update, context)

    # Schedule Management
    elif data == "schedule_list":
        await list_schedules(update, context)
    elif data.startswith("schedule_view_"):
        job_id = data.split("_")[2]
        config = config_manager.get_config()
        if job_id not in config.get("scheduled_posts", {}):
            await query.message.edit_text(
                style_text("Scheduled job not found.", "error"),
                reply_markup=create_schedule_management_keyboard(),
                parse_mode="Markdown"
            )
            await query.answer()
            logger.warning(f"User {user_id} attempted to view invalid job {job_id}")
            return
        job_data = config["scheduled_posts"][job_id]
        channels = config_manager.get_all_channels()
        text = f"""
⏖ *Scheduled Job Details: {job_id[:8]}...*

• Time: `{format_timestamp(job_data['time'])}`
• Batch Size: `{job_data.get('batch_size', 0)}` messages
• Channels: `{len(job_data.get('channels', []))}`
"""
        for i, cid in enumerate(job_data.get("channels", []), 1):
            if cid in channels:
                text += f"{i}. {channels[cid]['name']}\n"
        keyboard = [
            [
                InlineKeyboardButton(f"{EMOJI['edit']} Reschedule", callback_data=f"reschedule_{job_id}"),
                InlineKeyboardButton(f"{EMOJI['remove']} Cancel Job", callback_data=f"schedule_delete_{job_id}")
            ],
            [InlineKeyboardButton(f"{EMOJI['back']} Back", callback_data="back_to_schedule_menu")]
        ]
        await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        await query.answer()
    elif data == "back_to_schedule_menu":
        await query.message.edit_text(
            style_text("Schedule Management Dashboard", "header"),
            reply_markup=create_schedule_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
    elif data.startswith("schedule_delete_"):
        job_id = data.split("_")[2]
        config = config_manager.get_config()
        if job_id in config.get("scheduled_posts", {}):
            config["scheduled_posts"].pop(job_id)
            config_manager.save_config()
            await query.message.edit_text(
                style_text(f"Scheduled job {job_id[:8]} canceled.", "success"),
                reply_markup=create_schedule_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} canceled scheduled job {job_id}")
        else:
            await query.message.edit_text(
                style_text("Scheduled job not found.", "error"),
                reply_markup=create_schedule_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.warning(f"User {user_id} attempted to cancel invalid job {job_id}")
        await query.answer()
    elif data.startswith("reschedule_"):
        job_id = data.split("_")[1]
        context.user_data["reschedule_job_id"] = job_id
        await query.message.edit_text(
            style_text("Enter new schedule time (YYYY-MM-DD HH:MM).", "info"),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_schedule")]
            ]),
            parse_mode="Markdown"
        )
        context.user_data["schedule_input"] = "reschedule_batch"
        await query.answer()
    elif data == "cancel_schedule":
        await query.message.edit_text(
            style_text("Schedule operation canceled.", "info"),
            reply_markup=create_schedule_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        context.user_data.pop("schedule_input", None)
        context.user_data.pop("reschedule_job_id", None)
        logger.debug(f"User {user_id} canceled schedule operation")

    # Post Execution
    elif data == "confirm_post":
        success = await execute_post(update, context)
        if not success:
            await query.message.edit_text(
                style_text("Posting failed. Check logs or try again.", "error"),
                reply_markup=create_main_menu(),
                parse_mode="Markdown"
            )
        await query.answer()
        logger.info(f"User {user_id} confirmed post execution")
    elif data == "edit_settings":
        await query.message.edit_text(
            style_text("Configure Posting Settings", "header"),
            reply_markup=create_post_settings_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
    elif data == "cancel_post":
        context.user_data["selected_channels"] = set()
        context.user_data.pop("page", None)
        await query.message.edit_text(
            style_text("Posting canceled.", "info"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        logger.info(f"User {user_id} canceled post")

    # Settings Management
    elif data == "post_settings":
        await query.message.edit_text(
            style_text("Configure Posting Settings", "header"),
            reply_markup=create_post_settings_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
    elif data == "set_delay":
        await set_delay_prompt(update, context)
    elif data == "set_retries":
        await set_retries_prompt(update, context)
    elif data == "set_footer":
        await set_footer_prompt(update, context)
    elif data == "toggle_notifications":
        await toggle_notifications(update, context)
    elif data == "save_settings":
        await save_settings(update, context)
    elif data == "cancel_settings":
        await cancel_settings(update, context)

# ==================== Input Handlers ====================
async def handle_settings_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id: int = update.effective_user.id
    text: str = update.message.text.strip()
    settings_input: Optional[str] = context.user_data.get("settings_input")

    if not settings_input:
        logger.debug(f"User {user_id} sent input without active settings prompt")
        return ConversationHandler.END

    config = config_manager.get_config()
    settings = config.setdefault("settings", {})

    if settings_input == "set_delay":
        try:
            delay = float(text)
            if 0 <= delay <= 5:
                settings["default_delay"] = delay
                config_manager.save_config()
                await update.message.reply_text(
                    style_text(f"Post delay set to {delay}s.", "success"),
                    reply_markup=create_post_settings_keyboard(),
                    parse_mode="Markdown"
                )
                logger.info(f"User {user_id} set post delay to {delay}s")
            else:
                await update.message.reply_text(
                    style_text("Delay must be between 0 and 5 seconds.", "error"),
                    reply_markup=create_post_settings_keyboard(),
                    parse_mode="Markdown"
                )
                logger.warning(f"User {user_id} provided invalid delay: {text}")
                return POST_SETTINGS
        except ValueError:
            await update.message.reply_text(
                style_text("Invalid number format for delay.", "error"),
                reply_markup=create_post_settings_keyboard(),
                parse_mode="Markdown"
            )
            logger.warning(f"User {user_id} provided invalid delay format: {text}")
            return POST_SETTINGS

    elif settings_input == "set_retries":
        try:
            retries = int(text)
            if 1 <= retries <= 10:
                settings["max_retries"] = retries
                config_manager.save_config()
                await update.message.reply_text(
                    style_text(f"Max retries set to {retries}.", "success"),
                    reply_markup=create_post_settings_keyboard(),
                    parse_mode="Markdown"
                )
                logger.info(f"User {user_id} set max retries to {retries}")
            else:
                await update.message.reply_text(
                    style_text("Retries must be between 1 and 10.", "error"),
                    reply_markup=create_post_settings_keyboard(),
                    parse_mode="Markdown"
                )
                logger.warning(f"User {user_id} provided invalid retries: {text}")
                return POST_SETTINGS
        except ValueError:
            await update.message.reply_text(
                style_text("Invalid number format for retries.", "error"),
                reply_markup=create_post_settings_keyboard(),
                parse_mode="Markdown"
            )
            logger.warning(f"User {user_id} provided invalid retries format: {text}")
            return POST_SETTINGS

    elif settings_input == "set_footer":
        if text.lower() == "clear":
            settings["footer"] = ""
            config_manager.save_config()
            await update.message.reply_text(
                style_text("Footer cleared.", "success"),
                reply_markup=create_post_settings_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} cleared footer")
        elif len(text) <= MAX_FOOTER_LENGTH:
            settings["footer"] = text
            config_manager.save_config()
            await update.message.reply_text(
                style_text("Footer updated successfully.", "success"),
                reply_markup=create_post_settings_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} set footer: {text[:50]}...")
        else:
            await update.message.reply_text(
                style_text(f"Footer too long. Max {MAX_FOOTER_LENGTH} characters.", "error"),
                reply_markup=create_post_settings_keyboard(),
                parse_mode="Markdown"
            )
            logger.warning(f"User {user_id} provided too long footer: {len(text)} chars")
            return POST_SETTINGS

    elif settings_input == "add_admin":
        if not validate_user_id(text):
            await update.message.reply_text(
                style_text("Invalid User ID.", "error"),
                reply_markup=create_admin_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.warning(f"User {user_id} provided invalid admin ID: {text}")
            return ADMIN_MANAGEMENT
        if text == str(OWNER_ID) or text in config.get("admins", []):
            await update.message.reply_text(
                style_text("User is already an admin or is the owner.", "warning"),
                reply_markup=create_admin_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} attempted to add existing admin/owner: {text}")
        else:
            config["admins"].append(text)
            config_manager.save_config()
            await update.message.reply_text(
                style_text(f"Admin {text} added successfully.", "success"),
                reply_markup=create_admin_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} added admin: {text}")

    elif settings_input == "delete_admin":
        if not validate_user_id(text):
            await update.message.reply_text(
                style_text("Invalid User ID.", "error"),
                reply_markup=create_admin_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.warning(f"User {user_id} provided invalid admin ID: {text}")
            return ADMIN_MANAGEMENT
        if text == str(OWNER_ID):
            await update.message.reply_text(
                style_text("Cannot remove the owner.", "error"),
                reply_markup=create_admin_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.warning(f"User {user_id} attempted to remove owner: {text}")
        elif text not in config.get("admins", []):
            await update.message.reply_text(
                style_text("User is not an admin.", "warning"),
                reply_markup=create_admin_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} attempted to remove non-admin: {text}")
        else:
            config["admins"].remove(text)
            config_manager.save_config()
            await update.message.reply_text(
                style_text(f"Admin {text} removed successfully.", "success"),
                reply_markup=create_admin_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} removed admin: {text}")

    context.user_data.pop("settings_input", None)
    return ConversationHandler.END

async def handle_channel_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id: int = update.effective_user.id
    text: str = update.message.text.strip()
    channel_input: Optional[str] = context.user_data.get("channel_input")

    if not channel_input:
        logger.debug(f"User {user_id} sent input without active channel prompt")
        return ConversationHandler.END

    config = config_manager.get_config()

    if channel_input == "add_channel":
        match = re.match(r"^(-100\d{10,})\s+(.+)$", text)
        if not match:
            await update.message.reply_text(
                style_text("Invalid format. Use: -1001234567890 Channel Name", "error"),
                reply_markup=create_channel_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.warning(f"User {user_id} provided invalid channel format: {text}")
            return CHANNEL_MANAGEMENT
        channel_id, name = match.groups()
        if not validate_channel_id(channel_id):
            await update.message.reply_text(
                style_text("Invalid channel ID.", "error"),
                reply_markup=create_channel_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.warning(f"User {user_id} provided invalid channel ID: {channel_id}")
            return CHANNEL_MANAGEMENT
        if channel_id in FIXED_CHANNELS or channel_id in config.get("channels", {}):
            await update.message.reply_text(
                style_text("Channel already exists.", "warning"),
                reply_markup=create_channel_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} attempted to add existing channel: {channel_id}")
        else:
            config["channels"][channel_id] = {"name": name.strip(), "stats": {"post_count": 0}}
            config_manager.save_config()
            await update.message.reply_text(
                style_text(f"Channel {name} ({channel_id}) added.", "success"),
                reply_markup=create_channel_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} added channel: {channel_id} ({name})")

    elif channel_input == "delete_channel":
        if not validate_channel_id(text):
            await update.message.reply_text(
                style_text("Invalid channel ID.", "error"),
                reply_markup=create_channel_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.warning(f"User {user_id} provided invalid channel ID: {text}")
            return CHANNEL_MANAGEMENT
        if text in FIXED_CHANNELS:
            await update.message.reply_text(
                style_text("Cannot remove fixed channel.", "error"),
                reply_markup=create_channel_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.warning(f"User {user_id} attempted to remove fixed channel: {text}")
        elif text not in config.get("channels", {}):
            await update.message.reply_text(
                style_text("Channel not found.", "warning"),
                reply_markup=create_channel_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} attempted to remove non-existent channel: {text}")
        else:
            channel_name = config["channels"][text]["name"]
            config["channels"].pop(text)
            config_manager.save_config()
            await update.message.reply_text(
                style_text(f"Channel {channel_name} ({text}) removed.", "success"),
                reply_markup=create_channel_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} removed channel: {text} ({channel_name})")

    context.user_data.pop("channel_input", None)
    return ConversationHandler.END

async def handle_schedule_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id: int = update.effective_user.id
    text: str = update.message.text.strip()
    schedule_input: Optional[str] = context.user_data.get("schedule_input")

    if not schedule_input:
        logger.debug(f"User {user_id} sent input without active schedule prompt")
        return ConversationHandler.END

    config = config_manager.get_config()
    schedule_dt = validate_schedule_time(text)

    if not schedule_dt:
        await update.message.reply_text(
            style_text("Invalid or past date format. Use YYYY-MM-DD HH:MM.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.warning(f"User {user_id} provided invalid schedule time: {text}")
        return SCHEDULE_BATCH

    batch = context.user_data.get("batch", [])
    selected_channels = context.user_data.get("selected_channels", set())

    if not batch or not selected_channels:
        await update.message.reply_text(
            style_text("No messages or channels selected. Start over.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.warning(f"User {user_id} attempted to schedule without batch or channels")
        context.user_data.pop("schedule_input", None)
        return ConversationHandler.END

    if check_schedule_conflict(config, schedule_dt, selected_channels):
        await update.message.reply_text(
            style_text("Schedule conflict detected. Choose a different time.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.warning(f"User {user_id} attempted to schedule with conflict at {text}")
        return SCHEDULE_BATCH

    if schedule_input == "schedule_batch":
        job_id = str(uuid4())
        config["scheduled_posts"][job_id] = {
            "time": schedule_dt.isoformat(),
            "batch_ids": batch[:],
            "batch_size": len(batch),
            "channels": list(selected_channels),
            "user_id": str(user_id)
        }
        config_manager.save_config()
        context.user_data["schedule_time"] = schedule_dt.isoformat()
        await update.message.reply_text(
            style_text(f"Batch scheduled for {schedule_dt.strftime('%Y-%m-%d %H:%M')} to {len(selected_channels)} channels.", "success"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.info(f"User {user_id} scheduled batch job {job_id} at {text}")

    elif schedule_input == "reschedule_batch":
        job_id = context.user_data.get("reschedule_job_id")
        if not job_id or job_id not in config.get("scheduled_posts", {}):
            await update.message.reply_text(
                style_text("Scheduled job not found.", "error"),
                reply_markup=create_schedule_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.warning(f"User {user_id} attempted to reschedule invalid job {job_id}")
            context.user_data.pop("reschedule_job_id", None)
            return ConversationHandler.END

        config["scheduled_posts"][job_id]["time"] = schedule_dt.isoformat()
        config_manager.save_config()
        await update.message.reply_text(
            style_text(f"Job {job_id[:8]} rescheduled to {schedule_dt.strftime('%Y-%m-%d %H:%M')}.", "success"),
            reply_markup=create_schedule_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.info(f"User {user_id} rescheduled job {job_id} to {text}")
        context.user_data.pop("reschedule_job_id", None)

    context.user_data.pop("schedule_input", None)
    return ConversationHandler.END

# ==================== Scheduled Job Runner ====================
async def run_scheduled_jobs(application: Application) -> None:
    while True:
        config = config_manager.get_config()
        config_manager._cleanup_expired_jobs()
        now = datetime.now()
        scheduled_posts = config.get("scheduled_posts", {})

        for job_id, job_data in list(scheduled_posts.items()):
            try:
                job_time = datetime.fromisoformat(job_data["time"])
                if now >= job_time:
                    user_id = int(job_data["user_id"])
                    context = ContextTypes.DEFAULT_TYPE(application=application)
                    context.user_data["batch"] = job_data["batch_ids"]
                    context.user_data["selected_channels"] = set(job_data["channels"])

                    update = Update(
                        update_id=0,
                        callback_query=CallbackQuery(
                            id=str(uuid4()),
                            from_user=application.bot.get_user(user_id),
                            chat_instance=str(uuid4()),
                            message=await application.bot.send_message(
                                chat_id=user_id,
                                text="Executing scheduled post..."
                            ),
                            data="confirm_post"
                        )
                    )

                    await execute_post(update, context, is_scheduled=True, job_id=job_id)
                    logger.info(f"Executed scheduled job {job_id} for user {user_id}")
            except Exception as e:
                logger.error(f"Error executing scheduled job {job_id}: {e}", exc_info=True)
                await application.bot.send_message(
                    chat_id=job_data["user_id"],
                    text=style_text(f"Scheduled job {job_id[:8]} failed: {str(e)[:500]}.", "error"),
                    parse_mode="Markdown"
                )

        await asyncio.sleep(60)  # Check every minute

# ==================== Main Application ====================
def main() -> None:
    application = Application.builder().token(BOT_TOKEN).build()

    # Conversation Handlers
    admin_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^(admin_add|admin_remove)$")],
        states={
            ADMIN_MANAGEMENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_settings_input)
            ]
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(button_handler, pattern="^cancel_admin$")
        ]
    )

    channel_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^(channel_add|channel_remove)$")],
        states={
            CHANNEL_MANAGEMENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_channel_input)
            ]
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(button_handler, pattern="^cancel_channel$")
        ]
    )

    settings_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^(set_delay|set_retries|set_footer)$")],
        states={
            POST_SETTINGS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_settings_input)
            ]
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(button_handler, pattern="^cancel_settings$")
        ]
    )

    schedule_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^(batch_schedule_menu|reschedule_.*)$")],
        states={
            SCHEDULE_BATCH: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_schedule_input)
            ]
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(button_handler, pattern="^cancel_schedule$")
        ]
    )

    # Command Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", show_help))
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(CommandHandler("status", status))

    # Message Handlers
    application.add_handler(MessageHandler(
        filters.Regex(f"^{'|'.join(map(re.escape, [f'{emoji} {text}' for emoji, text in [
            (EMOJI['admin'], 'Admin Panel'),
            (EMOJI['channel'], 'Channels'),
            (EMOJI['stats'], 'Analytics'),
            (EMOJI['batch'], 'Post'),
            (EMOJI['schedule'], 'Schedules'),
            (EMOJI['settings'], 'Settings'),
            (EMOJI['help'], 'Help')
        ]]))}$"),
        handle_main_menu
    ))
    application.add_handler(MessageHandler(
        filters.TEXT & (~filters.COMMAND | filters.Document.ALL | filters.PHOTO | filters.VIDEO),
        add_to_batch
    ))

    # Callback Query Handler
    application.add_handler(CallbackQueryHandler(button_handler))

    # Inline Query Handler
    application.add_handler(InlineQueryHandler(inline_search))

    # Conversation Handlers
    application.add_handler(admin_conv_handler)
    application.add_handler(channel_conv_handler)
    application.add_handler(settings_conv_handler)
    application.add_handler(schedule_conv_handler)

    # Start scheduled job runner
    application.job_queue.run_once(lambda ctx: asyncio.create_task(run_scheduled_jobs(application)), 0)

    # Start the Bot
    logger.info("Starting Advanced Channel Manager Pro")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
                
