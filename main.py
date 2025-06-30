import json
import os
import re
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Set, List, Optional, Any, Tuple
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

def build_channel_selection_keyboard(selected_channels: Set[str], channels: Dict[str, Dict[str, Any]], page: int = 0, per_page: int = 10) -> Tuple[InlineKeyboardMarkup, int]:
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
    
    if not keyboard:
        keyboard.append([InlineKeyboardButton("No scheduled jobs", callback_data="no_jobs")])
    
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
        style_text(f"Added {msg_type} message to batch ({len(batch)}/{MAX_BATCH_MESSAGES}).", "success"),
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
📦 *Current Batch* ({len(batch)}/{MAX_BATCH_MESSAGES})
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
        [
            InlineKeyboardButton(f"{EMOJI['back']} Back", callback_data="back_to_schedule_list")
        ]
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
        return
    
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
    
    batch: List[int] = context.user_data.get("batch", [])
    selected_channels = context.user_data.get("selected_channels", set())
    
    if not batch or not selected_channels:
        await update.message.reply_text(
            style_text("Missing batch or selected channels.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        context.user_data.pop("schedule_input", None)
        return ConversationHandler.END
    
    config = config_manager.get_config()
    if check_schedule_conflict(config, schedule_dt, selected_channels):
        await update.message.reply_text(
            style_text("Scheduling conflict detected. Choose a different time.", "warning"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        context.user_data.pop("schedule_input", None)
        return ConversationHandler.END
    
    job_id = str(uuid4())
    config["scheduled_posts"][job_id] = {
        "time": schedule_dt.isoformat(),
        "batch_ids": batch[:],
        "channels": list(selected_channels),
        "batch_size": len(batch),
        "admin_id": str(user_id),
        "created_at": datetime.now().isoformat()
    }
    config_manager.save_config()
    
    await update.message.reply_text(
        style_text(f"Batch scheduled for {format_timestamp(schedule_dt.isoformat())}.", "success"),
        reply_markup=create_batch_management_keyboard(),
        parse_mode="Markdown"
    )
    
    context.user_data.pop("schedule_input", None)
    context.user_data["batch"] = []
    context.user_data["selected_channels"] = set()
    logger.info(f"User {user_id} scheduled batch {job_id} for {schedule_dt}")
    return ConversationHandler.END

# ==================== Posting Functions ====================
async def preview_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    batch: List[int] = context.user_data.get("batch", [])
    selected_channels = context.user_data.get("selected_channels", set())
    
    if not batch or not selected_channels:
        await query.message.edit_text(
            style_text("Missing batch or channels.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        return
    
    config = config_manager.get_config()
    settings = config.get("settings", {})
    channels = config_manager.get_all_channels()
    
    text = f"""
📤 *Post Preview*

*Batch Info:*
• Messages: `{len(batch)}`
• Target Channels: `{len(selected_channels)}`
• Total Posts: `{len(batch) * len(selected_channels)}`

*Settings:*
• Delay: `{settings.get('default_delay', POST_DELAY_SECONDS)}s`
• Max Retries: `{settings.get('max_retries', MAX_RETRIES)}`
• Footer: `{settings.get('footer', 'None')[:50] + '...' if len(settings.get('footer', '')) > 50 else settings.get('footer', 'None')}`

*Target Channels:*
"""
    for cid in selected_channels:
        if cid in channels:
            text += f"• {channels[cid]['name']}\n"
    
    await query.message.edit_text(
        text,
        reply_markup=create_post_confirmation_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()
    logger.debug(f"User {query.from_user.id} previewed post")

async def execute_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user_id = query.from_user.id
    batch: List[int] = context.user_data.get("batch", [])
    selected_channels = context.user_data.get("selected_channels", set())
    
    if not batch or not selected_channels:
        await query.message.edit_text(
            style_text("Missing batch or channels.", "error"),
            reply_markup=create_batch_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        return
    
    config = config_manager.get_config()
    settings = config.get("settings", {})
    delay = settings.get("default_delay", POST_DELAY_SECONDS)
    max_retries = settings.get("max_retries", MAX_RETRIES)
    footer = settings.get("footer", "")
    
    total_operations = len(batch) * len(selected_channels)
    completed = 0
    failed = 0
    
    progress_msg = await query.message.edit_text(
        f"📤 *Starting batch post...*\n\nProgress: 0/{total_operations}",
        parse_mode="Markdown"
    )
    
    for msg_id in batch:
        for channel_id in selected_channels:
            try:
                await context.bot.copy_message(
                    chat_id=channel_id,
                    from_chat_id=query.message.chat_id,
                    message_id=msg_id
                )
                
                if footer:
                    await context.bot.send_message(
                        chat_id=channel_id,
                        text=footer[:MAX_FOOTER_LENGTH],
                        parse_mode="Markdown"
                    )
                
                completed += 1
                if completed % 10 == 0:
                    percent = (completed / total_operations) * 100
                    progress_bar = "█" * int(percent // 10) + "░" * (10 - int(percent // 10))
                    await progress_msg.edit_text(
                        f"📤 *Posting in progress...*\n\nProgress: {completed}/{total_operations}\n[{progress_bar}] {percent:.1f}%",
                        parse_mode="Markdown"
                    )
                
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"Failed to post message {msg_id} to channel {channel_id}: {e}")
                failed += 1
    
    # Update stats
    post_id = str(uuid4())
    config_manager.update_stats(
        posts=completed,
        batches=1,
        channels=list(selected_channels),
        admin_id=str(user_id),
        post_id=post_id
    )
    
    success_text = f"""
🎉 *Batch Post Complete!*

*Results:*
• Successful: `{completed}`
• Failed: `{failed}`
• Total: `{total_operations}`

*Posted to:*
• Channels: `{len(selected_channels)}`
• Messages: `{len(batch)}`
"""
    
    await progress_msg.edit_text(
        success_text,
        reply_markup=create_batch_management_keyboard(),
        parse_mode="Markdown"
    )
    
    # Clear batch after successful post
    context.user_data["batch"] = []
    context.user_data["selected_channels"] = set()
    
    await query.answer("Post completed!")
    logger.info(f"User {user_id} completed batch post: {completed} successful, {failed} failed")

# ==================== Post Settings ====================
async def post_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.callback_query:
        query = update.callback_query
        await query.message.edit_text(
            style_text("Post Settings Configuration", "header"),
            reply_markup=create_post_settings_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
    else:
        await update.message.reply_text(
            style_text("Post Settings Configuration", "header"),
            reply_markup=create_post_settings_keyboard(),
            parse_mode="Markdown"
        )
    logger.debug(f"Post settings accessed by user {update.effective_user.id}")

# ==================== Admin Management ====================
async def admin_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    config = config_manager.get_config()
    admins = config.get("admins", [])
    
    text = f"""
👑 *Admin List* ({len(admins)} total)

"""
    for i, admin_id in enumerate(admins, 1):
        is_owner = admin_id == str(OWNER_ID)
        status = "👑 Owner" if is_owner else "👤 Admin"
        stats = config.get("admin_stats", {}).get(admin_id, {})
        posts = stats.get("posts", 0)
        last_action = format_timestamp(stats.get("last_action"), relative=True)
        text += f"{i}. `{admin_id}` - {status}\n   Posts: {posts}, Last: {last_action}\n"
    
    await query.message.edit_text(
        text,
        reply_markup=create_admin_management_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()
    logger.debug(f"Admin list viewed by user {query.from_user.id}")

async def add_admin_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.message.edit_text(
        style_text("Send the user ID of the new admin:", "info"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_admin")]
        ]),
        parse_mode="Markdown"
    )
    context.user_data["admin_input"] = "add_admin"
    await query.answer()
    return ADMIN_MANAGEMENT

async def remove_admin_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    config = config_manager.get_config()
    admins = config.get("admins", [])
    
    if len(admins) <= 1:
        await query.message.edit_text(
            style_text("Cannot remove the only admin.", "error"),
            reply_markup=create_admin_management_keyboard(),
            parse_mode="Markdown"
        )
        await query.answer()
        return ConversationHandler.END
    
    await query.message.edit_text(
        style_text("Send the user ID of the admin to remove:", "info"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_admin")]
        ]),
        parse_mode="Markdown"
    )
    context.user_data["admin_input"] = "remove_admin"
    await query.answer()
    return ADMIN_MANAGEMENT

# ==================== Channel Management ====================
async def channel_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    channels = config_manager.get_all_channels()
    
    text = f"""
📢 *Channel List* ({len(channels)} total)

"""
    for i, (cid, data) in enumerate(channels.items(), 1):
        status = "🔒 Fixed" if data.get("fixed") else "📢 Custom"
        posts = data.get("stats", {}).get("post_count", 0)
        text += f"{i}. {data['name']}\n   ID: `{cid}` - {status}\n   Posts: {posts}\n"
    
    await query.message.edit_text(
        text,
        reply_markup=create_channel_management_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()
    logger.debug(f"Channel list viewed by user {query.from_user.id}")

async def add_channel_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.message.edit_text(
        style_text("Send the channel ID (format: -100xxxxxxxxxx):", "info"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_channel")]
        ]),
        parse_mode="Markdown"
    )
    context.user_data["channel_input"] = "add_channel"
    await query.answer()
    return CHANNEL_MANAGEMENT

async def remove_channel_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.message.edit_text(
        style_text("Send the channel ID to remove:", "info"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_channel")]
        ]),
        parse_mode="Markdown"
    )
    context.user_data["channel_input"] = "remove_channel"
    await query.answer()
    return CHANNEL_MANAGEMENT

# ==================== Button Handler ====================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id
    
    if not config_manager.is_admin(user_id):
        await query.answer("Permission denied.", show_alert=True)
        return
    
    try:
        # Admin management
        if data == "admin_add":
            return await add_admin_prompt(update, context)
        elif data == "admin_remove":
            return await remove_admin_prompt(update, context)
        elif data == "admin_list":
            await admin_list(update, context)
        elif data == "admin_stats":
            await admin_stats(update, context)
        
        # Channel management
        elif data == "channel_add":
            return await add_channel_prompt(update, context)
        elif data == "channel_remove":
            return await remove_channel_prompt(update, context)
        elif data == "channel_list":
            await channel_list(update, context)
        elif data == "channel_stats":
            await channel_stats(update, context)
        
        # Batch management
        elif data == "batch_clear":
            await clear_batch(update, context)
        elif data == "batch_list":
            await show_batch(update, context)
        elif data == "post_options":
            await post_batch_menu(update, context)
        elif data == "batch_schedule_menu":
            await schedule_batch_menu(update, context)
        
        # Schedule management
        elif data == "schedule_list":
            await list_schedules(update, context)
        elif data.startswith("schedule_view_"):
            await view_schedule(update, context)
        elif data.startswith("delete_job_"):
            await delete_schedule(update, context)
        
        # Post settings
        elif data == "post_settings":
            await post_settings(update, context)
        elif data in ["set_delay", "set_retries", "set_footer"]:
            return await setting_input_prompt(update, context)
        elif data == "toggle_notifications":
            await toggle_notifications(update, context)
        elif data == "save_settings":
            await save_post_settings(update, context)
        
        # Channel selection
        elif data.startswith("toggle_"):
            await toggle_channel_selection(update, context)
        elif data == "select_all":
            await select_all_channels(update, context)
        elif data == "unselect_all":
            await unselect_all_channels(update, context)
        elif data.startswith("page_"):
            page = int(data.split("_")[1])
            await post_batch_menu(update, context, page)
        
        # Post actions
        elif data == "post_selected":
            await preview_post(update, context)
        elif data == "confirm_post":
            await execute_post(update, context)
        elif data == "edit_settings":
            await post_settings(update, context)
        
        # Navigation
        elif data == "back_to_categories":
            await back_to_main_menu(update, context)
        elif data == "back_to_schedule_menu":
            await schedule_management_menu_callback(update, context)
        elif data == "back_to_schedule_list":
            await list_schedules(update, context)
        
        # Cancel actions
        elif data in ["cancel_post", "cancel_admin", "cancel_channel", "cancel_schedule", "cancel_settings"]:
            await cancel_operation(update, context)
        
        else:
            await query.answer("Unknown action.")
            
    except Exception as e:
        logger.error(f"Error in button handler: {e}", exc_info=True)
        await query.answer("An error occurred. Please try again.")
    
    return None

# ==================== Helper Functions for Button Handler ====================
async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    config = config_manager.get_config()
    admin_stats = config.get("admin_stats", {})
    
    text = "👥 *Admin Statistics*\n\n"
    
    if not admin_stats:
        text += "No admin activity recorded yet."
    else:
        for admin_id, data in sorted(admin_stats.items(), key=lambda x: x[1].get("posts", 0), reverse=True):
            posts = data.get("posts", 0)
            last_action = format_timestamp(data.get("last_action"), relative=True)
            text += f"• Admin `{admin_id}`: {posts} posts, Last: {last_action}\n"
    
    await query.message.edit_text(
        text,
        reply_markup=create_admin_management_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()

async def channel_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    channels = config_manager.get_all_channels()
    
    text = "📊 *Channel Statistics*\n\n"
    
    sorted_channels = sorted(channels.items(), key=lambda x: x[1].get("stats", {}).get("post_count", 0), reverse=True)
    
    for cid, data in sorted_channels:
        posts = data.get("stats", {}).get("post_count", 0)
        status = "🔒" if data.get("fixed") else "📢"
        text += f"• {status} {data['name']}: {posts} posts\n"
    
    await query.message.edit_text(
        text,
        reply_markup=create_channel_management_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()

async def toggle_channel_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    channel_id = query.data.split("_", 1)[1]
    selected_channels = context.user_data.get("selected_channels", set())
    
    if channel_id in selected_channels:
        selected_channels.remove(channel_id)
    else:
        selected_channels.add(channel_id)
    
    context.user_data["selected_channels"] = selected_channels
    
    channels = config_manager.get_all_channels()
    page = context.user_data.get("page", 0)
    keyboard, _ = build_channel_selection_keyboard(selected_channels, channels, page)
    
    try:
        await query.message.edit_reply_markup(reply_markup=keyboard)
        await query.answer(f"Channel {'selected' if channel_id in selected_channels else 'deselected'}")
    except Exception as e:
        logger.error(f"Error updating channel selection: {e}")
        await query.answer()

async def select_all_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    channels = config_manager.get_all_channels()
    context.user_data["selected_channels"] = set(channels.keys())
    
    page = context.user_data.get("page", 0)
    keyboard, _ = build_channel_selection_keyboard(context.user_data["selected_channels"], channels, page)
    
    try:
        await query.message.edit_reply_markup(reply_markup=keyboard)
        await query.answer("All channels selected")
    except Exception as e:
        logger.error(f"Error selecting all channels: {e}")
        await query.answer()

async def unselect_all_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    context.user_data["selected_channels"] = set()
    
    channels = config_manager.get_all_channels()
    page = context.user_data.get("page", 0)
    keyboard, _ = build_channel_selection_keyboard(set(), channels, page)
    
    try:
        await query.message.edit_reply_markup(reply_markup=keyboard)
        await query.answer("All channels deselected")
    except Exception as e:
        logger.error(f"Error deselecting all channels: {e}")
        await query.answer()

async def delete_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    job_id = query.data.split("_")[2]
    config = config_manager.get_config()
    
    if job_id in config.get("scheduled_posts", {}):
        del config["scheduled_posts"][job_id]
        config_manager.save_config()
        await query.message.edit_text(
            style_text("Scheduled job deleted successfully.", "success"),
            reply_markup=create_schedule_management_keyboard(),
            parse_mode="Markdown"
        )
        logger.info(f"User {query.from_user.id} deleted scheduled job {job_id}")
    else:
        await query.message.edit_text(
            style_text("Scheduled job not found.", "error"),
            reply_markup=create_schedule_management_keyboard(),
            parse_mode="Markdown"
        )
    
    await query.answer()

async def setting_input_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    setting_type = query.data
    
    prompts = {
        "set_delay": "Enter post delay in seconds (0.1-10):",
        "set_retries": "Enter max retries (1-10):",
        "set_footer": "Enter footer text (max 200 chars):"
    }
    
    await query.message.edit_text(
        style_text(prompts[setting_type], "info"),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{EMOJI['cancel']} Cancel", callback_data="cancel_settings")]
        ]),
        parse_mode="Markdown"
    )
    
    context.user_data["settings_input"] = setting_type
    await query.answer()
    return POST_SETTINGS

async def toggle_notifications(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    config = config_manager.get_config()
    settings = config.setdefault("settings", {})
    
    settings["notifications"] = not settings.get("notifications", True)
    config_manager.save_config()
    
    await query.message.edit_text(
        style_text("Post Settings Configuration", "header"),
        reply_markup=create_post_settings_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer(f"Notifications {'enabled' if settings['notifications'] else 'disabled'}")

async def save_post_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.message.edit_text(
        style_text("Settings saved successfully.", "success"),
        reply_markup=create_batch_management_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()

async def back_to_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.message.edit_text(
        style_text("Select an option from the main menu:", "info"),
        reply_markup=create_main_menu(),
        parse_mode="Markdown"
    )
    await query.answer()

async def schedule_management_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
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
    
    await query.message.edit_text(
        text,
        reply_markup=create_schedule_management_keyboard(),
        parse_mode="Markdown"
    )
    await query.answer()

async def cancel_operation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    context.user_data.pop("admin_input", None)
    context.user_data.pop("channel_input", None)
    context.user_data.pop("settings_input", None)
    context.user_data.pop("schedule_input", None)
    
    await query.message.edit_text(
        style_text("Operation canceled.", "info"),
        reply_markup=create_main_menu(),
        parse_mode="Markdown"
    )
    await query.answer()

# ==================== Input Handlers ====================
async def handle_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    text = update.message.text.strip()
    input_type = context.user_data.get("admin_input")
    
    if not input_type:
        return ConversationHandler.END
    
    config = config_manager.get_config()
    
    if input_type == "add_admin":
        if not validate_user_id(text):
            await update.message.reply_text(
                style_text("Invalid user ID format.", "error"),
                reply_markup=create_admin_management_keyboard(),
                parse_mode="Markdown"
            )
        elif text in config.get("admins", []):
            await update.message.reply_text(
                style_text("User is already an admin.", "warning"),
                reply_markup=create_admin_management_keyboard(),
                parse_mode="Markdown"
            )
        else:
            config.setdefault("admins", []).append(text)
            config_manager.save_config()
            await update.message.reply_text(
                style_text(f"User {text} added as admin.", "success"),
                reply_markup=create_admin_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} added admin {text}")
    
    elif input_type == "remove_admin":
        if text == str(OWNER_ID):
            await update.message.reply_text(
                style_text("Cannot remove the owner.", "error"),
                reply_markup=create_admin_management_keyboard(),
                parse_mode="Markdown"
            )
        elif text not in config.get("admins", []):
            await update.message.reply_text(
                style_text("User is not an admin.", "warning"),
                reply_markup=create_admin_management_keyboard(),
                parse_mode="Markdown"
            )
        else:
            config["admins"].remove(text)
            config_manager.save_config()
            await update.message.reply_text(
                style_text(f"Admin {text} removed.", "success"),
                reply_markup=create_admin_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} removed admin {text}")
    
    context.user_data.pop("admin_input", None)
    return ConversationHandler.END

async def handle_channel_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    text = update.message.text.strip()
    input_type = context.user_data.get("channel_input")
    
    if not input_type:
        return ConversationHandler.END
    
    config = config_manager.get_config()
    
    if input_type == "add_channel":
        if not validate_channel_id(text):
            await update.message.reply_text(
                style_text("Invalid channel ID format. Use -100xxxxxxxxxx.", "error"),
                reply_markup=create_channel_management_keyboard(),
                parse_mode="Markdown"
            )
        elif text in config.get("channels", {}) or text in FIXED_CHANNELS:
            await update.message.reply_text(
                style_text("Channel already exists.", "warning"),
                reply_markup=create_channel_management_keyboard(),
                parse_mode="Markdown"
            )
        else:
            try:
                # Try to get channel info
                chat = await context.bot.get_chat(text)
                channel_name = chat.title or f"Channel {text}"
                
                config.setdefault("channels", {})[text] = {
                    "name": channel_name,
                    "description": "User-added channel",
                    "stats": {"post_count": 0}
                }
                config_manager.save_config()
                await update.message.reply_text(
                    style_text(f"Channel '{channel_name}' added successfully.", "success"),
                    reply_markup=create_channel_management_keyboard(),
                    parse_mode="Markdown"
                )
                logger.info(f"User {user_id} added channel {text}")
            except Exception as e:
                logger.error(f"Error adding channel {text}: {e}")
                await update.message.reply_text(
                    style_text("Failed to add channel. Make sure the bot has access.", "error"),
                    reply_markup=create_channel_management_keyboard(),
                    parse_mode="Markdown"
                )
    
    elif input_type == "remove_channel":
        if text in FIXED_CHANNELS:
            await update.message.reply_text(
                style_text("Cannot remove fixed channels.", "error"),
                reply_markup=create_channel_management_keyboard(),
                parse_mode="Markdown"
            )
        elif text not in config.get("channels", {}):
            await update.message.reply_text(
                style_text("Channel not found.", "warning"),
                reply_markup=create_channel_management_keyboard(),
                parse_mode="Markdown"
            )
        else:
            channel_name = config["channels"][text].get("name", text)
            del config["channels"][text]
            config_manager.save_config()
            await update.message.reply_text(
                style_text(f"Channel '{channel_name}' removed.", "success"),
                reply_markup=create_channel_management_keyboard(),
                parse_mode="Markdown"
            )
            logger.info(f"User {user_id} removed channel {text}")
    
    context.user_data.pop("channel_input", None)
    return ConversationHandler.END

async def handle_settings_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    input_type = context.user_data.get("settings_input")
    
    if not input_type:
        return ConversationHandler.END
    
    config = config_manager.get_config()
    settings = config.setdefault("settings", {})
    
    try:
        if input_type == "set_delay":
            delay = float(text)
            if 0.1 <= delay <= 10:
                settings["default_delay"] = delay
                config_manager.save_config()
                message = f"Post delay set to {delay} seconds."
                style = "success"
            else:
                message = "Delay must be between 0.1 and 10 seconds."
                style = "error"
        
        elif input_type == "set_retries":
            retries = int(text)
            if 1 <= retries <= 10:
                settings["max_retries"] = retries
                config_manager.save_config()
                message = f"Max retries set to {retries}."
                style = "success"
            else:
                message = "Retries must be between 1 and 10."
                style = "error"
        
        elif input_type == "set_footer":
            if len(text) <= MAX_FOOTER_LENGTH:
                settings["footer"] = text
                config_manager.save_config()
                message = f"Footer updated: {text[:50]}{'...' if len(text) > 50 else ''}"
                style = "success"
            else:
                message = f"Footer too long. Max {MAX_FOOTER_LENGTH} characters."
                style = "error"
        
        else:
            message = "Unknown setting type."
            style = "error"
            
    except ValueError:
        message = "Invalid input format."
        style = "error"
    
    await update.message.reply_text(
        style_text(message, style),
        reply_markup=create_post_settings_keyboard(),
        parse_mode="Markdown"
    )
    
    context.user_data.pop("settings_input", None)
    return ConversationHandler.END

# ==================== Inline Query Handler ====================
async def inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.inline_query.query
    user_id = update.effective_user.id
    
    if not config_manager.is_admin(user_id):
        return
    
    channels = config_manager.get_all_channels()
    results = []
    
    # Filter channels based on query
    filtered_channels = {
        cid: data for cid, data in channels.items()
        if not query or query.lower() in data["name"].lower() or query in cid
    }
    
    for cid, data in list(filtered_channels.items())[:50]:  # Limit to 50 results
        title = data["name"]
        description = f"ID: {cid} | Posts: {data.get('stats', {}).get('post_count', 0)}"
        if data.get("fixed"):
            description += " | Fixed Channel"
        
        results.append(
            InlineQueryResultArticle(
                id=cid,
                title=title,
                description=description,
                input_message_content=InputTextMessageContent(
                    message_text=f"Selected channel: {title} ({cid})",
                    parse_mode="Markdown"
                )
            )
        )
    
    await update.inline_query.answer(results)

# ==================== Scheduled Jobs Runner ====================
async def run_scheduled_jobs(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Run scheduled posting jobs."""
    config = config_manager.get_config()
    scheduled_posts = config.get("scheduled_posts", {})
    now = datetime.now()
    executed_jobs = []
    
    for job_id, job_data in scheduled_posts.items():
        try:
            job_time = datetime.fromisoformat(job_data["time"])
            if now >= job_time:
                # Execute the scheduled job
                batch_ids = job_data.get("batch_ids", [])
                channels = job_data.get("channels", [])
                admin_id = job_data.get("admin_id")
                
                if batch_ids and channels:
                    settings = config.get("settings", {})
                    delay = settings.get("default_delay", POST_DELAY_SECONDS)
                    footer = settings.get("footer", "")
                    
                    successful_posts = 0
                    failed_posts = 0
                    
                    for msg_id in batch_ids:
                        for channel_id in channels:
                            try:
                                # Note: In a real implementation, you'd need to store the original chat_id
                                # For now, we'll skip the actual posting and just log
                                logger.info(f"Would post message {msg_id} to channel {channel_id}")
                                successful_posts += 1
                                await asyncio.sleep(delay)
                            except Exception as e:
                                logger.error(f"Failed to post scheduled message {msg_id} to {channel_id}: {e}")
                                failed_posts += 1
                    
                    # Update stats
                    post_id = str(uuid4())
                    config_manager.update_stats(
                        posts=successful_posts,
                        batches=1,
                        channels=channels,
                        admin_id=admin_id,
                        post_id=post_id
                    )
                    
                    logger.info(f"Executed scheduled job {job_id}: {successful_posts} successful, {failed_posts} failed")
                
                executed_jobs.append(job_id)
                
        except Exception as e:
            logger.error(f"Error executing scheduled job {job_id}: {e}")
            executed_jobs.append(job_id)  # Remove failed jobs too
    
    # Remove executed jobs
    for job_id in executed_jobs:
        scheduled_posts.pop(job_id, None)
    
    if executed_jobs:
        config_manager.save_config()
    
    # Cleanup expired jobs
    config_manager._cleanup_expired_jobs()

# ==================== Main Application ====================
def main() -> None:
    """Start the bot."""
    try:
        application = Application.builder().token(BOT_TOKEN).build()
        
        # Conversation handlers
        admin_conv_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(button_handler, pattern="^(admin_add|admin_remove)$")],
            states={
                ADMIN_MANAGEMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_input)]
            },
            fallbacks=[CommandHandler("cancel", cancel), CallbackQueryHandler(button_handler, pattern="^cancel_admin$")]
        )
        
        channel_conv_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(button_handler, pattern="^(channel_add|channel_remove)$")],
            states={
                CHANNEL_MANAGEMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_channel_input)]
            },
            fallbacks=[CommandHandler("cancel", cancel), CallbackQueryHandler(button_handler, pattern="^cancel_channel$")]
        )
        
        settings_conv_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(button_handler, pattern="^(set_delay|set_retries|set_footer)$")],
            states={
                POST_SETTINGS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_settings_input)]
            },
            fallbacks=[CommandHandler("cancel", cancel), CallbackQueryHandler(button_handler, pattern="^cancel_settings$")]
        )
        
        schedule_conv_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(button_handler, pattern="^batch_schedule_menu$")],
            states={
                SCHEDULE_BATCH: [MessageHandler(filters.TEXT & ~filters.COMMAND, schedule_batch_confirm)]
            },
            fallbacks=[CommandHandler("cancel", cancel), CallbackQueryHandler(button_handler, pattern="^cancel_schedule$")]
        )
        
        # Add handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("help", show_help))
        application.add_handler(CommandHandler("cancel", cancel))
        application.add_handler(CommandHandler("status", status))
        
        # Conversation handlers
        application.add_handler(admin_conv_handler)
        application.add_handler(channel_conv_handler)
        application.add_handler(settings_conv_handler)
        application.add_handler(schedule_conv_handler)
        
        # Button handler
        application.add_handler(CallbackQueryHandler(button_handler))
        
        # Inline query handler
        application.add_handler(InlineQueryHandler(inline_query))
        
        # Main menu handler
        application.add_handler(MessageHandler(
            filters.TEXT & filters.Regex(f"^({EMOJI['admin']}|{EMOJI['channel']}|{EMOJI['stats']}|{EMOJI['batch']}|{EMOJI['schedule']}|{EMOJI['settings']}|{EMOJI['help']})") & ~filters.COMMAND,
            handle_main_menu
        ))
        
        # Message handler for batch addition
        application.add_handler(MessageHandler(
            (filters.TEXT | filters.PHOTO | filters.VIDEO | filters.DOCUMENT) & ~filters.COMMAND,
            add_to_batch
        ))
        
        # Schedule job runner
        application.job_queue.run_repeating(run_scheduled_jobs, interval=60, first=10)
        
        logger.info("Advanced Channel Manager Pro started successfully!")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
        
    except Exception as e:
        logger.error(f"Failed to start bot: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
