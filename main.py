import asyncio
import datetime
import logging
import re
from urllib.parse import urlparse, quote
import aiohttp
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from bs4 import BeautifulSoup
import sqlite3

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Bot and VK tokens
BOT_TOKEN = "8141698569:AAH5bRGGVYGKRbv0eyZ9hX0BlsAMtJwad8E"
VK_TOKEN = "c26551e5c26551e5c26551e564c1513cc2cc265c26551e5aa37c66a6a6d8f7092ca2102"

if not BOT_TOKEN or not VK_TOKEN:
    raise ValueError("BOT_TOKEN and VK_TOKEN must be set")

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot=bot, storage=storage)
router = Router()
dp.include_router(router)
stats_cache = {}

# Database setup
class Database:
    def __init__(self, db_name='links.db'):
        self.db_name = db_name
        self._init_db()

    def _init_db(self):
        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute('''CREATE TABLE IF NOT EXISTS links
                             (user_id TEXT, title TEXT, short TEXT, original TEXT, group_name TEXT, created TEXT)''')
                c.execute('''CREATE TABLE IF NOT EXISTS groups
                             (user_id TEXT, name TEXT)''')
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {e}")
            raise

    def execute(self, query, params=()):
        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                logger.debug(f"Executing query: {query} with params: {params}")
                c.execute(query, params)
                conn.commit()
                return c.fetchall() if query.strip().upper().startswith('SELECT') else None
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}, query: {query}, params: {params}")
            raise

db = Database()

# FSM states
class LinkForm(StatesGroup):
    waiting_for_link = State()
    waiting_for_title = State()
    bulk_links = State()
    bulk_titles = State()
    creating_group = State()
    rename_link = State()
    choosing_group = State()
    bulk_to_group = State()
    select_links_for_group = State()
    confirm_delete_link = State()

# Error handler decorator
def handle_error(handler):
    async def wrapper(*args, **kwargs):
        try:
            return await handler(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {handler.__name__}: {e}")
            reply = get_main_menu()
            text = f'❌ Ошибка ({datetime.datetime.now().strftime("%H:%M:%S")}): {str(e)[:50]}'
            try:
                if isinstance(args[0], CallbackQuery):
                    try:
                        await args[0].message.edit_text(text, parse_mode="HTML", reply_markup=reply)
                    except aiogram.exceptions.TelegramBadRequest as bad_request:
                        if "message is not modified" in str(bad_request):
                            logger.info(f"Skipping edit in {handler.__name__} due to unchanged message")
                            await args[0].answer()
                        else:
                            await args[0].message.answer(text, parse_mode="HTML", reply_markup=reply)
                    await args[0].answer()
                elif isinstance(args[0], Message):
                    await args[0].answer(text, parse_mode="HTML", reply_markup=reply)
            except Exception as inner_e:
                logger.error(f"Failed to handle error in {handler.__name__}: {inner_e}")
    return wrapper

# Utility functions
def sanitize_input(text: str) -> str:
    return re.sub(r'[^\w\s-]', '', text.strip())[:100]

async def shorten_link_vk(url: str) -> tuple[str | None, str]:
    """
    Returns (short_url, error_message).
    """
    if not is_valid_url(url):
        return None, "Недействительный URL."
    encoded_url = quote(url, safe='')
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://api.vk.com/method/utils.getShortLink?url={encoded_url}&v=5.199&access_token={VK_TOKEN}",
                    timeout=10
                ) as resp:
                    data = await resp.json()
                    logger.debug(f"VK API response for {url}: {data}")
                    if 'response' in data and 'short_url' in data['response']:
                        return data['response']['short_url'], ""
                    elif 'error' in data:
                        error_code = data['error'].get('error_code', 'Unknown')
                        error_msg = data['error'].get('error_msg', 'Unknown error')
                        if error_code == 100:
                            return None, "Недействительный URL, проверьте ссылку."
                        elif error_code == 5:
                            return None, "Недействительный токен VK, обратитесь к администратору."
                        else:
                            return None, f"Ошибка VK API: {error_msg}"
        except aiohttp.ClientError as e:
            logger.error(f"Attempt {attempt+1} failed to shorten URL {url}: {e}")
            if attempt == 2:
                return None, f"Не удалось сократить ссылку: {str(e)[:50]}"
            await asyncio.sleep(2 ** attempt)
    return None, "Не удалось сократить ссылку после нескольких попыток."

async def get_link_stats(key: str) -> int:
    if key in stats_cache:
        return stats_cache[key]
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://api.vk.com/method/utils.getLinkStats?access_token={VK_TOKEN}&key={key}&v=5.199",
                    timeout=10
                ) as resp:
                    data = await resp.json()
                    logger.debug(f"VK API stats response for {key}: {data}")
                    if 'response' in data and 'stats' in data['response']:
                        views = sum(item.get('views', 0) for item in data['response']['stats'])
                        stats_cache[key] = views
                        return views
                    else:
                        logger.error(f"No stats data in response for key {key}: {data}")
                        return 0
        except aiohttp.ClientError as e:
            logger.error(f"Attempt {attempt+1} failed to fetch stats for key {key}: {e}")
            if attempt == 2:
                return 0
            await asyncio.sleep(2 ** attempt)
    return 0

async def fetch_page_title(url: str) -> str | None:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    return None
                soup = BeautifulSoup(await resp.text(), 'html.parser')
                return soup.title.string.strip() if soup.title else None
    except Exception as e:
        logger.error(f"Failed to fetch page title for {url}: {e}")
        return None

def is_valid_url(url: str) -> bool:
    parsed = urlparse(url)
    if not (parsed.scheme in ['http', 'https'] and parsed.netloc):
        return False
    return not re.search(r'\b(javascript|vbscript|eval|onerror|onload|onclick)\b', url.lower(), re.IGNORECASE)

async def cleanup_chat(message: Message, count=5):
    for i in range(count):
        try:
            await bot.delete_message(message.chat.id, message.message_id - i)
        except Exception:
            pass

# Keyboard functions
def make_kb(buttons: list, row_width=2, extra_buttons=None):
    keyboard = []
    for i in range(0, len(buttons), row_width):
        row = buttons[i:i + row_width]
        keyboard.append(row)
    if extra_buttons:
        keyboard.append(extra_buttons[:row_width])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_main_menu():
    return make_kb([
        InlineKeyboardButton(text='🔗 Управление ссылками', callback_data='menu_links'),
        InlineKeyboardButton(text='📁 Папки', callback_data='menu_groups'),
        InlineKeyboardButton(text='📊 Статистика переходов', callback_data='menu_stats'),
        InlineKeyboardButton(text='🗑 Очистить всё', callback_data='clear_all')
    ])

def get_links_menu():
    return make_kb([
        InlineKeyboardButton(text='➕ Добавить одну ссылку', callback_data='add_single'),
        InlineKeyboardButton(text='➕ Добавить несколько ссылок', callback_data='add_bulk'),
        InlineKeyboardButton(text='📋 Мои ссылки', callback_data='my_links'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ], row_width=1)

def get_groups_menu():
    return make_kb([
        InlineKeyboardButton(text='➕ Создать папку', callback_data='create_group'),
        InlineKeyboardButton(text='📁 Мои папки', callback_data='show_groups'),
        InlineKeyboardButton(text='🗑 Удалить папку', callback_data='del_group'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ], row_width=1)

def get_stats_menu():
    return make_kb([
        InlineKeyboardButton(text='🔗 Статистика всех ссылок', callback_data='show_stats:root'),
        InlineKeyboardButton(text='📁 Статистика по папкам', callback_data='group_stats_select'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])

cancel_kb = make_kb([InlineKeyboardButton(text='🚫 Отмена', callback_data='cancel')], row_width=1)

def get_post_add_menu():
    return make_kb([
        InlineKeyboardButton(text='➕ Добавить ещё ссылку', callback_data='add_single'),
        InlineKeyboardButton(text='📋 Мои ссылки', callback_data='my_links'),
        InlineKeyboardButton(text='📁 Добавить в папку', callback_data='ask_to_group'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])

# Handlers
@router.message(Command("start"))
@handle_error
async def cmd_start(message: Message, state: FSMContext, **kwargs):
    logger.info(f"Received /start from user {message.from_user.id}")
    await state.clear()
    welcome_text = (
        "✨ <b>Добро пожаловать в @vkcc_bot!</b> ✨\n\n"
        "Я помогу вам:\n"
        "🔗 Сократить ссылки через VK\n"
        "📁 Организовать их в папки\n"
        "📊 Отслеживать статистику переходов\n\n"
        "Выберите действие в меню ниже 👇"
    )
    await message.answer(welcome_text, parse_mode="HTML", reply_markup=get_main_menu())

@router.message(Command("cancel"))
@handle_error
async def cmd_cancel(message: Message, state: FSMContext, **kwargs):
    logger.info(f"Received /cancel from user {message.from_user.id}")
    await state.clear()
    await message.answer('✅ Действие отменено. Выберите, что делать дальше:', reply_markup=get_main_menu())

@router.callback_query(F.data == "menu")
@handle_error
async def main_menu_handler(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling menu for user {cb.from_user.id}")
    await state.clear()
    text = "🏠 <b>Главное меню</b>\n\nВыберите действие:"
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_main_menu())
    await cb.answer()

@router.callback_query(F.data == "cancel")
@handle_error
async def cancel_action(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling cancel for user {cb.from_user.id}")
    await state.clear()
    text = "✅ Действие отменено. Выберите, что делать дальше:"
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_main_menu())
    await cb.answer()

@router.callback_query(F.data == "menu_links")
@handle_error
async def menu_links(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling menu_links for user {cb.from_user.id}")
    await state.clear()
    text = (
        "🔗 <b>Управление ссылками</b>\n\n"
        "Вы можете:\n"
        "➕ Добавить новую ссылку\n"
        "➕ Загрузить несколько ссылок сразу\n"
        "📋 Просмотреть свои ссылки\n"
        "Выберите действие:"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_links_menu())
    await cb.answer()

@router.callback_query(F.data == "menu_groups")
@handle_error
async def menu_groups(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling menu_groups for user {cb.from_user.id}")
    await state.clear()
    text = (
        "📁 <b>Управление папками</b>\n\n"
        "Вы можете:\n"
        "➕ Создать новую папку\n"
        "📁 Просмотреть свои папки\n"
        "🗑 Удалить ненужную папку\n"
        "Выберите действие:"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_groups_menu())
    await cb.answer()

@router.callback_query(F.data == "menu_stats")
@handle_error
async def menu_stats(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling menu_stats for user {cb.from_user.id}")
    await state.clear()
    text = (
        "📊 <b>Статистика переходов</b>\n\n"
        "Вы можете:\n"
        "🔗 Посмотреть статистику всех ссылок\n"
        "📁 Посмотреть статистику по папкам\n"
        "Выберите действие:"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_stats_menu())
    await cb.answer()

@router.callback_query(F.data == "clear_all")
@handle_error
async def confirm_clear(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling clear_all for user {cb.from_user.id}")
    await state.clear()
    kb = make_kb([
        InlineKeyboardButton(text='✅ Подтвердить удаление', callback_data='confirm_delete_all'),
        InlineKeyboardButton(text='🚫 Отмена', callback_data='menu')
    ])
    text = (
        "⚠️ <b>Внимание!</b>\n\n"
        "Вы собираетесь удалить все ссылки и папки. Это действие нельзя отменить.\n"
        "Продолжить?"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "confirm_delete_all")
@handle_error
async def do_clear(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling confirm_delete_all for user {cb.from_user.id}")
    uid = str(cb.from_user.id)
    db.execute('DELETE FROM links WHERE user_id = ?', (uid,))
    db.execute('DELETE FROM groups WHERE user_id = ?', (uid,))
    stats_cache.clear()
    text = "✅ Все данные удалены. Выберите, что делать дальше:"
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_main_menu())
    await cb.answer()

@router.callback_query(F.data.startswith("show_stats:"))
@handle_error
async def show_stats(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling show_stats for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    loading_msg = await bot.send_message(cb.message.chat.id, '⏳ Загружаем статистику...')
    uid = str(cb.from_user.id)
    scope = cb.data.split(':')[1]
    links = []
    text = '📊 <b>Статистика переходов</b>\n\n'

    try:
        if scope == 'root':
            links = db.execute('SELECT title, short, original FROM links WHERE user_id = ? AND group_name IS NULL', (uid,))
            text += '🔗 Все ссылки:\n'
        else:
            links = db.execute('SELECT title, short, original FROM links WHERE user_id = ? AND group_name = ?', (uid, scope))
            text += f'📁 Папка "{scope}":\n'

        if not links:
            await loading_msg.delete()
            text += '👁 Нет данных для отображения.'
            await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_stats_menu())
            await cb.answer()
            return

        # Convert tuples to dicts
        link_list = [{'title': r[0], 'short': r[1], 'original': r[2]} for r in links]
        stats = await asyncio.gather(*(get_link_stats(l['short'].split('/')[-1]) for l in link_list))
        text += '\n'.join(f"🔗 {l['title']} ({l['short']}): {stats[i]} переходов" for i, l in enumerate(link_list))
        text += f"\n\n👁 Всего переходов: {sum(stats)}"
    except Exception as e:
        logger.error(f"Error processing stats: {e}")
        text += f"❌ Ошибка при загрузке статистики: {str(e)[:50]}"
    finally:
        await loading_msg.delete()
        kb = make_kb([InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')])
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        await cb.answer()

@router.callback_query(F.data == "group_stats_select")
@handle_error
async def group_stats_select(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling group_stats_select for user {cb.from_user.id}")
    await state.clear()
    uid = str(cb.from_user.id)
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    root_groups = [{'name': g[0]} for g in groups]
    if not root_groups:
        text = "📁 <b>Нет папок</b>\n\nСоздайте папку, чтобы посмотреть статистику."
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_stats_menu())
        await cb.answer()
        return
    kb = make_kb([InlineKeyboardButton(text=f"📁 {g['name']}", callback_data=f'show_stats:{g["name"]}') for g in root_groups], row_width=1, extra_buttons=[
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])
    text = "📊 <b>Выберите папку для статистики</b>\n\nНажмите на папку:"
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "add_single")
@handle_error
async def add_single(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling add_single for user {cb.from_user.id}")
    await state.clear()
    text = (
        "🔗 <b>Добавление одной ссылки</b>\n\n"
        "Отправьте ссылку (например, https://example.com).\n"
        "Я сокращу её и предложу название.\n\n"
        "Для отмены нажмите кнопку ниже 👇"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.waiting_for_link)
    await cb.answer()

@router.message(StateFilter(LinkForm.waiting_for_link))
@handle_error
async def process_link(message: Message, state: FSMContext, **kwargs):
    logger.info(f"Processing link from user {message.from_user.id}")
    url = message.text.strip()
    if not is_valid_url(url):
        text = (
            "❌ <b>Некорректный URL</b>\n\n"
            "Введите ссылку, начинающуюся с http:// или https://, например:\n"
            "https://example.com\n\n"
            "Попробуйте снова или нажмите 'Отмена':"
        )
        await message.answer(text, parse_mode="HTML", reply_markup=cancel_kb)
        return
    loading_msg = await message.answer('⏳ Сокращаю ссылку...')
    short_url, error_msg = await shorten_link_vk(url)
    title = await fetch_page_title(url)
    await loading_msg.delete()
    if not short_url:
        text = f"❌ <b>Не удалось сократить ссылку</b>\n\nПричина: {error_msg}\n\nПопробуйте другую ссылку:"
        await message.answer(text, parse_mode="HTML", reply_markup=cancel_kb)
        return
    await state.update_data(original=url, short=short_url, suggested_title=title)
    buttons = [
        InlineKeyboardButton(text='✏️ Ввести своё название', callback_data='enter_title'),
        InlineKeyboardButton(text='🚫 Отмена', callback_data='cancel')
    ]
    if title:
        buttons.insert(0, InlineKeyboardButton(text='✅ Использовать это название', callback_data='use_suggested_title'))
    text = (
        f"🔗 <b>Ссылка успешно сокращена!</b>\n\n"
        f"Оригинал: {url}\n"
        f"Сокращённая: {short_url}\n"
        f"Название: \"{title or 'Нет названия'}\"\n\n"
        "Выберите действие:"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=make_kb(buttons))
    await cleanup_chat(message, count=2)

@router.callback_query(F.data == "use_suggested_title")
@handle_error
async def use_suggested_title(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling use_suggested_title for user {cb.from_user.id}")
    data = await state.get_data()
    title = sanitize_input(data.get('suggested_title') or data['original'][:50])
    uid = str(cb.from_user.id)
    db.execute('INSERT INTO links (user_id, title, short, original, created) VALUES (?, ?, ?, ?, ?)',
               (uid, title, data['short'], data['original'], datetime.datetime.now().isoformat()))
    stats_cache.pop(data['short'].split('/')[-1], None)
    text = (
        f"✅ <b>Ссылка сохранена!</b>\n\n"
        f"Название: {title}\n"
        f"Сокращённая: {data['short']}\n\n"
        "Что дальше?"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_post_add_menu())
    await state.update_data(last_added_entry={'title': title, 'short': data['short'], 'original': data['original']})
    await state.set_state(LinkForm.choosing_group)
    await cb.answer()

@router.callback_query(F.data == "enter_title")
@handle_error
async def enter_title(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling enter_title for user {cb.from_user.id}")
    text = (
        "✏️ <b>Введите своё название</b>\n\n"
        "Название для ссылки (до 100 символов):"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.waiting_for_title)
    await cb.answer()

@router.message(StateFilter(LinkForm.waiting_for_title))
@handle_error
async def process_title(message: Message, state: FSMContext, **kwargs):
    logger.info(f"Processing title from user {message.from_user.id}")
    title = sanitize_input(message.text)
    if not title:
        text = (
            "❌ <b>Некорректное название</b>\n\n"
            "Название должно быть от 1 до 100 символов.\n"
            "Попробуйте снова:"
        )
        await message.answer(text, parse_mode="HTML", reply_markup=cancel_kb)
        return
    data = await state.get_data()
    uid = str(message.from_user.id)
    db.execute('INSERT INTO links (user_id, title, short, original, created) VALUES (?, ?, ?, ?, ?)',
               (uid, title, data['short'], data['original'], datetime.datetime.now().isoformat()))
    stats_cache.pop(data['short'].split('/')[-1], None)
    text = (
        f"✅ <b>Ссылка сохранена!</b>\n\n"
        f"Название: {title}\n"
        f"Сокращённая: {data['short']}\n\n"
        "Что дальше?"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_post_add_menu())
    await state.update_data(last_added_entry={'title': title, 'short': data['short'], 'original': data['original']})
    await cleanup_chat(message, count=2)
    await state.set_state(LinkForm.choosing_group)

@router.callback_query(F.data == "add_bulk")
@handle_error
async def add_bulk(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling add_bulk for user {cb.from_user.id}")
    await state.clear()
    text = (
        "🔗 <b>Добавление нескольких ссылок</b>\n\n"
        "Отправьте список ссылок, по одной на строку, например:\n"
        "https://example.com\n"
        "https://anotherexample.com\n\n"
        "Для отмены нажмите кнопку ниже 👇"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.bulk_links)
    await cb.answer()

@router.message(StateFilter(LinkForm.bulk_links))
@handle_error
async def process_bulk_links(message: Message, state: FSMContext, **kwargs):
    logger.info(f"Processing bulk links from user {message.from_user.id}")
    lines = [l.strip() for l in message.text.splitlines() if l.strip()]
    valid = [l for l in lines if is_valid_url(l)]
    if not valid:
        text = (
            "❌ <b>Нет валидных ссылок</b>\n\n"
            "Убедитесь, что ссылки начинаются с http:// или https://.\n"
            "Попробуйте снова:"
        )
        await message.answer(text, parse_mode="HTML", reply_markup=cancel_kb)
        return
    await state.update_data(bulk_links=valid, success=[], failed=[])
    kb = make_kb([
        InlineKeyboardButton(text='📝 Ввести названия вручную', callback_data='bulk_enter_titles'),
        InlineKeyboardButton(text='🔗 Использовать URL как названия', callback_data='bulk_use_url'),
        InlineKeyboardButton(text='🚫 Отмена', callback_data='cancel')
    ])
    text = f"✅ <b>Найдено {len(valid)} валидных ссылок</b>\n\nВыберите, как назвать ссылки:"
    await message.answer(text, parse_mode="HTML", reply_markup=kb)
    await cleanup_chat(message)

@router.callback_query(F.data == "bulk_use_url")
@handle_error
async def bulk_use_url(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling bulk_use_url for user {cb.from_user.id}")
    data = await state.get_data()
    uid = str(cb.from_user.id)
    success, failed = [], data.get('failed', [])
    loading_msg = await bot.send_message(cb.message.chat.id, '⏳ Обрабатываем ссылки...')
    for url in data['bulk_links']:
        short, error_msg = await shorten_link_vk(url)
        if short:
            title = sanitize_input(url[:50])
            db.execute('INSERT INTO links (user_id, title, short, original, created) VALUES (?, ?, ?, ?, ?)',
                       (uid, title, short, url, datetime.datetime.now().isoformat()))
            stats_cache.pop(short.split('/')[-1], None)
            success.append({'title': title, 'short': short, 'original': url})
        else:
            failed.append({'url': url, 'error': error_msg})
    await loading_msg.delete()
    report = f"✅ <b>Обработано ссылок: {len(success)}</b>\n"
    report += '\n'.join(f"🔗 {e['title']} → {e['short']}" for e in success)
    if failed:
        report += f"\n\n❌ <b>Ошибок: {len(failed)}</b>\n"
        report += '\n'.join(f"🔗 {e['url']}: {e['error']}" for e in failed)
    kb = make_kb([
        InlineKeyboardButton(text='📁 Добавить в папку', callback_data='bulk_to_group'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])
    text = f"{report}\n\nЧто дальше?"
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.update_data(success=success, failed=failed)
    await state.set_state(LinkForm.bulk_to_group)
    await cb.answer()

@router.callback_query(F.data == "bulk_enter_titles")
@handle_error
async def bulk_enter_titles(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling bulk_enter_titles for user {cb.from_user.id}")
    data = await state.get_data()
    await state.update_data(bulk_index=0)
    text = (
        f"✏️ <b>Название для ссылки 1/{len(data['bulk_links'])}</b>\n\n"
        f"Ссылка: {data['bulk_links'][0]}\n"
        "Введите название (до 100 символов):"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.bulk_titles)
    await cb.answer()

@router.message(StateFilter(LinkForm.bulk_titles))
@handle_error
async def process_bulk_titles(message: Message, state: FSMContext, **kwargs):
    logger.info(f"Processing bulk titles from user {message.from_user.id}")
    data = await state.get_data()
    idx = data['bulk_index']
    url = data['bulk_links'][idx]
    title = sanitize_input(message.text)
    if not title:
        text = (
            "❌ <b>Некорректное название</b>\n\n"
            "Название должно быть от 1 до 100 символов.\n"
            f"Ссылка: {url}\n"
            "Попробуйте снова:"
        )
        await message.answer(text, parse_mode="HTML", reply_markup=cancel_kb)
        return
    loading_msg = await message.answer('⏳ Обрабатываем...')
    uid = str(message.from_user.id)
    short, error_msg = await shorten_link_vk(url)
    if short:
        db.execute('INSERT INTO links (user_id, title, short, original, created) VALUES (?, ?, ?, ?, ?)',
                   (uid, title, short, url, datetime.datetime.now().isoformat()))
        stats_cache.pop(short.split('/')[-1], None)
        data['success'].append({'title': title, 'short': short, 'original': url})
    else:
        data['failed'].append({'url': url, 'error': error_msg})
    await loading_msg.delete()
    idx += 1
    if idx < len(data['bulk_links']):
        await state.update_data(bulk_index=idx)
        text = (
            f"✏️ <b>Название для ссылки {idx+1}/{len(data['bulk_links'])}</b>\n\n"
            f"Ссылка: {data['bulk_links'][idx]}\n"
            "Введите название (до 100 символов):"
        )
        await message.answer(text, parse_mode="HTML", reply_markup=cancel_kb)
    else:
        report = f"✅ <b>Обработано ссылок: {len(data['success'])}</b>\n"
        report += '\n'.join(f"🔗 {e['title']} → {e['short']}" for e in data['success'])
        if data.get('failed'):
            report += f"\n\n❌ <b>Ошибок: {len(data['failed'])}</b>\n"
            report += '\n'.join(f"🔗 {e['url']}: {e['error']}" for e in data['failed'])
        kb = make_kb([
            InlineKeyboardButton(text='📁 Добавить в папку', callback_data='bulk_to_group'),
            InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
        ])
        text = f"{report}\n\nЧто дальше?"
        await message.answer(text, parse_mode="HTML", reply_markup=kb)
        await cleanup_chat(message, count=2)
        await state.set_state(LinkForm.bulk_to_group)
    await state.update_data(success=data['success'], failed=data['failed'])

@router.callback_query(F.data == "bulk_to_group")
@handle_error
async def bulk_to_group(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling bulk_to_group for user {cb.from_user.id}")
    uid = str(cb.from_user.id)
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    root_groups = [{'name': g[0]} for g in groups]
    if not root_groups:
        text = (
            "❌ <b>Нет папок</b>\n\n"
            "Создайте папку через меню 'Папки' → 'Создать папку'."
        )
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_groups_menu())
        await state.clear()
        return
    kb = make_kb([InlineKeyboardButton(text=f"📁 {g['name']}", callback_data=f'bulk_assign:{g["name"]}') for g in root_groups], row_width=1, extra_buttons=[
        InlineKeyboardButton(text='🚫 Пропустить', callback_data='bulk_skip_group'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])
    text = (
        "📁 <b>Выберите папку</b>\n\n"
        "Куда добавить сохранённые ссылки?"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.set_state(LinkForm.bulk_to_group)
    await cb.answer()

@router.callback_query(F.data == "bulk_skip_group")
@handle_error
async def bulk_skip_group(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling bulk_skip_group for user {cb.from_user.id}")
    text = "✅ Ссылки сохранены без папки. Что дальше?"
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_links_menu())
    await state.clear()
    await cb.answer()

@router.callback_query(F.data.startswith("bulk_assign:"))
@handle_error
async def bulk_assign_to_group(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling bulk_assign for user {cb.from_user.id}, data={cb.data}")
    group_name = cb.data.split(':', 1)[1]
    data = await state.get_data()
    uid = str(cb.from_user.id)
    success = data.get('success', [])
    if not success:
        text = "❌ <b>Нет ссылок для добавления</b>\n\nПопробуйте добавить ссылки заново."
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_links_menu())
        await state.clear()
        await cb.answer()
        return
    updated = 0
    for entry in success:
        result = db.execute('UPDATE links SET group_name = ? WHERE user_id = ? AND short = ?', (group_name, uid, entry['short']))
        if result is None:  # UPDATE returns None for success
            updated += 1
    text = f"✅ <b>{updated} ссылок добавлены в папку \"{group_name}\"</b>\n\n"
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, group_name))
    text += '\n'.join(f"🔗 {l[0]} → {l[1]}" for l in links) or '📚 Пусто.'
    kb = make_kb([
        InlineKeyboardButton(text='📁 Папки', callback_data='menu_groups'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.clear()
    await cb.answer()

@router.callback_query(F.data == "my_links")
@handle_error
async def my_links(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling my_links for user {cb.from_user.id}")
    await state.clear()
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short, original FROM links WHERE user_id = ? AND group_name IS NULL', (uid,))
    if not links:
        text = (
            "📋 <b>Нет ссылок</b>\n\n"
            "Добавьте ссылки через меню 'Управление ссылками'."
        )
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_links_menu())
        await cb.answer()
        return
    link_list = [{'title': r[0], 'short': r[1], 'original': r[2]} for r in links]
    stats = await asyncio.gather(*(get_link_stats(l['short'].split('/')[-1]) for l in link_list))
    buttons = [InlineKeyboardButton(text=f"🔗 {l['title']} ({stats[i]})", callback_data=f'link_action:root:{idx}') for idx, l in enumerate(link_list)]
    kb = make_kb(buttons, row_width=1, extra_buttons=[
        InlineKeyboardButton(text='📁 Переместить в папку', callback_data='select_links_for_group'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])
    text = (
        "🔗 <b>Ваши ссылки</b>\n\n"
        "Нажмите на ссылку для действий или выберите ниже:"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("link_action:"))
@handle_error
async def link_action(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling link_action for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    _, scope, idx = cb.data.split(':', 2)
    idx = int(idx)
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short, original, group_name FROM links WHERE user_id = ? AND group_name IS NULL', (uid,)) if scope == 'root' else db.execute('SELECT title, short, original, group_name FROM links WHERE user_id = ? AND group_name = ?', (uid, scope))
    link = {'title': links[idx][0], 'short': links[idx][1], 'original': links[idx][2], 'group_name': links[idx][3]}
    back_data = 'my_links' if scope == 'root' else f'view_group:{scope}'
    path = '🔗 Ссылки' if scope == 'root' else f'📁 {scope}'
    kb = make_kb([
        InlineKeyboardButton(text='📊 Посмотреть статистику', callback_data=f'stats:{scope}:{idx}'),
        InlineKeyboardButton(text='✍ Переименовать', callback_data=f'rename:{scope}:{idx}'),
        InlineKeyboardButton(text='🗑 Удалить', callback_data=f'confirm_delete:{scope}:{idx}'),
        InlineKeyboardButton(text='📁 Переместить в папку', callback_data=f'togroup:{scope}:{idx}'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu'),
        InlineKeyboardButton(text='⬅ Назад', callback_data=back_data)
    ])
    text = (
        f"{path}\n\n"
        f"🔗 <b>{link['title']}</b>\n"
        f"Сокращённая: {link['short']}\n"
        f"Оригинал: {link['original']}\n\n"
        "Выберите действие:"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("togroup:"))
@handle_error
async def togroup(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling togroup for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    _, scope, idx = cb.data.split(':', 2)
    idx = int(idx)
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short, original FROM links WHERE user_id = ? AND group_name IS NULL', (uid,)) if scope == 'root' else db.execute('SELECT title, short, original FROM links WHERE user_id = ? AND group_name = ?', (uid, scope))
    link = {'title': links[idx][0], 'short': links[idx][1], 'original': links[idx][2]}
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    if not groups:
        text = (
            "❌ <b>Нет папок</b>\n\n"
            "Создайте папку через меню 'Папки' → 'Создать папку'."
        )
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_groups_menu())
        await state.clear()
        return
    await state.update_data(togroup_link=link)
    kb = make_kb([InlineKeyboardButton(text=f"📁 {g[0]}", callback_data=f'assign:{g[0]}') for g in groups], row_width=1, extra_buttons=[
        InlineKeyboardButton(text='🚫 Отмена', callback_data='cancel')
    ])
    text = (
        "📁 <b>Выберите папку</b>\n\n"
        f"Куда переместить ссылку \"{link['title']}\"?"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.set_state(LinkForm.choosing_group)
    await cb.answer()

@router.callback_query(F.data.startswith("assign:"))
@handle_error
async def assign_to_group_single(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling assign for user {cb.from_user.id}, data={cb.data}")
    group_name = cb.data.split(':', 1)[1]
    data = await state.get_data()
    link = data.get('togroup_link')
    if not link:
        text = "❌ <b>Ссылка не найдена</b>\n\nПопробуйте снова."
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_links_menu())
        await state.clear()
        await cb.answer()
        return
    uid = str(cb.from_user.id)
    db.execute('UPDATE links SET group_name = ? WHERE user_id = ? AND short = ?', (group_name, uid, link['short']))
    text = f"✅ <b>Ссылка перемещена в папку \"{group_name}\"</b>\n\n"
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, group_name))
    text += '\n'.join(f"🔗 {l[0]} → {l[1]}" for l in links) or '📚 Пусто.'
    kb = make_kb([
        InlineKeyboardButton(text='📁 Папки', callback_data='menu_groups'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.clear()
    await cb.answer()

@router.callback_query(F.data == "ask_to_group")
@handle_error
async def ask_to_group(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling ask_to_group for user {cb.from_user.id}")
    uid = str(cb.from_user.id)
    data = await state.get_data()
    if not data.get('last_added_entry'):
        text = "❌ <b>Ссылка не найдена</b>\n\nДобавьте ссылку заново."
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_links_menu())
        await state.clear()
        await cb.answer()
        return
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    root_groups = [{'name': g[0]} for g in groups]
    if not root_groups:
        text = (
            "❌ <b>Нет папок</b>\n\n"
            "Создайте папку через меню 'Папки' → 'Создать папку'."
        )
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_groups_menu())
        await state.clear()
        return
    kb = make_kb([InlineKeyboardButton(text=f"📁 {g['name']}", callback_data=f'single_assign:{g["name"]}') for g in root_groups], row_width=1, extra_buttons=[
        InlineKeyboardButton(text='🚫 Пропустить', callback_data='skip_group'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])
    text = (
        "📁 <b>Добавить ссылку в папку?</b>\n\n"
        "Выберите папку или пропустите:"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "skip_group")
@handle_error
async def skip_group(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling skip_group for user {cb.from_user.id}")
    text = "✅ Ссылка сохранена без папки. Что дальше?"
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_links_menu())
    await state.clear()
    await cb.answer()

@router.callback_query(F.data.startswith("single_assign:"))
@handle_error
async def single_assign_to_group(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling single_assign for user {cb.from_user.id}, data={cb.data}")
    group_name = cb.data.split(':', 1)[1]
    data = await state.get_data()
    entry = data.get('last_added_entry')
    if not entry:
        text = "❌ <b>Ссылка не найдена</b>\n\nДобавьте ссылку заново."
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_links_menu())
        await state.clear()
        await cb.answer()
        return
    uid = str(cb.from_user.id)
    db.execute('UPDATE links SET group_name = ? WHERE user_id = ? AND short = ?', (group_name, uid, entry['short']))
    text = f"✅ <b>Ссылка добавлена в папку \"{group_name}\"</b>\n\n"
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, group_name))
    text += '\n'.join(f"🔗 {l[0]} → {l[1]}" for l in links) or '📚 Пусто.'
    kb = make_kb([
        InlineKeyboardButton(text='📁 Папки', callback_data='menu_groups'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.clear()
    await cb.answer()

@router.callback_query(F.data.startswith("stats:"))
@handle_error
async def stats_handler(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling stats for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    loading_msg = await bot.send_message(cb.message.chat.id, '⏳ Загружаем статистику...')
    parts = cb.data.split(':')
    uid = str(cb.from_user.id)
    scope, idx = parts[1], int(parts[2])
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name IS NULL', (uid,)) if scope == 'root' else db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, scope))
    link = {'title': links[idx][0], 'short': links[idx][1]}
    back_data = 'my_links' if scope == 'root' else f'view_group:{scope}'
    path = '🔗 Ссылки' if scope == 'root' else f'📁 {scope}'
    views = await get_link_stats(link['short'].split('/')[-1])
    text = (
        f"{path}\n\n"
        f"📊 <b>Статистика для \"{link['title']}\"</b>\n\n"
        f"Сокращённая: {link['short']}\n"
        f"👁 Переходов: {views}"
    )
    kb = make_kb([InlineKeyboardButton(text='⬅ Назад', callback_data=back_data)])
    await loading_msg.delete()
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("confirm_delete:"))
@handle_error
async def confirm_delete_link(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling confirm_delete for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    _, scope, idx = cb.data.split(':', 2)
    idx = int(idx)
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name IS NULL', (uid,)) if scope == 'root' else db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, scope))
    link = {'title': links[idx][0], 'short': links[idx][1]}
    back_data = 'my_links' if scope == 'root' else f'view_group:{scope}'
    await state.update_data(delete_scope=scope, delete_idx=idx, delete_short=link['short'])
    kb = make_kb([
        InlineKeyboardButton(text='✅ Удалить', callback_data=f'do_delete:{scope}:{idx}'),
        InlineKeyboardButton(text='🚫 Отмена', callback_data=back_data)
    ])
    text = (
        f"⚠️ <b>Удалить ссылку?</b>\n\n"
        f"Название: {link['title']}\n"
        f"Сокращённая: {link['short']}\n\n"
        "Подтвердите действие:"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.set_state(LinkForm.confirm_delete_link)
    await cb.answer()

@router.callback_query(F.data.startswith("do_delete:"))
@handle_error
async def do_delete(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling do_delete for user {cb.from_user.id}, data={cb.data}")
    _, scope, idx = cb.data.split(':', 2)
    idx = int(idx)
    uid = str(cb.from_user.id)
    data = await state.get_data()
    short = data['delete_short']
    db.execute('DELETE FROM links WHERE user_id = ? AND short = ?', (uid, short))
    stats_cache.pop(short.split('/')[-1], None)
    back_data = 'my_links' if scope == 'root' else f'view_group:{scope}'
    kb = make_kb([InlineKeyboardButton(text='⬅ Назад', callback_data=back_data)])
    text = "✅ Ссылка удалена. Что дальше?"
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.clear()
    await cb.answer()

@router.callback_query(F.data.startswith("rename:"))
@handle_error
async def rename_link(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling rename for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    _, scope, idx = cb.data.split(':', 2)
    idx = int(idx)
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name IS NULL', (uid,)) if scope == 'root' else db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, scope))
    link = {'title': links[idx][0], 'short': links[idx][1]}
    await state.update_data(rename_link_short=link['short'], rename_scope=scope)
    text = (
        f"✍ <b>Переименовать ссылку</b>\n\n"
        f"Текущее название: {link['title']}\n"
        f"Сокращённая: {link['short']}\n\n"
        "Введите новое название (до 100 символов):"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.rename_link)
    await cb.answer()

@router.message(StateFilter(LinkForm.rename_link))
@handle_error
async def process_rename_link(message: Message, state: FSMContext, **kwargs):
    logger.info(f"Processing rename link from user {message.from_user.id}")
    title = sanitize_input(message.text)
    if not title:
        text = (
            "❌ <b>Некорректное название</b>\n\n"
            "Название должно быть от 1 до 100 символов.\n"
            "Попробуйте снова:"
        )
        await message.answer(text, parse_mode="HTML", reply_markup=cancel_kb)
        return
    data = await state.get_data()
    short, scope = data['rename_link_short'], data['rename_scope']
    uid = str(message.from_user.id)
    db.execute('UPDATE links SET title = ? WHERE user_id = ? AND short = ?', (title, uid, short))
    back_data = 'my_links' if scope == 'root' else f'view_group:{scope}'
    kb = make_kb([InlineKeyboardButton(text='⬅ Назад', callback_data=back_data)])
    text = f"✅ Ссылка переименована в \"{title}\". Что дальше?"
    await message.answer(text, parse_mode="HTML", reply_markup=kb)
    await cleanup_chat(message)
    await state.clear()

@router.callback_query(F.data == "create_group")
@handle_error
async def create_group(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling create_group for user {cb.from_user.id}")
    await state.clear()
    text = (
        "📁 <b>Создать новую папку</b>\n\n"
        "Введите название папки (до 100 символов):"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.creating_group)
    await cb.answer()

@router.message(StateFilter(LinkForm.creating_group))
@handle_error
async def process_create_group(message: Message, state: FSMContext, **kwargs):
    logger.info(f"Processing create group from user {message.from_user.id}")
    name = sanitize_input(message.text)
    if not name:
        text = (
            "❌ <b>Некорректное название</b>\n\n"
            "Название папки должно быть от 1 до 100 символов.\n"
            "Попробуйте снова:"
        )
        await message.answer(text, parse_mode="HTML", reply_markup=cancel_kb)
        return
    uid = str(message.from_user.id)
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    if any(g[0] == name for g in groups):
        text = (
            "❌ <b>Папка уже существует</b>\n\n"
            "Введите другое название:"
        )
        await message.answer(text, parse_mode="HTML", reply_markup=cancel_kb)
        return
    db.execute('INSERT INTO groups (user_id, name) VALUES (?, ?)', (uid, name))
    text = f"✅ Папка \"{name}\" создана. Что дальше?"
    await message.answer(text, parse_mode="HTML", reply_markup=get_groups_menu())
    await cleanup_chat(message)
    await state.clear()

@router.callback_query(F.data == "show_groups")
@handle_error
async def show_groups(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling show_groups for user {cb.from_user.id}")
    await state.clear()
    uid = str(cb.from_user.id)
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    root_groups = [{'name': g[0]} for g in groups]
    if not root_groups:
        text = (
            "📁 <b>Нет папок</b>\n\n"
            "Создайте новую папку через 'Создать папку'."
        )
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_groups_menu())
        await cb.answer()
        return
    buttons = [InlineKeyboardButton(text=f"📁 {g['name']}", callback_data=f'view_group:{g["name"]}') for g in root_groups]
    kb = make_kb(buttons, row_width=1, extra_buttons=[
        InlineKeyboardButton(text='🔗 Ссылки', callback_data='my_links'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])
    text = (
        "📁 <b>Ваши папки</b>\n\n"
        "Нажмите на папку для просмотра содержимого:"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("view_group:"))
@handle_error
async def view_group(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling view_group for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    name = cb.data.split(':')[1]
    uid = str(cb.from_user.id)
    groups = db.execute('SELECT name FROM groups WHERE user_id = ? AND name = ?', (uid, name))
    if not groups:
        text = (
            "❌ <b>Папка не найдена</b>\n\n"
            "Возможно, она была удалена."
        )
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_groups_menu())
        await cb.answer()
        return
    text = f"📁 <b>Папка \"{name}\"</b>\n\n"
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, name))
    items = [{'title': l[0], 'short': l[1]} for l in links]
    buttons = []
    if not items:
        text += '📚 Пусто.\n'
    else:
        stats = await asyncio.gather(*(get_link_stats(l['short'].split('/')[-1]) for l in items))
        buttons.extend(InlineKeyboardButton(text=f"🔗 {l['title']} ({stats[i]})", callback_data=f'link_action:{name}:{i}') for i, l in enumerate(items))
    kb = make_kb(buttons, row_width=1, extra_buttons=[
        InlineKeyboardButton(text='📊 Статистика папки', callback_data=f'show_stats:{name}'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu'),
        InlineKeyboardButton(text='⬅ Назад', callback_data='show_groups')
    ])
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "del_group")
@handle_error
async def del_group(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling del_group for user {cb.from_user.id}")
    await state.clear()
    uid = str(cb.from_user.id)
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    root_groups = [{'name': g[0]} for g in groups]
    if not root_groups:
        text = (
            "📁 <b>Нет папок</b>\n\n"
            "Создайте новую папку через 'Создать папку'."
        )
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_groups_menu())
        await cb.answer()
        return
    kb = make_kb([InlineKeyboardButton(text=f"🗑 {g['name']}", callback_data=f"confirm_delete_group:{g['name']}") for g in root_groups], row_width=1, extra_buttons=[
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])
    text = (
        "📁 <b>Удалить папку</b>\n\n"
        "Выберите папку для удаления:"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("confirm_delete_group:"))
@handle_error
async def confirm_delete_group(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling confirm_delete_group for user {cb.from_user.id}, data={cb.data}")
    name = cb.data.split(':', 1)[1]
    uid = str(cb.from_user.id)
    db.execute('DELETE FROM groups WHERE user_id = ? AND name = ?', (uid, name))
    db.execute('DELETE FROM links WHERE user_id = ? AND group_name = ?', (uid, name))
    text = f"✅ Папка \"{name}\" удалена. Что дальше?"
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_groups_menu())
    await cb.answer()

@router.callback_query(F.data == "select_links_for_group")
@handle_error
async def select_links_for_group(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling select_links_for_group for user {cb.from_user.id}")
    await state.clear()
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name IS NULL', (uid,))
    root = [{'title': r[0], 'short': r[1]} for r in links]
    if not root:
        text = (
            "📋 <b>Нет ссылок</b>\n\n"
            "Добавьте ссылки через меню 'Управление ссылками'."
        )
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_links_menu())
        await cb.answer()
        return
    await state.update_data(selected_links=[])
    buttons = [InlineKeyboardButton(text=f"🔗 {l['title']}", callback_data=f'toggle_select:root:{idx}') for idx, l in enumerate(root)]
    kb = make_kb(buttons, row_width=1, extra_buttons=[
        InlineKeyboardButton(text='✅ Готово', callback_data='confirm_select_links'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])
    text = (
        "🔗 <b>Выберите ссылки для перемещения</b>\n\n"
        "Нажмите на ссылки, затем 'Готово':"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.set_state(LinkForm.select_links_for_group)
    await cb.answer()

@router.callback_query(F.data.startswith("toggle_select:"))
@handle_error
async def toggle_select_link(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling toggle_select for user {cb.from_user.id}, data={cb.data}")
    _, scope, idx = cb.data.split(':', 2)
    idx = int(idx)
    data = await state.get_data()
    selected = data.get('selected_links', [])
    link_id = f"{scope}:{idx}"
    if link_id in selected:
        selected.remove(link_id)
    else:
        selected.append(link_id)
    await state.update_data(selected_links=selected)
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name IS NULL', (uid,))
    root = [{'title': r[0], 'short': r[1]} for r in links]
    buttons = [InlineKeyboardButton(text=f"{'✅ ' if f'root:{i}' in selected else '🔗 '}{l['title']}", callback_data=f'toggle_select:root:{i}') for i, l in enumerate(root)]
    kb = make_kb(buttons, row_width=1, extra_buttons=[
        InlineKeyboardButton(text='✅ Готово', callback_data='confirm_select_links'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])
    text = (
        "🔗 <b>Выберите ссылки для перемещения</b>\n\n"
        f"Выбрано: {len(selected)}\n"
        "Нажмите на ссылки, затем 'Готово':"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "confirm_select_links")
@handle_error
async def confirm_select_links(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling confirm_select_links for user {cb.from_user.id}")
    data = await state.get_data()
    selected = data.get('selected_links', [])
    if not selected:
        text = (
            "❌ <b>Не выбраны ссылки</b>\n\n"
            "Выберите хотя бы одну ссылку."
        )
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_links_menu())
        await state.clear()
        await cb.answer()
        return
    uid = str(cb.from_user.id)
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    root_groups = [{'name': g[0]} for g in groups]
    if not root_groups:
        text = (
            "❌ <b>Нет папок</b>\n\n"
            "Создайте папку через меню 'Папки' → 'Создать папку'."
        )
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=get_groups_menu())
        await state.clear()
        await cb.answer()
        return
    kb = make_kb([InlineKeyboardButton(text=f"📁 {g['name']}", callback_data=f'multi_assign:{g["name"]}') for g in root_groups], row_width=1, extra_buttons=[
        InlineKeyboardButton(text='🚫 Отмена', callback_data='cancel')
    ])
    text = (
        "📁 <b>Выберите папку</b>\n\n"
        f"Переместить {len(selected)} ссылок в папку:"
    )
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("multi_assign:"))
@handle_error
async def multi_assign_to_group(cb: CallbackQuery, state: FSMContext, **kwargs):
    logger.info(f"Handling multi_assign for user {cb.from_user.id}, data={cb.data}")
    group_name = cb.data.split(':', 1)[1]
    data = await state.get_data()
    selected = data.get('selected_links', [])
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name IS NULL', (uid,))
    root = [{'title': r[0], 'short': r[1]} for r in links]
    updated = 0
    for link_id in selected:
        _, idx = link_id.split(':')
        idx = int(idx)
        db.execute('UPDATE links SET group_name = ? WHERE user_id = ? AND short = ?', (group_name, uid, root[idx]['short']))
        updated += 1
    text = f"✅ <b>{updated} ссылок перемещены в \"{group_name}\"</b>\n\n"
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, group_name))
    text += '\n'.join(f"🔗 {l[0]} → {l[1]}" for l in links) or '📚 Пусто.'
    kb = make_kb([
        InlineKeyboardButton(text='📁 Папки', callback_data='menu_groups'),
        InlineKeyboardButton(text='🏠 Главное меню', callback_data='menu')
    ])
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.clear()
    await cb.answer()

async def main():
    logger.info("Starting bot polling")
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Polling failed: {e}")
        raise

if __name__ == '__main__':
    asyncio.run(main())