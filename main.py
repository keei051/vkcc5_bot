import os
from loguru import logger

BOT_TOKEN = os.getenv("BOT_TOKEN") or "7348002301:AAH2AY0N6oFUWjK5OBn7epUWeD-63ZlSb-k"
VK_API_TOKEN = os.getenv("VK_API_TOKEN") or "c26551e5c26551e5c26551e564c1513cc2cc265c26551e5aa37c66a6a6d8f7092ca2102"

logger.add("bot.log", rotation="1 MB")
logger.info("🚀 Бот запускается")
import asyncio
import datetime
import logging
import re
from urllib.parse import urlparse, quote
import aiohttp
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from bs4 import BeautifulSoup
import sqlite3

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

BOT_TOKEN = "7348002301:AAH2AY0N6oFUWjK5OBn7epUWeD-63ZlSb-k"
VK_TOKEN = "c26551e5c26551e5c26551e564c1513cc2cc265c26551e5aa37c66a6a6d8f7092ca2102"

if not BOT_TOKEN or not VK_TOKEN:
    raise ValueError("BOT_TOKEN and VK_TOKEN must be set")

bot = Bot(BOT_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
router = Router()
dp.include_router(router)
stats_cache = {}

class Database:
    def __init__(self, db_name='links.db'):
        try:
            self._init_db()
        except sqlite3.Error as e:
            logger.error("DB init failed: {e}")
            raise
    def _init_db(self):
        with sqlite3.connect(self.db_name) as conn:
            c = conn.cursor()
            c.execute('CREATE TABLE IF NOT EXISTS links (user_id TEXT, title TEXT, short TEXT, original TEXT, group_name TEXT, created TEXT)')
            c.execute('CREATE TABLE IF NOT EXISTS groups (user_id TEXT, name TEXT)')
            conn.commit()
    def execute(self, query, params=()):
        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute(query, params)
                conn.commit()
                return c.fetchall() if query.upper().startswith('SELECT') else c.rowcount
        except sqlite3.Error as e:
            logger.error(f"DB execute failed: {e}")
            raise

db = Database()

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
    waiting_for_stats_date = State()

def handle_error(handler):
    async def wrapper(*args, **kwargs):
        try: return await handler(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {handler.__name__}: {e}")
            reply = get_main_menu()
            text = f'❌ Ошибка ({datetime.datetime.now().strftime("%H:%M:%S")}): {str(e)[:50]}'
            if isinstance(args[0], types.CallbackQuery):
                try: await args[0].message.edit_text(text, parse_mode="HTML", reply_markup=reply)
                except: await args[0].message.answer(text, parse_mode="HTML", reply_markup=reply)
                await args[0].answer()
            elif isinstance(args[0], types.Message):
                await args[0].answer(text, parse_mode="HTML", reply_markup=reply)
    return wrapper

def sanitize_input(text): return re.sub(r'[^\w\s-]', '', text.strip())[:100]

async def shorten_link_vk(url):
    if not is_valid_url(url): return None, "Недействительный URL."
    encoded_url = quote(url, safe='')
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"https://api.vk.com/method/utils.getShortLink?url={encoded_url}&v=5.199&access_token={VK_TOKEN}", timeout=10) as resp:
                    data = await resp.json()
                    if 'response' in data and 'short_url' in data['response']: return data['response']['short_url'], ""
                    if 'error' in data:
                        error_code, error_msg = data['error'].get('error_code', 'Unknown'), data['error'].get('error_msg', 'Unknown')
                        if error_code in [100, 5]: return None, f"Ошибка: {error_msg}"
                        return None, f"Ошибка VK API: {error_msg}"
        except aiohttp.ClientError as e:
            if attempt == 2: return None, f"Не удалось сократить: {str(e)[:50]}"
            await asyncio.sleep(2 ** attempt)
    return None, "Не удалось сократить после попыток."

async def get_link_stats(key, date_from=None, date_to=None):
    cache_key = f"{key}:{date_from}:{date_to}"
    if cache_key in stats_cache and (datetime.datetime.now() - stats_cache.get(f"{cache_key}:time", 0)).seconds < 600:
        return stats_cache[cache_key]
    params = {"access_token": VK_TOKEN, "key": key, "v": "5.199", "extended": 1, "interval": "day"}
    if date_from and date_to: params.update({"date_from": date_from, "date_to": date_to})
    result = {"views": 0, "cities": {}}
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.vk.com/method/utils.getLinkStats", params=params, timeout=10) as resp:
                    data = await resp.json()
                    if "response" in data and "stats" in data["response"]:
                        for period in data["response"]["stats"]:
                            result["views"] += period.get("views", 0)
                            for city in period.get("cities", []): result["cities"][str(city.get("city_id"))] = result["cities"].get(str(city.get("city_id")), 0) + city.get("views", 0)
                        stats_cache[cache_key] = result
                        stats_cache[f"{cache_key}:time"] = datetime.datetime.now()
                        return result
                    if "error" in data: logger.error(f"VK API error: {data['error']}")
        except aiohttp.ClientError as e:
            if attempt == 2: return result
            await asyncio.sleep(2 ** attempt)
    return result

async def get_city_names(city_ids):
    if not city_ids: return {}
    cache_key = f"cities:{','.join(map(str, city_ids))}"
    if cache_key in stats_cache: return stats_cache[cache_key]
    params = {"access_token": VK_TOKEN, "city_ids": ",".join(map(str, city_ids)), "v": "5.199"}
    result = {}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.vk.com/method/database.getCitiesById", params=params, timeout=10) as resp:
                data = await resp.json()
                if "response" in data: result.update({str(city["id"]): city.get("title", "Неизвестный город") for city in data["response"]})
                stats_cache[cache_key] = result
                return result
    except aiohttp.ClientError as e:
        logger.error(f"Failed to fetch city names: {e}")
        return result

async def fetch_page_title(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200: return None
                soup = BeautifulSoup(await resp.text(), 'html.parser')
                return soup.title.string.strip() if soup.title else None
    except Exception as e:
        logger.error(f"Failed to fetch page title for {url}: {e}")
        return None

def is_valid_url(url):
    parsed = urlparse(url)
    return parsed.scheme in ['http', 'https'] and parsed.netloc and not re.search(r'\b(javascript|vbscript|eval|onerror|onload|onclick)\b', url.lower(), re.IGNORECASE)

async def cleanup_chat(message, count=5):
    for i in range(count): await bot.delete_message(message.chat.id, message.message_id - i)

def make_kb(buttons, row_width=2, extra_buttons=None):
    keyboard = [buttons[i:i + row_width] for i in range(0, len(buttons), row_width)]
    if extra_buttons: keyboard.append(extra_buttons[:row_width])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_main_menu(): return make_kb([InlineKeyboardButton('🔗 Ссылки', callback_data='menu_links'), InlineKeyboardButton('📁 Папки', callback_data='menu_groups'), InlineKeyboardButton('📊 Статистика', callback_data='menu_stats'), InlineKeyboardButton('🗑 Очистить', callback_data='clear_all')])
def get_links_menu(): return make_kb([InlineKeyboardButton('➕ Одна', callback_data='add_single'), InlineKeyboardButton('➕ Несколько', callback_data='add_bulk'), InlineKeyboardButton('📋 Мои', callback_data='my_links'), InlineKeyboardButton('🏠 Меню', callback_data='menu')], row_width=1)
def get_groups_menu(): return make_kb([InlineKeyboardButton('➕ Создать', callback_data='create_group'), InlineKeyboardButton('📁 Показать', callback_data='show_groups'), InlineKeyboardButton('🗑 Удалить', callback_data='del_group'), InlineKeyboardButton('🏠 Меню', callback_data='menu')], row_width=1)
def get_stats_menu(): return make_kb([InlineKeyboardButton('🔗 Все', callback_data='show_stats:root'), InlineKeyboardButton('📅 Период', callback_data='stats_by_date'), InlineKeyboardButton('🔗 Одна', callback_data='select_link_stats'), InlineKeyboardButton('📁 По папкам', callback_data='group_stats_select'), InlineKeyboardButton('🏠 Меню', callback_data='menu')], row_width=1)
cancel_kb = make_kb([InlineKeyboardButton('🚫 Отмена', callback_data='cancel')], row_width=1)
def get_post_add_menu(): return make_kb([InlineKeyboardButton('➕ Ещё', callback_data='add_single'), InlineKeyboardButton('📋 Мои', callback_data='my_links'), InlineKeyboardButton('📁 Папка', callback_data='ask_to_group'), InlineKeyboardButton('🏠 Меню', callback_data='menu')])

# Handlers
@router.message(Command("start"))
@handle_error
async def cmd_start(message: types.Message, state: FSMContext):
    logger.info(f"Received /start from user {message.from_user.id}")
    await state.clear()
    await message.answer("✨ @KaraLinka! ✨\n🔗 Ссылки\n📁 Папки\n📊 Статистика\nВыберите:", parse_mode="HTML", reply_markup=get_main_menu())

@router.message(Command("cancel"))
@handle_error
async def cmd_cancel(message: types.Message, state: FSMContext):
    logger.info(f"Received /cancel from user {message.from_user.id}")
    await state.clear()
    await message.answer('✅ Отменено. Выберите:', reply_markup=get_main_menu())

@router.callback_query(F.data == "menu")
@handle_error
async def main_menu_handler(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling menu for user {cb.from_user.id}")
    await state.clear()
    await cb.message.edit_text("🏠 Меню\nВыберите:", parse_mode="HTML", reply_markup=get_main_menu())
    await cb.answer()

@router.callback_query(F.data == "cancel")
@handle_error
async def cancel_action(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling cancel for user {cb.from_user.id}")
    await state.clear()
    await cb.message.edit_text("✅ Отменено. Выберите:", parse_mode="HTML", reply_markup=get_main_menu())
    await cb.answer()

@router.callback_query(F.data == "menu_links")
@handle_error
async def menu_links(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling menu_links for user {cb.from_user.id}")
    await state.clear()
    await cb.message.edit_text("🔗 Ссылки\n➕ Добавить\n➕ Несколько\n📋 Мои\nВыберите:", parse_mode="HTML", reply_markup=get_links_menu())
    await cb.answer()

@router.callback_query(F.data == "menu_groups")
@handle_error
async def menu_groups(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling menu_groups for user {cb.from_user.id}")
    await state.clear()
    await cb.message.edit_text("📁 Папки\n➕ Создать\n📁 Показать\n🗑 Удалить\nВыберите:", parse_mode="HTML", reply_markup=get_groups_menu())
    await cb.answer()

@router.callback_query(F.data == "menu_stats")
@handle_error
async def menu_stats(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling menu_stats for user {cb.from_user.id}")
    await state.clear()
    await cb.message.edit_text("📊 Статистика\n🔗 Все\n📅 Период\n🔗 Одна\n📁 По папкам\nВыберите:", parse_mode="HTML", reply_markup=get_stats_menu())
    await cb.answer()

@router.callback_query(F.data == "clear_all")
@handle_error
async def confirm_clear(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling clear_all for user {cb.from_user.id}")
    await state.clear()
    kb = make_kb([InlineKeyboardButton('✅ Да', callback_data='confirm_delete_all'), InlineKeyboardButton('🚫 Нет', callback_data='menu')])
    await cb.message.edit_text("⚠️ Удалить всё? Необратимо.", parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "confirm_delete_all")
@handle_error
async def do_clear(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling confirm_delete_all for user {cb.from_user.id}")
    uid = str(cb.from_user.id)
    db.execute('DELETE FROM links WHERE user_id = ?', (uid,))
    db.execute('DELETE FROM groups WHERE user_id = ?', (uid,))
    stats_cache.clear()
    await cb.message.edit_text("✅ Всё удалено. Выберите:", parse_mode="HTML", reply_markup=get_main_menu())
    await cb.answer()

@router.callback_query(F.data.startswith("show_stats:"))
@handle_error
async def show_stats(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling show_stats for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    loading_msg = await bot.send_message(cb.message.chat.id, '⏳ Загружаем...')
    uid, scope = str(cb.from_user.id), cb.data.split(':')[1]
    links = db.execute('SELECT title, short, original FROM links WHERE user_id = ? AND group_name IS NULL', (uid,)) if scope == 'root' else db.execute('SELECT title, short, original FROM links WHERE user_id = ? AND group_name = ?', (uid, scope))
    text = f"📊 Статистика {'всех' if scope == 'root' else scope}\n"
    if not links:
        text += "👁 Нет данных."
    else:
        link_list = [{'title': r[0], 'short': r[1], 'original': r[2]} for r in links]
        stats = await asyncio.gather(*(get_link_stats(l['short'].split('/')[-1]) for l in link_list))
        all_cities = {cid: sum(s['cities'].get(cid, 0) for s in stats) for cid in {c for s in stats for c in s['cities']}}
        city_names = await get_city_names(list(all_cities))
        for i, l in enumerate(link_list):
    text += f"🔗 {l['title']} ({l['short']}): {stats[i]['views']}\n"
        text += "\n👁 Всего: {sum(s['views'] for s in stats)}"
        if all_cities:
            city_lines = [f'- {city_names.get(cid, "Неизв.")}: {views}' for cid, views in all_cities.items()]
            text += "\n🏙 Города:\n" + '" + "\n".join(city_lines) + "
        else:
            text += "\n🏙 Нет данных."
    kb = make_kb([InlineKeyboardButton('🔗 Одна', callback_data='select_link_stats'), InlineKeyboardButton('🏠 Меню', callback_data='menu')])
    await loading_msg.delete()
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "stats_by_date")
@handle_error
async def stats_by_date(cb: types.CallbackQuery, state: FSMContext):
    logger.info("Handling stats_by_date for user {cb.from_user.id}")
    await state.clear()
    await cb.message.edit_text("📅 Введите даты: ГГГГ-ММ-ДД ГГГГ-ММ-ДД (прим. 2025-06-01 2025-06-24)", parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.waiting_for_stats_date)
    await cb.answer()

@router.message(StateFilter(LinkForm.waiting_for_stats_date))
@handle_error
async def process_stats_date(message: types.Message, state: FSMContext):
    logger.info(f"Processing stats date from user {message.from_user.id}")
    
    dates = message.text.strip().split()
    if len(dates) != 2 or not all(re.match(r"\d{4}-\d{2}-\d{2}", d) for d in dates):
        await message.answer("❌ Неверный формат. Введите даты.", reply_markup=cancel_kb)
        return

    date_from, date_to = dates
    uid = str(message.from_user.id)

    links = db.execute('SELECT title, short FROM links WHERE user_id = ?', (uid,))
    if not links:
        await message.answer("📋 Нет ссылок.", reply_markup=get_stats_menu())
        await state.clear()
        return

    loading_msg = await message.answer('⏳ Загружаем...')

    stats = await asyncio.gather(
        *(get_link_stats(l[1].split('/')[-1], date_from, date_to) for l in links)
    )

    all_cities = {
        cid: sum(s['cities'].get(cid, 0) for s in stats)
        for cid in {c for s in stats for c in s['cities']}
    }

    city_names = await get_city_names(list(all_cities))

    text = f"📊 Статистика за {date_from}—{date_to}\n"
    text += '" + "\n".join(f"🔗 {l[0]}: {stats[i]['views']}" for i, l in enumerate(links) + ")
    text += f"\n👁 Всего: {sum(s['views'] for s in stats)}"

    if all_cities:
        city_lines = [
            "- {city_names.get(cid, 'Неизв.')}: {views}"
            for cid, views in all_cities.items()
        ]
        text += "\n🏙 Города:\n" + "\n".join(city_lines)
    else:
        text += "\n🏙 Нет данных."

    await loading_msg.delete()
    await message.answer(text, parse_mode="HTML", reply_markup=get_stats_menu())
    await cleanup_chat(message)
    await state.clear()


@router.callback_query(F.data == "select_link_stats")
@handle_error
async def select_link_stats(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling select_link_stats for user {cb.from_user.id}")
    await state.clear()
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short FROM links WHERE user_id = ?', (uid,))
    if not links:
        await cb.message.edit_text("📋 Нет ссылок.\nДобавьте через 'Ссылки'.", parse_mode="HTML", reply_markup=get_stats_menu())
        await cb.answer()
        return
    buttons = [InlineKeyboardButton(f"🔗 {l[0]}", callback_data=f'single_link_stats:root:{i}') for i, l in enumerate(links)]
    kb = make_kb(buttons, row_width=1, extra_buttons=[InlineKeyboardButton('🏠 Меню', callback_data='menu')])
    await cb.message.edit_text("🔗 Выберите ссылку:", parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("single_link_stats:"))
@handle_error
async def single_link_stats(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling single_link_stats for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    loading_msg = await bot.send_message(cb.message.chat.id, '⏳ Загружаем...')
    _, scope, idx = cb.data.split(':')
    idx = int(idx)
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short, original FROM links WHERE user_id = ? AND group_name IS NULL', (uid,)) if scope == 'root' else db.execute('SELECT title, short, original FROM links WHERE user_id = ? AND group_name = ?', (uid, scope))
    link = {'title': links[idx][0], 'short': links[idx][1], 'original': links[idx][2]}
    stats = await get_link_stats(link['short'].split('/')[-1])
    city_names = await get_city_names(list(stats['cities'].keys()))
    text = f"📊 {link['title']}\n{link['short']}\n{link['original']}\n👁 {stats['views']}"
    city_lines = [
        f"- {city_names.get(cid, 'Неизв.')}: {views}"
        for cid, views in stats['cities'].items()
    ]
    text += "\n🏙 " + "\n".join(city_lines)
    else: text += "\n🏙 Нет данных."
    kb = make_kb([InlineKeyboardButton('🔄 Обновить', callback_data=f'single_link_stats:{scope}:{idx}'), InlineKeyboardButton('⬅ Назад', callback_data='select_link_stats'), InlineKeyboardButton('🏠 Меню', callback_data='menu')])
    await loading_msg.delete()
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "group_stats_select")
@handle_error
async def group_stats_select(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling group_stats_select for user {cb.from_user.id}")
    await state.clear()
    uid = str(cb.from_user.id)
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    if not groups:
        await cb.message.edit_text("📁 Нет папок.\nСоздайте через 'Папки'.", parse_mode="HTML", reply_markup=get_stats_menu())
        await cb.answer()
        return
    kb = make_kb([InlineKeyboardButton(f"📁 {g[0]}", callback_data=f'show_stats:{g[0]}') for g in groups], row_width=1, extra_buttons=[InlineKeyboardButton('🏠 Меню', callback_data='menu')])
    await cb.message.edit_text("📊 Выберите папку:", parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "add_single")
@handle_error
async def add_single(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling add_single for user {cb.from_user.id}")
    await state.clear()
    await cb.message.edit_text("🔗 Введите ссылку (http://...)\nЯ сокращу.\nОтмена: 🚫", parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.waiting_for_link)
    await cb.answer()

@router.message(StateFilter(LinkForm.waiting_for_link))
@handle_error
async def process_link(message: types.Message, state: FSMContext):
    logger.info(f"Processing link from user {message.from_user.id}")
    url = message.text.strip()
    if not is_valid_url(url):
        await message.answer("❌ Неверный URL.\nПопробуйте снова:", reply_markup=cancel_kb)
        return
    loading_msg = await message.answer('⏳ Сокращаю...')
    short_url, error_msg = await shorten_link_vk(url)
    title = await fetch_page_title(url)
    await loading_msg.delete()
    if not short_url:
        await message.answer(f"❌ Ошибка: {error_msg}", reply_markup=cancel_kb)
        return
    await state.update_data(original=url, short=short_url, suggested_title=title)
    buttons = [InlineKeyboardButton('✏️ Название', callback_data='enter_title'), InlineKeyboardButton('🚫 Отмена', callback_data='cancel')]
    if title: buttons.insert(0, InlineKeyboardButton('✅ Использовать', callback_data='use_suggested_title'))
    await message.answer(f"🔗 {short_url}\nНазвание: \"{title or 'Нет'}\"", parse_mode="HTML", reply_markup=make_kb(buttons))
    await cleanup_chat(message, 2)

@router.callback_query(F.data == "use_suggested_title")
@handle_error
async def use_suggested_title(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling use_suggested_title for user {cb.from_user.id}")
    data = await state.get_data()
    title = sanitize_input(data.get('suggested_title') or data['original'][:50])
    uid = str(cb.from_user.id)
    db.execute('INSERT INTO links (user_id, title, short, original, created) VALUES (?, ?, ?, ?, ?)', (uid, title, data['short'], data['original'], datetime.datetime.now().isoformat()))
    stats_cache.pop(data['short'].split('/')[-1], None)
    await cb.message.edit_text(f"✅ {title}\n{data['short']}\nЧто дальше?", parse_mode="HTML", reply_markup=get_post_add_menu())
    await state.update_data(last_added_entry={'title': title, 'short': data['short'], 'original': data['original']})
    await state.set_state(LinkForm.choosing_group)
    await cb.answer()

@router.callback_query(F.data == "enter_title")
@handle_error
async def enter_title(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling enter_title for user {cb.from_user.id}")
    await cb.message.edit_text("✏️ Введите название (до 100):", parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.waiting_for_title)
    await cb.answer()

@router.message(StateFilter(LinkForm.waiting_for_title))
@handle_error
async def process_title(message: types.Message, state: FSMContext):
    logger.info(f"Processing title from user {message.from_user.id}")
    title = sanitize_input(message.text)
    if not title:
        await message.answer("❌ Недействительно.\nПопробуйте снова:", reply_markup=cancel_kb)
        return
    data = await state.get_data()
    uid = str(message.from_user.id)
    db.execute('INSERT INTO links (user_id, title, short, original, created) VALUES (?, ?, ?, ?, ?)', (uid, title, data['short'], data['original'], datetime.datetime.now().isoformat()))
    stats_cache.pop(data['short'].split('/')[-1], None)
    await message.answer(f"✅ {title}\n{data['short']}\nЧто дальше?", parse_mode="HTML", reply_markup=get_post_add_menu())
    await state.update_data(last_added_entry={'title': title, 'short': data['short'], 'original': data['original']})
    await cleanup_chat(message, 2)
    await state.set_state(LinkForm.choosing_group)

@router.callback_query(F.data == "add_bulk")
@handle_error
async def add_bulk(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling add_bulk for user {cb.from_user.id}")
    await state.clear()
    await cb.message.edit_text("🔗 Введите ссылки по строкам:\nhttp://...\nОтмена: 🚫", parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.bulk_links)
    await cb.answer()

@router.message(StateFilter(LinkForm.bulk_links))
@handle_error
async def process_bulk_links(message: types.Message, state: FSMContext):
    logger.info(f"Processing bulk links from user {message.from_user.id}")
    valid = [l.strip() for l in message.text.splitlines() if l.strip() and is_valid_url(l)]
    if not valid:
        await message.answer("❌ Нет валидных ссылок.", reply_markup=cancel_kb)
        return
    await state.update_data(bulk_links=valid, success=[], failed=[])
    kb = make_kb([InlineKeyboardButton('📝 Вручную', callback_data='bulk_enter_titles'), InlineKeyboardButton('🔗 URL', callback_data='bulk_use_url'), InlineKeyboardButton('🚫 Отмена', callback_data='cancel')])
    await message.answer(f"✅ {len(valid)} ссылок.\nВыберите способ:", parse_mode="HTML", reply_markup=kb)
    await cleanup_chat(message)

@router.callback_query(F.data == "bulk_use_url")
@handle_error
async def bulk_use_url(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling bulk_use_url for user {cb.from_user.id}")
    data = await state.get_data()
    uid = str(cb.from_user.id)
    loading_msg = await bot.send_message(cb.message.chat.id, '⏳ Обрабатываем...')
    for url in data['bulk_links']:
        short, error_msg = await shorten_link_vk(url)
        if short: db.execute('INSERT INTO links (user_id, title, short, original, created) VALUES (?, ?, ?, ?, ?)', (uid, url[:50], short, url, datetime.datetime.now().isoformat()))
        else: data['failed'].append({'url': url, 'error': error_msg})
    await loading_msg.delete()
    report = f"✅ Обработано: {len(data['bulk_links']) - len(data.get('failed', []))}"
    if data.get('failed'): report += f"\n❌ {len(data['failed'])}\n{'" + "\n".join(f'🔗 {f['url']}: {f['error']}' for f in data['failed']) + "}"
    kb = make_kb([InlineKeyboardButton('📁 Папка', callback_data='bulk_to_group'), InlineKeyboardButton('🏠 Меню', callback_data='menu')])
    await cb.message.edit_text("{report}\nЧто дальше?", parse_mode="HTML", reply_markup=kb)
    await state.update_data(failed=data.get('failed', []))
    await state.set_state(LinkForm.bulk_to_group)
    await cb.answer()

@router.callback_query(F.data == "bulk_enter_titles")
@handle_error
async def bulk_enter_titles(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling bulk_enter_titles for user {cb.from_user.id}")
    data = await state.get_data()
    await state.update_data(bulk_index=0)
    await cb.message.edit_text(f"✏️ 1/{len(data['bulk_links'])}\n{data['bulk_links'][0]}\nВведите название:", parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.bulk_titles)
    await cb.answer()

@router.message(StateFilter(LinkForm.bulk_titles))
@handle_error
async def process_bulk_titles(message: types.Message, state: FSMContext):
    logger.info(f"Processing bulk titles from user {message.from_user.id}")
    data = await state.get_data()
    idx = data['bulk_index']
    url = data['bulk_links'][idx]
    title = sanitize_input(message.text)
    if not title:
        await message.answer(f"❌ Недействительно.\n{url}\nПопробуйте:", reply_markup=cancel_kb)
        return
    loading_msg = await message.answer('⏳ Обрабатываем...')
    uid = str(message.from_user.id)
    short, error_msg = await shorten_link_vk(url)
    if short:
        db.execute('INSERT INTO links (user_id, title, short, original, created) VALUES (?, ?, ?, ?, ?)', (uid, title, short, url, datetime.datetime.now().isoformat()))
        data['success'].append({'title': title, 'short': short, 'original': url})
    else: data['failed'].append({'url': url, 'error': error_msg})
    await loading_msg.delete()
    idx += 1
    if idx < len(data['bulk_links']):
        await state.update_data(bulk_index=idx)
        await message.answer(f"✏️ {idx+1}/{len(data['bulk_links'])}\n{data['bulk_links'][idx]}\nВведите:", parse_mode="HTML", reply_markup=cancel_kb)
    else:
        report = f"✅ {len(data['success'])}\n{'" + "\n".join(f'🔗 {s['title']} → {s['short']}' for s in data['success']) + "}"
        if data.get('failed'): report += "\n❌ {len(data['failed'])}\n{'" + "\n".join(f'🔗 {f['url']}: {f['error']}' for f in data['failed']) + "}"
        kb = make_kb([InlineKeyboardButton('📁 Папка', callback_data='bulk_to_group'), InlineKeyboardButton('🏠 Меню', callback_data='menu')])
        await message.answer("{report}\nЧто дальше?", parse_mode="HTML", reply_markup=kb)
        await cleanup_chat(message, 2)
    await state.update_data(success=data.get('success', []), failed=data.get('failed', []))

@router.callback_query(F.data == "bulk_to_group")
@handle_error
async def bulk_to_group(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling bulk_to_group for user {cb.from_user.id}")
    uid = str(cb.from_user.id)
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    if not groups:
        await cb.message.edit_text("❌ Нет папок.\nСоздайте.", parse_mode="HTML", reply_markup=get_groups_menu())
        await state.clear()
        return
    kb = make_kb([InlineKeyboardButton(f"📁 {g[0]}", callback_data=f'bulk_assign:{g[0]}') for g in groups], row_width=1, extra_buttons=[InlineKeyboardButton('➕ Новая', callback_data='create_group_in_flow'), InlineKeyboardButton('🚫 Пропустить', callback_data='bulk_skip_group'), InlineKeyboardButton('🏠 Меню', callback_data='menu')])
    await cb.message.edit_text("📁 Выберите папку:", parse_mode="HTML", reply_markup=kb)
    await state.set_state(LinkForm.bulk_to_group)
    await cb.answer()

@router.callback_query(F.data == "bulk_skip_group")
@handle_error
async def bulk_skip_group(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling bulk_skip_group for user {cb.from_user.id}")
    await cb.message.edit_text("✅ Без папки.\nЧто дальше?", parse_mode="HTML", reply_markup=get_links_menu())
    await state.clear()
    await cb.answer()

@router.callback_query(F.data.startswith("bulk_assign:"))
@handle_error
async def bulk_assign_to_group(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling bulk_assign for user {cb.from_user.id}, data={cb.data}")
    group_name = cb.data.split(':')[1]
    data = await state.get_data()
    uid = str(cb.from_user.id)
    success = data.get('success', [])
    if not success:
        await cb.message.edit_text("❌ Нет ссылок.", parse_mode="HTML", reply_markup=get_links_menu())
        await state.clear()
        return
    updated = sum(db.execute('UPDATE links SET group_name = ? WHERE user_id = ? AND short = ?', (group_name, uid, entry['short'])) for entry in success)
    text = f"✅ {updated} в \"{group_name}\"\n"
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, group_name))
    text += '" + "\n".join(f"🔗 {l[0]} → {l[1]}" for l in links) + " or '📚 Пусто.'
    kb = make_kb([InlineKeyboardButton('📁 Папки', callback_data='menu_groups'), InlineKeyboardButton('🏠 Меню', callback_data='menu')])
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.clear()
    await cb.answer()

@router.callback_query(F.data == "my_links")
@handle_error
async def my_links(cb: types.CallbackQuery, state: FSMContext):
    logger.info("Handling my_links for user {cb.from_user.id}")
    await state.clear()
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short, original FROM links WHERE user_id = ? AND group_name IS NULL', (uid,))
    if not links:
        await cb.message.edit_text("📋 Нет ссылок.\nДобавьте.", parse_mode="HTML", reply_markup=get_links_menu())
        await cb.answer()
        return
    link_list = [{'title': r[0], 'short': r[1], 'original': r[2]} for r in links]
    stats = await asyncio.gather(*(get_link_stats(l['short'].split('/')[-1]) for l in link_list))
    buttons = [InlineKeyboardButton(f"🔗 {l['title']} ({stats[i]['views']})", callback_data=f'link_action:root:{i}') for i, l in enumerate(link_list)]
    kb = make_kb(buttons, row_width=1, extra_buttons=[InlineKeyboardButton('📁 Папка', callback_data='select_links_for_group'), InlineKeyboardButton('🏠 Меню', callback_data='menu')])
    await cb.message.edit_text("🔗 Ссылки:\nВыберите:", parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("link_action:"))
@handle_error
async def link_action(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling link_action for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    _, scope, idx = cb.data.split(':')
    idx = int(idx)
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short, original, group_name FROM links WHERE user_id = ? AND group_name IS NULL', (uid,)) if scope == 'root' else db.execute('SELECT title, short, original, group_name FROM links WHERE user_id = ? AND group_name = ?', (uid, scope))
    link = {'title': links[idx][0], 'short': links[idx][1], 'original': links[idx][2], 'group_name': links[idx][3]}
    back_data = 'my_links' if scope == 'root' else f'view_group:{scope}'
    path = '🔗 Ссылки' if scope == 'root' else f'📁 {scope}'
    kb = make_kb([InlineKeyboardButton('📊 Статистика', callback_data=f'single_link_stats:{scope}:{idx}'), InlineKeyboardButton('✍ Переименовать', callback_data=f'rename:{scope}:{idx}'), InlineKeyboardButton('🗑 Удалить', callback_data=f'confirm_delete:{scope}:{idx}'), InlineKeyboardButton('📁 Папка', callback_data=f'togroup:{scope}:{idx}'), InlineKeyboardButton('🏠 Меню', callback_data='menu'), InlineKeyboardButton('⬅ Назад', callback_data=back_data)])
    await cb.message.edit_text(f"{path}\n🔗 {link['title']}\n{link['short']}\n{link['original']}\nВыберите:", parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("togroup:"))
@handle_error
async def togroup(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling togroup for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    _, scope, idx = cb.data.split(':')
    idx = int(idx)
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short, original FROM links WHERE user_id = ? AND group_name IS NULL', (uid,)) if scope == 'root' else db.execute('SELECT title, short, original FROM links WHERE user_id = ? AND group_name = ?', (uid, scope))
    link = {'title': links[idx][0], 'short': links[idx][1], 'original': links[idx][2]}
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    if not groups:
        await cb.message.edit_text("❌ Нет папок.\nСоздайте.", parse_mode="HTML", reply_markup=get_groups_menu())
        await state.clear()
        return
    await state.update_data(togroup_link=link)
    kb = make_kb([InlineKeyboardButton(f"📁 {g[0]}", callback_data=f'assign:{g[0]}') for g in groups], row_width=1, extra_buttons=[InlineKeyboardButton('➕ Новая', callback_data='create_group_in_flow'), InlineKeyboardButton('🚫 Отмена', callback_data='cancel')])
    await cb.message.edit_text(f"📁 Куда \"{link['title']}\"?", parse_mode="HTML", reply_markup=kb)
    await state.set_state(LinkForm.choosing_group)
    await cb.answer()

@router.callback_query(F.data.startswith("assign:"))
@handle_error
async def assign_to_group_single(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling assign for user {cb.from_user.id}, data={cb.data}")
    group_name = cb.data.split(':')[1]
    data = await state.get_data()
    link = data.get('togroup_link')
    if not link:
        await cb.message.edit_text("❌ Ссылка не найдена.", parse_mode="HTML", reply_markup=get_links_menu())
        await state.clear()
        return
    uid = str(cb.from_user.id)
    try:
        with sqlite3.connect(db.db_name) as conn:
            c = conn.cursor()
            c.execute('UPDATE links SET group_name = ? WHERE user_id = ? AND short = ?', (group_name, uid, link['short']))
            conn.commit()
            if c.rowcount == 0: raise ValueError
    except Exception as e:
        logger.error(f"Error assigning link: {e}")
        await cb.message.edit_text("❌ Ошибка.", parse_mode="HTML", reply_markup=get_links_menu())
        await state.clear()
        return
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, group_name))
    text = f"✅ В \"{group_name}\"\n{'" + "\n".join(f'🔗 {l[0]} → {l[1]}' for l in links) + " or '📚 Пусто.'}"
    kb = make_kb([InlineKeyboardButton('📁 Папки', callback_data='menu_groups'), InlineKeyboardButton('🏠 Меню', callback_data='menu')])
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.clear()
    await cb.answer()

@router.callback_query(F.data == "ask_to_group")
@handle_error
async def ask_to_group(cb: types.CallbackQuery, state: FSMContext):
    logger.info("Handling ask_to_group for user {cb.from_user.id}")
    uid = str(cb.from_user.id)
    data = await state.get_data()
    if not data.get('last_added_entry'):
        await cb.message.edit_text("❌ Ссылка не найдена.", parse_mode="HTML", reply_markup=get_links_menu())
        await state.clear()
        return
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    if not groups:
        await cb.message.edit_text("❌ Нет папок.\nСоздайте.", parse_mode="HTML", reply_markup=get_groups_menu())
        await state.clear()
        return
    kb = make_kb([InlineKeyboardButton(f"📁 {g[0]}", callback_data=f'single_assign:{g[0]}') for g in groups], row_width=1, extra_buttons=[InlineKeyboardButton('➕ Новая', callback_data='create_group_in_flow'), InlineKeyboardButton('🚫 Пропустить', callback_data='skip_group'), InlineKeyboardButton('🏠 Меню', callback_data='menu')])
    await cb.message.edit_text("📁 В папку?\nВыберите:", parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "skip_group")
@handle_error
async def skip_group(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling skip_group for user {cb.from_user.id}")
    await cb.message.edit_text("✅ Без папки.\nЧто дальше?", parse_mode="HTML", reply_markup=get_links_menu())
    await state.clear()
    await cb.answer()

@router.callback_query(F.data.startswith("single_assign:"))
@handle_error
async def single_assign_to_group(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling single_assign for user {cb.from_user.id}, data={cb.data}")
    group_name = cb.data.split(':')[1]
    data = await state.get_data()
    entry = data.get('last_added_entry')
    if not entry:
        await cb.message.edit_text("❌ Ссылка не найдена.", parse_mode="HTML", reply_markup=get_links_menu())
        await state.clear()
        return
    uid = str(cb.from_user.id)
    try:
        with sqlite3.connect(db.db_name) as conn:
            c = conn.cursor()
            c.execute('UPDATE links SET group_name = ? WHERE user_id = ? AND short = ?', (group_name, uid, entry['short']))
            conn.commit()
            if c.rowcount == 0: raise ValueError
    except Exception as e:
        logger.error(f"Error assigning single link: {e}")
        await cb.message.edit_text("❌ Ошибка.", parse_mode="HTML", reply_markup=get_links_menu())
        await state.clear()
        return
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, group_name))
    text = f"✅ В \"{group_name}\"\n{'" + "\n".join(f'🔗 {l[0]} → {l[1]}' for l in links) + " or '📚 Пусто.'}"
    kb = make_kb([InlineKeyboardButton('📁 Папки', callback_data='menu_groups'), InlineKeyboardButton('🏠 Меню', callback_data='menu')])
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await state.clear()
    await cb.answer()

@router.callback_query(F.data == "create_group_in_flow")
@handle_error
async def create_group_in_flow(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling create_group_in_flow for user {cb.from_user.id}")
    await cb.message.edit_text("📁 Введите название (до 100):", parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.creating_group)
    await cb.answer()

@router.message(StateFilter(LinkForm.creating_group))
@handle_error
async def process_create_group(message: types.Message, state: FSMContext):
    logger.info(f"Processing create group from user {message.from_user.id}")
    name = sanitize_input(message.text)
    if not name:
        await message.answer("❌ Некорректно.\nПопробуйте:", reply_markup=cancel_kb)
        return
    uid = str(message.from_user.id)
    if any(g[0] == name for g in db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))):
        await message.answer("❌ Уже есть.\nВведите другое:", reply_markup=cancel_kb)
        return
    db.execute('INSERT INTO groups (user_id, name) VALUES (?, ?)', (uid, name))
    data = await state.get_data()
    entry = data.get('last_added_entry') or data.get('togroup_link')
    text = f"✅ Папка \"{name}\" создана."
    if entry:
        try:
            with sqlite3.connect(db.db_name) as conn:
                c = conn.cursor()
                c.execute('UPDATE links SET group_name = ? WHERE user_id = ? AND short = ?', (name, uid, entry['short']))
                conn.commit()
                if c.rowcount == 0: raise ValueError
            stats = await get_link_stats(entry['short'].split('/')[-1])
            text += f"\n🔗 {entry['title']}: {stats['views']}"
        except Exception as e:
            logger.error(f"Error assigning link: {e}")
            text += "\n❌ Не добавлена."
    await message.answer(text, parse_mode="HTML", reply_markup=get_groups_menu())
    await cleanup_chat(message)
    await state.clear()

@router.callback_query(F.data.startswith("confirm_delete:"))
@handle_error
async def confirm_delete_link(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling confirm_delete for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    _, scope, idx = cb.data.split(':')
    idx = int(idx)
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name IS NULL', (uid,)) if scope == 'root' else db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, scope))
    link = {'title': links[idx][0], 'short': links[idx][1]}
    back_data = 'my_links' if scope == 'root' else f'view_group:{scope}'
    await state.update_data(delete_scope=scope, delete_idx=idx, delete_short=link['short'])
    kb = make_kb([InlineKeyboardButton('✅ Удалить', callback_data=f'do_delete:{scope}:{idx}'), InlineKeyboardButton('🚫 Отмена', callback_data=back_data)])
    await cb.message.edit_text(f"⚠️ Удалить?\n{link['title']}\n{link['short']}", parse_mode="HTML", reply_markup=kb)
    await state.set_state(LinkForm.confirm_delete_link)
    await cb.answer()

@router.callback_query(F.data.startswith("do_delete:"))
@handle_error
async def do_delete(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling do_delete for user {cb.from_user.id}, data={cb.data}")
    _, scope, idx = cb.data.split(':')
    idx = int(idx)
    uid = str(cb.from_user.id)
    data = await state.get_data()
    db.execute('DELETE FROM links WHERE user_id = ? AND short = ?', (uid, data['delete_short']))
    stats_cache.pop(data['delete_short'].split('/')[-1], None)
    back_data = 'my_links' if scope == 'root' else f'view_group:{scope}'
    kb = make_kb([InlineKeyboardButton('⬅ Назад', callback_data=back_data)])
    await cb.message.edit_text("✅ Удалено. Что дальше?", parse_mode="HTML", reply_markup=kb)
    await state.clear()
    await cb.answer()

@router.callback_query(F.data.startswith("rename:"))
@handle_error
async def rename_link(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling rename for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    _, scope, idx = cb.data.split(':')
    idx = int(idx)
    uid = str(cb.from_user.id)
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name IS NULL', (uid,)) if scope == 'root' else db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, scope))
    link = {'title': links[idx][0], 'short': links[idx][1]}
    await state.update_data(rename_link_short=link['short'], rename_scope=scope)
    await cb.message.edit_text(f"✍ {link['title']}\n{link['short']}\nВведите новое:", parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.rename_link)
    await cb.answer()

@router.message(StateFilter(LinkForm.rename_link))
@handle_error
async def process_rename_link(message: types.Message, state: FSMContext):
    logger.info(f"Processing rename link from user {message.from_user.id}")
    title = sanitize_input(message.text)
    if not title:
        await message.answer("❌ Недействительно.\nПопробуйте:", reply_markup=cancel_kb)
        return
    data = await state.get_data()
    short, scope = data['rename_link_short'], data['rename_scope']
    uid = str(message.from_user.id)
    db.execute('UPDATE links SET title = ? WHERE user_id = ? AND short = ?', (title, uid, short))
    back_data = 'my_links' if scope == 'root' else f'view_group:{scope}'
    kb = make_kb([InlineKeyboardButton('⬅ Назад', callback_data=back_data)])
    await message.answer(f"✅ \"{title}\". Что дальше?", parse_mode="HTML", reply_markup=kb)
    await cleanup_chat(message)
    await state.clear()

@router.callback_query(F.data == "create_group")
@handle_error
async def create_group(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling create_group for user {cb.from_user.id}")
    await state.clear()
    await cb.message.edit_text("📁 Введите название (до 100):", parse_mode="HTML", reply_markup=cancel_kb)
    await state.set_state(LinkForm.creating_group)
    await cb.answer()

@router.callback_query(F.data == "show_groups")
@handle_error
async def show_groups(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling show_groups for user {cb.from_user.id}")
    await state.clear()
    uid = str(cb.from_user.id)
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    if not groups:
        await cb.message.edit_text("📁 Нет папок.\nСоздайте.", parse_mode="HTML", reply_markup=get_groups_menu())
        await cb.answer()
        return
    buttons = [InlineKeyboardButton(f"📁 {g[0]}", callback_data=f'view_group:{g[0]}') for g in groups]
    kb = make_kb(buttons, row_width=1, extra_buttons=[InlineKeyboardButton('🔗 Ссылки', callback_data='my_links'), InlineKeyboardButton('🏠 Меню', callback_data='menu')])
    await cb.message.edit_text("📁 Папки:\nВыберите:", parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("view_group:"))
@handle_error
async def view_group(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling view_group for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    name = cb.data.split(':')[1]
    uid = str(cb.from_user.id)
    groups = db.execute('SELECT name FROM groups WHERE user_id = ? AND name = ?', (uid, name))
    if not groups:
        await cb.message.edit_text("❌ Папка не найдена.", parse_mode="HTML", reply_markup=get_groups_menu())
        await cb.answer()
        return
    text = f"📁 {name}\n"
    links = db.execute('SELECT title, short FROM links WHERE user_id = ? AND group_name = ?', (uid, name))
    items = [{'title': l[0], 'short': l[1]} for l in links]
    buttons = []
    if not items: text += '📚 Пусто.'
    else:
        stats = await asyncio.gather(*(get_link_stats(l['short'].split('/')[-1]) for l in items))
        buttons.extend(InlineKeyboardButton(f"🔗 {l['title']} ({stats[i]['views']})", callback_data=f'link_action:{name}:{i}') for i, l in enumerate(items))
    kb = make_kb(buttons, row_width=1, extra_buttons=[InlineKeyboardButton('📊 Статистика', callback_data=f'show_stats:{name}'), InlineKeyboardButton('🏠 Меню', callback_data='menu'), InlineKeyboardButton('⬅ Назад', callback_data='show_groups')])
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "del_group")
@handle_error
async def del_group(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling del_group for user {cb.from_user.id}")
    await state.clear()
    uid = str(cb.from_user.id)
    groups = db.execute('SELECT name FROM groups WHERE user_id = ?', (uid,))
    if not groups:
        await cb.message.edit_text("📁 Нет папок.\nСоздайте.", parse_mode="HTML", reply_markup=get_groups_menu())
        await cb.answer()
        return
    kb = make_kb([InlineKeyboardButton(f"🗑 {g[0]}", callback_data=f"confirm_delete_group:{g[0]}") for g in groups], row_width=1, extra_buttons=[InlineKeyboardButton('🏠 Меню', callback_data='menu')])
    await cb.message.edit_text("📁 Удалить:\nВыберите:", parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("confirm_delete_group:"))
@handle_error
async def confirm_delete_group(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling confirm_delete_group for user {cb.from_user.id}, data={cb.data}")
    await state.clear()
    group_name = cb.data.split(':')[1]
    uid = str(cb.from_user.id)
    groups = db.execute('SELECT name FROM groups WHERE user_id = ? AND name = ?', (uid, group_name))
    if not groups:
        await cb.message.edit_text("❌ Папка не найдена.", parse_mode="HTML", reply_markup=get_groups_menu())
        await cb.answer()
        return
    await state.update_data(group_to_delete=group_name)
    kb = make_kb([InlineKeyboardButton('✅ Удалить', callback_data=f'do_delete_group:{group_name}'), InlineKeyboardButton('🚫 Отмена', callback_data='show_groups')])
    await cb.message.edit_text(f"⚠️ Удалить \"{group_name}\"? Ссылки в корень.", parse_mode="HTML", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data.startswith("do_delete_group:"))
@handle_error
async def do_delete_group(cb: types.CallbackQuery, state: FSMContext):
    logger.info(f"Handling do_delete_group for user {cb.from_user.id}, data={cb.data}")
    group_name = cb.data.split(':')[1]
    uid = str(cb.from_user.id)
    try:
        with sqlite3.connect(db.db_name) as conn:
            c = conn.cursor()
            c.execute('UPDATE links SET group_name = NULL WHERE user_id = ? AND group_name = ?', (uid, group_name))
            c.execute('DELETE FROM groups WHERE user_id = ? AND name = ?', (uid, group_name))
            conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error deleting group: {e}")
        await cb.message.edit_text("❌ Ошибка удаления.", parse_mode="HTML", reply_markup=get_groups_menu())
        await state.clear()
        return
    await cb.message.edit_text(f"✅ \"{group_name}\" удалена.", parse_mode="HTML", reply_markup=get_groups_menu())
    await state.clear()
    await cb.answer()

async def main():
    logger.info("Starting bot...")
    try:
        await dp.start_polling()
    except Exception as e:
        logger.error(f"Bot failed: {e}")
    finally:
        await dp.storage.close()
        await dp.storage.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())

# 👇 Новый хендлер: нажата кнопка "Одна ссылка"
@router.callback_query(F.data == "add_single")
@handle_error
async def add_single_link(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("🔗 Введите ссылку для сокращения:", reply_markup=cancel_kb)
    await state.set_state(LinkForm.waiting_for_link)

# 👇 Пользователь вводит ссылку
@router.message(StateFilter(LinkForm.waiting_for_link))
@handle_error
async def process_link_input(message: types.Message, state: FSMContext):
    url = message.text.strip()
    if not url.startswith("http"):
        await message.answer("❌ Неверная ссылка. Введите корректную.", reply_markup=cancel_kb)
        return
    await state.update_data(link=url)
    await message.answer("📝 Введите заголовок для ссылки:", reply_markup=cancel_kb)
    await state.set_state(LinkForm.waiting_for_title)

# 👇 Пользователь вводит заголовок
@router.message(StateFilter(LinkForm.waiting_for_title))
@handle_error
async def process_link_title(message: types.Message, state: FSMContext):
    data = await state.get_data()
    url = data.get("link")
    title = message.text.strip()
    short_link = await shorten_link(url)
    db.execute("INSERT INTO links (user_id, title, short, original) VALUES (?, ?, ?, ?)", (
        str(message.from_user.id), title, short_link, url))
    await message.answer(f"✅ Ссылка сохранена:
<b>{title}</b>
{short_link}", parse_mode="HTML", reply_markup=get_links_menu())
    await state.clear()

# 👇 Обработка массовых ссылок
@router.message(StateFilter(LinkForm.bulk_links))
@handle_error
async def process_bulk_links(message: types.Message, state: FSMContext):
    raw_links = message.text.strip().splitlines()
    added = []
    for line in raw_links:
        if line.startswith("http"):
            short = await shorten_link(line)
            db.execute("INSERT INTO links (user_id, title, short, original) VALUES (?, ?, ?, ?)", (
                str(message.from_user.id), line[:30] + "...", short, line))
            added.append(short)
    if not added:
        await message.answer("❌ Не удалось сократить ни одной ссылки.", reply_markup=get_links_menu())
    else:
        await message.answer(f"✅ Сокращено ссылок: {len(added)}", reply_markup=get_links_menu())
    await state.clear()


# 👇 Нажата кнопка "Создать папку"
@router.callback_query(F.data == "create_group")
@handle_error
async def create_group(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("📁 Введите название новой папки:", reply_markup=cancel_kb)
    await state.set_state(LinkForm.waiting_for_group_name)

# 👇 Пользователь вводит название папки
@router.message(StateFilter(LinkForm.waiting_for_group_name))
@handle_error
async def save_group_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    uid = str(message.from_user.id)
    db.execute("INSERT INTO groups (user_id, name) VALUES (?, ?)", (uid, name))
    await message.answer(f"✅ Папка «{name}» создана.", reply_markup=get_groups_menu())
    await state.clear()

# 👇 Просмотр папок
@router.callback_query(F.data == "view_groups")
@handle_error
async def view_groups(callback: CallbackQuery):
    uid = str(callback.from_user.id)
    groups = db.execute("SELECT id, name FROM groups WHERE user_id = ?", (uid,))
    if not groups:
        await callback.message.edit_text("📂 У вас пока нет папок.", reply_markup=get_groups_menu())
        return
    text = "📂 Ваши папки:

" + "\n".join(f"📁 {g[1]}" for g in groups)
    await callback.message.edit_text(text, reply_markup=get_groups_menu())

# 👇 Удаление папки — выбор
@router.callback_query(F.data == "delete_group")
@handle_error
async def delete_group(callback: CallbackQuery, state: FSMContext):
    uid = str(callback.from_user.id)
    groups = db.execute("SELECT id, name FROM groups WHERE user_id = ?", (uid,))
    if not groups:
        await callback.message.edit_text("❌ У вас нет папок для удаления.", reply_markup=get_groups_menu())
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=g[1], callback_data=f"confirm_delete_group:{g[0]}")] for g in groups
    ] + [[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]])
    await callback.message.edit_text("❓ Выберите папку для удаления:", reply_markup=kb)

# 👇 Удаление папки — финальное
@router.callback_query(F.data.startswith("confirm_delete_group:"))
@handle_error
async def do_delete_group(callback: CallbackQuery):
    gid = int(callback.data.split(":")[1])
    db.execute("DELETE FROM groups WHERE id = ?", (gid,))
    await callback.message.edit_text("🗑 Папка удалена.", reply_markup=get_groups_menu())


# 👇 Кнопка «📁 Переместить» возле ссылки
@router.callback_query(F.data.startswith("move_link:"))
@handle_error
async def choose_group_to_move(callback: CallbackQuery, state: FSMContext):
    link_id = int(callback.data.split(":")[1])
    uid = str(callback.from_user.id)
    groups = db.execute("SELECT id, name FROM groups WHERE user_id = ?", (uid,))
    if not groups:
        await callback.message.answer("❌ У вас нет папок. Сначала создайте.", reply_markup=get_links_menu())
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=g[1], callback_data=f"confirm_move:{link_id}:{g[0]}")] for g in groups
    ] + [[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]])
    await callback.message.edit_text("📁 В какую папку переместить ссылку?", reply_markup=kb)

# 👇 Подтверждение перемещения
@router.callback_query(F.data.startswith("confirm_move:"))
@handle_error
async def move_link(callback: CallbackQuery):
    _, link_id, group_id = callback.data.split(":")
    db.execute("UPDATE links SET group_id = ? WHERE id = ?", (group_id, link_id))
    await callback.message.edit_text("✅ Ссылка перемещена в выбранную папку.", reply_markup=get_links_menu())


# 👇 Пользователь выбирает диапазон дат для статистики
@router.message(StateFilter(LinkForm.waiting_for_stats_date))
@handle_error
async def process_stats_date(message: types.Message, state: FSMContext):
    logger.info(f"Processing stats date from user {message.from_user.id}")
    dates = message.text.strip().split()
    if len(dates) != 2 or not all(re.match(r"\d{4}-\d{2}-\d{2}", d) for d in dates):
        await message.answer("❌ Неверный формат. Введите даты в формате YYYY-MM-DD YYYY-MM-DD", reply_markup=cancel_kb)
        return
    date_from, date_to = dates
    uid = str(message.from_user.id)
    links = db.execute('SELECT id, title, short FROM links WHERE user_id = ?', (uid,))
    if not links:
        await message.answer("📋 У вас пока нет ссылок.", reply_markup=get_stats_menu())
        await state.clear()
        return
    loading_msg = await message.answer("⏳ Загружаем статистику...")

    stats = await asyncio.gather(*(get_link_stats(l[2].split("/")[-1], date_from, date_to) for l in links))
    all_cities = {cid: sum(s['cities'].get(cid, 0) for s in stats) for cid in {c for s in stats for c in s['cities']}}
    city_names = await get_city_names(list(all_cities))

    text = f"📊 Статистика за {date_from}—{date_to}\n\n"
    for i, l in enumerate(links):
        text += f"🔗 {l[1]} — {stats[i]['views']} просмотров\n"

    total_views = sum(s['views'] for s in stats)
    text += f"\n👁 Всего просмотров: {total_views}\n"

    if all_cities:
        city_lines = [f"- {city_names.get(cid, 'Неизв.')}: {views}" for cid, views in all_cities.items()]
        text += "\n🏙 Города:\n" + "\n".join(city_lines)
    else:
        text += "\n🏙 Нет данных по городам."

    await loading_msg.delete()
    await message.answer(text.strip(), parse_mode="HTML", reply_markup=get_stats_menu())
    await cleanup_chat(message)
    await state.clear()


# 👇 Пользователь нажимает «📊 По ссылке» в меню
@router.callback_query(F.data == "stats_by_link")
@handle_error
async def choose_link_for_stats(callback: CallbackQuery, state: FSMContext):
    uid = str(callback.from_user.id)
    links = db.execute("SELECT id, title FROM links WHERE user_id = ?", (uid,))
    if not links:
        await callback.message.edit_text("❌ У вас пока нет ссылок.", reply_markup=get_stats_menu())
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=l[1], callback_data=f"stats_link_dates:{l[0]}")] for l in links
    ] + [[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]])
    await callback.message.edit_text("🔗 Выберите ссылку для просмотра статистики:", reply_markup=kb)

# 👇 Запрашиваем даты
@router.callback_query(F.data.startswith("stats_link_dates:"))
@handle_error
async def ask_dates_for_one_link(callback: CallbackQuery, state: FSMContext):
    link_id = int(callback.data.split(":")[1])
    await state.update_data(link_id=link_id)
    await state.set_state(LinkForm.waiting_for_stats_date_one)
    await callback.message.edit_text("📅 Введите диапазон дат в формате YYYY-MM-DD YYYY-MM-DD", reply_markup=cancel_kb)

# 👇 Обработка введённых дат
@router.message(StateFilter(LinkForm.waiting_for_stats_date_one))
@handle_error
async def process_dates_for_one_link(message: types.Message, state: FSMContext):
    dates = message.text.strip().split()
    if len(dates) != 2 or not all(re.match(r"\d{4}-\d{2}-\d{2}", d) for d in dates):
        await message.answer("❌ Неверный формат. Введите две даты через пробел.", reply_markup=cancel_kb)
        return
    data = await state.get_data()
    link_id = data.get("link_id")
    link = db.execute("SELECT title, short FROM links WHERE id = ?", (link_id,))
    if not link:
        await message.answer("❌ Ссылка не найдена.", reply_markup=get_stats_menu())
        await state.clear()
        return
    date_from, date_to = dates
    short = link[0][1].split("/")[-1]
    loading = await message.answer("⏳ Считаем...")
    stats = await get_link_stats(short, date_from, date_to)
    city_names = await get_city_names(list(stats["cities"].keys()))
    text = f"📊 Статистика за {date_from}—{date_to}\n"
    text += f"🔗 {link[0][0]} — {stats['views']} кликов"
    if stats["cities"]:
        city_lines = [f"- {city_names.get(cid, 'Неизв.')}: {views}" for cid, views in stats["cities"].items()]
        text += "\n\n🏙 Города:\n" + "\n".join(city_lines)
    else:
        text += "\n\n🏙 Нет данных по городам."
    await loading.delete()
    await message.answer(text, parse_mode="HTML", reply_markup=get_stats_menu())
    await cleanup_chat(message)
    await state.clear()


from datetime import datetime, timedelta

# 📁 Пользователь нажимает «📊 По папке»
@router.callback_query(F.data == "stats_by_group")
@handle_error
async def choose_group_for_stats(callback: CallbackQuery, state: FSMContext):
    uid = str(callback.from_user.id)
    groups = db.execute("SELECT DISTINCT group_id FROM links WHERE user_id = ? AND group_id IS NOT NULL", (uid,))
    if not groups:
        await callback.message.edit_text("❌ У вас пока нет папок.", reply_markup=get_stats_menu())
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📂 {g[0]}", callback_data=f"group_stats:{g[0]}")] for g in groups
    ] + [[InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]])
    await callback.message.edit_text("📁 Выберите папку:", reply_markup=kb)

# ⏳ Запрашиваем даты для выбранной группы
@router.callback_query(F.data.startswith("group_stats:"))
@handle_error
async def ask_dates_for_group(callback: CallbackQuery, state: FSMContext):
    group_id = callback.data.split(":")[1]
    await state.update_data(group_id=group_id)
    await state.set_state(LinkForm.waiting_for_stats_date_group)
    await callback.message.edit_text("📅 Введите даты в формате YYYY-MM-DD YYYY-MM-DD", reply_markup=cancel_kb)

# 📊 Статистика по выбранной группе
@router.message(StateFilter(LinkForm.waiting_for_stats_date_group))
@handle_error
async def process_group_stats(message: types.Message, state: FSMContext):
    dates = message.text.strip().split()
    if len(dates) != 2 or not all(re.match(r"\d{4}-\d{2}-\d{2}", d) for d in dates):
        await message.answer("❌ Неверный формат. Введите две даты через пробел.", reply_markup=cancel_kb)
        return
    data = await state.get_data()
    uid = str(message.from_user.id)
    group_id = data.get("group_id")
    links = db.execute("SELECT title, short FROM links WHERE user_id = ? AND group_id = ?", (uid, group_id))
    if not links:
        await message.answer("📂 В этой папке нет ссылок.", reply_markup=get_stats_menu())
        await state.clear()
        return
    date_from, date_to = dates
    loading = await message.answer("⏳ Загружаем...")
    stats = await asyncio.gather(*(get_link_stats(l[1].split("/")[-1], date_from, date_to) for l in links))
    all_cities = {cid: sum(s['cities'].get(cid, 0) for s in stats) for cid in {c for s in stats for c in s['cities']}}
    city_names = await get_city_names(list(all_cities))
    text = f"📊 Папка: {group_id}\nЗа {date_from}—{date_to}\n"
    text += "\n".join(f"🔗 {l[0]} — {stats[i]['views']} кликов" for i, l in enumerate(links))
    text += f"\n\n👁 Всего: {sum(s['views'] for s in stats)}"
    if all_cities:
        city_lines = [f"- {city_names.get(cid, 'Неизв.')}: {views}" for cid, views in all_cities.items()]
        text += "\n\n🏙 Города:\n" + "\n".join(city_lines)
    else:
        text += "\n\n🏙 Нет данных по городам."
    await loading.delete()
    await message.answer(text, parse_mode="HTML", reply_markup=get_stats_menu())
    await cleanup_chat(message)
    await state.clear()

# 🗓 Быстрая статистика за 7 дней по всем ссылкам
@router.callback_query(F.data == "quick_stats_7d")
@handle_error
async def quick_stats(callback: CallbackQuery, state: FSMContext):
    uid = str(callback.from_user.id)
    links = db.execute("SELECT title, short FROM links WHERE user_id = ?", (uid,))
    if not links:
        await callback.message.edit_text("❌ У вас нет ссылок.", reply_markup=get_stats_menu())
        return
    date_to = datetime.utcnow().date()
    date_from = date_to - timedelta(days=7)
    loading = await callback.message.answer("⏳ Считаем за последние 7 дней...")
    stats = await asyncio.gather(*(get_link_stats(l[1].split("/")[-1], str(date_from), str(date_to)) for l in links))
    all_cities = {cid: sum(s['cities'].get(cid, 0) for s in stats) for cid in {c for s in stats for c in s['cities']}}
    city_names = await get_city_names(list(all_cities))
    text = f"📊 Статистика за {date_from}—{date_to}\n"
    text += "\n".join(f"🔗 {l[0]} — {stats[i]['views']} кликов" for i, l in enumerate(links))
    text += f"\n\n👁 Всего: {sum(s['views'] for s in stats)}"
    if all_cities:
        city_lines = [f"- {city_names.get(cid, 'Неизв.')}: {views}" for cid, views in all_cities.items()]
        text += "\n\n🏙 Города:\n" + "\n".join(city_lines)
    else:
        text += "\n\n🏙 Нет данных по городам."
    await loading.delete()
    await callback.message.answer(text, parse_mode="HTML", reply_markup=get_stats_menu())
