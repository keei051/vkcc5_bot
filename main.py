import os
import asyncio
import sqlite3
import re
from datetime import datetime
from urllib.parse import quote
from loguru import logger
import aiohttp
from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# Настройка логгера
logger.add("bot.log", rotation="1 MB")
logger.info("🚀 Бот запускается")

# Получение токенов
BOT_TOKEN = os.getenv("BOT_TOKEN") or "8177494043:AAG0CE7HrXB17uIiqRewNvJemmITi3ShLUc"
VK_TOKEN = os.getenv("VK_API_TOKEN") or "b4a1b020b4a1b020b4a1b020b5b794c059bb4a1b4a1b020dcc370dd89310543ea6e73ed"

if not BOT_TOKEN or not VK_TOKEN:
    logger.error("Токены не установлены")
    raise ValueError("BOT_TOKEN и VK_TOKEN должны быть установлены")

# Инициализация бота
bot = Bot(BOT_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
router = Router()
dp.include_router(router)

# Класс состояний
class LinkForm(StatesGroup):
    waiting_for_link = State()
    waiting_for_title = State()
    waiting_for_stats_date = State()

# Класс для работы с базой данных
class Database:
    def __init__(self, db_name='links.db'):
        self.db_name = db_name
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_name) as conn:
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS links (
                user_id TEXT, title TEXT, short TEXT, original TEXT, created TEXT
            )''')
            conn.commit()

    def execute(self, query, params=()):
        try:
            with sqlite3.connect(self.db_name) as conn:
                c = conn.cursor()
                c.execute(query, params)
                conn.commit()
                return c.fetchall() if query.upper().startswith('SELECT') else c.rowcount
        except sqlite3.Error as e:
            logger.error(f"Ошибка базы данных: {e}")
            raise

db = Database()

# Проверка валидности URL
def is_valid_url(url):
    return bool(re.match(r'^https?://[^\s/$.?#].[^\s]*$', url, re.IGNORECASE))

# Функция сокращения ссылки через VK API
async def shorten_link_vk(url):
    if not is_valid_url(url):
        return None, "Недействительный URL"
    encoded_url = quote(url, safe='')
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://api.vk.com/method/utils.getShortLink?url={encoded_url}&v=5.199&access_token={VK_TOKEN}",
                timeout=10
            ) as resp:
                data = await resp.json()
                if 'response' in data and 'short_url' in data['response']:
                    return data['response']['short_url'], ""
                error_msg = data.get('error', {}).get('error_msg', 'Неизвестная ошибка')
                logger.error(f"Ошибка VK API: {error_msg}")
                return None, f"Ошибка: {error_msg}"
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка при сокращении ссылки: {e}")
        return None, f"Не удалось сократить: {str(e)[:50]}"

# Функция получения статистики по ссылке
async def get_link_stats(key, date_from=None, date_to=None):
    params = {"access_token": VK_TOKEN, "key": key, "v": "5.199", "extended": 1, "interval": "day"}
    if date_from and date_to:
        params.update({"date_from": date_from, "date_to": date_to})
    result = {"clicks": 0}  # Называем "clicks" для удобства, хотя это views
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.vk.com/method/utils.getLinkStats",
                params=params,
                timeout=10
            ) as resp:
                data = await resp.json()
                if "response" in data and "stats" in data["response"]:
                    for period in data["response"]["stats"]:
                        result["clicks"] += period.get("views", 0)  # Используем views как клики
                    return result
                logger.error(f"Ошибка VK API: {data.get('error', 'Нет данных')}")
                return result
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка получения статистики: {e}")
        return result

# Создание клавиатуры
def make_kb(buttons, row_width=2):
    keyboard = [buttons[i:i + row_width] for i in range(0, len(buttons), row_width)]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# Главное меню
def get_main_menu():
    return make_kb([
        InlineKeyboardButton('🔗 Сократить ссылку', callback_data='add_link'),
        InlineKeyboardButton('📊 Статистика переходов', callback_data='stats'),
    ])

# Клавиатура отмены
cancel_kb = make_kb([InlineKeyboardButton('🚫 Отмена', callback_data='cancel')])

# Декоратор обработки ошибок
def handle_error(handler):
    async def wrapper(*args, **kwargs):
        try:
            return await handler(*args, **kwargs)
        except Exception as e:
            logger.error(f"Ошибка в {handler.__name__}: {e}")
            text = f"❌ Ошибка: {str(e)[:50]}"
            reply = get_main_menu()
            if isinstance(args[0], types.CallbackQuery):
                await args[0].message.edit_text(text, reply_markup=reply)
                await args[0].answer()
            elif isinstance(args[0], types.Message):
                await args[0].answer(text, reply_markup=reply)
    return wrapper

# Обработчики
@router.message(Command("start"))
@handle_error
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "✨ Добро пожаловать! ✨\n🔗 Сокращайте ссылки\n📊 Смотрите статистику переходов\n\n⚠️ Примечание: Статистика показывает переходы, но VK API учитывает просмотры, что может включать не только клики.",
        reply_markup=get_main_menu()
    )

@router.callback_query(lambda c: c.data == "cancel")
@handle_error
async def cancel_action(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("✅ Отменено", reply_markup=get_main_menu())
    await cb.answer()

@router.callback_query(lambda c: c.data == "add_link")
@handle_error
async def add_link(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text(
        "🔗 Введите ссылку (http://... или https://...):",
        reply_markup=cancel_kb
    )
    await state.set_state(LinkForm.waiting_for_link)
    await cb.answer()

@router.message(StateFilter(LinkForm.waiting_for_link))
@handle_error
async def process_link(message: types.Message, state: FSMContext):
    url = message.text.strip()
    if not is_valid_url(url):
        await message.answer("❌ Неверный URL. Попробуйте снова (пример: https://example.com):", reply_markup=cancel_kb)
        return
    loading_msg = await message.answer('⏳ Сокращаю...')
    short_url, error_msg = await shorten_link_vk(url)
    await loading_msg.delete()
    if not short_url:
        await message.answer(f"❌ Ошибка: {error_msg}", reply_markup=cancel_kb)
        return
    await state.update_data(original=url, short=short_url)
    await message.answer("📝 Введите название для ссылки (до 100 символов):", reply_markup=cancel_kb)
    await state.set_state(LinkForm.waiting_for_title)

@router.message(StateFilter(LinkForm.waiting_for_title))
@handle_error
async def process_title(message: types.Message, state: FSMContext):
    title = message.text.strip()[:100]
    if not title:
        await message.answer("❌ Название не может быть пустым:", reply_markup=cancel_kb)
        return
    data = await state.get_data()
    uid = str(message.from_user.id)
    db.execute(
        'INSERT INTO links (user_id, title, short, original, created) VALUES (?, ?, ?, ?, ?)',
        (uid, title, data['short'], data['original'], datetime.now().isoformat())
    )
    await message.answer(
        f"✅ Ссылка сохранена:\n<b>{title}</b>\n{data['short']}",
        parse_mode="HTML",
        reply_markup=get_main_menu()
    )
    await state.clear()

@router.callback_query(lambda c: c.data == "stats")
@handle_error
async def stats_menu(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text(
        "📅 Введите даты (ГГГГ-ММ-ДД ГГГГ-ММ-ДД, например, 2025-06-01 2025-06-30):",
        reply_markup=cancel_kb
    )
    await state.set_state(LinkForm.waiting_for_stats_date)
    await cb.answer()

@router.message(StateFilter(LinkForm.waiting_for_stats_date))
@handle_error
async def process_stats_date(message: types.Message, state: FSMContext):
    dates = message.text.strip().split()
    if len(dates) != 2 or not all(re.match(r"\d{4}-\d{2}-\d{2}", d) for d in dates):
        await message.answer("❌ Неверный формат. Пример: 2025-06-01 2025-06-30", reply_markup=cancel_kb)
        return
    date_from, date_to = dates
    try:
        datetime.strptime(date_from, "%Y-%m-%d")
        datetime.strptime(date_to, "%Y-%m-%d")
    except ValueError:
        await message.answer("❌ Неверные даты. Используйте формат ГГГГ-ММ-ДД", reply_markup=cancel_kb)
        return
    if datetime.strptime(date_to, "%Y-%m-%d") < datetime.strptime(date_from, "%Y-%m-%d"):
        await message.answer("❌ Конечная дата не может быть раньше начальной", reply_markup=cancel_kb)
        return
    uid = str(message.from_user.id)
    links = db.execute('SELECT title, short FROM links WHERE user_id = ?', (uid,))
    if not links:
        await message.answer("📋 У вас нет ссылок", reply_markup=get_main_menu())
        await state.clear()
        return
    loading_msg = await message.answer('⏳ Загружаем...')
    stats = await asyncio.gather(
        *(get_link_stats(l[1].split('/')[-1], date_from, date_to) for l in links)
    )
    text = f"📊 Статистика переходов за {date_from}—{date_to}\n\n"
    total_clicks = 0
    for i, link in enumerate(links):
        clicks = stats[i]['clicks']
        total_clicks += clicks
        text += f"🔗 {link[0]}: {clicks} переходов\n"
    text += f"\n👁 Всего: {total_clicks} переходов"
    await loading_msg.delete()
    await message.answer(text, reply_markup=get_main_menu())
    await state.clear()

async def main():
    logger.info("Запуск бота...")
    try:
        await dp.start_polling()
    except Exception as e:
        logger.error(f"Ошибка бота: {e}")
    finally:
        await dp.storage.close()
        await dp.storage.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
