import os
import asyncio
import json
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
dp = Dispatcher(storage=MemoryStorage())
router = Router()

# Класс состояний
class LinkForm(StatesGroup):
    waiting_for_link = State()
    waiting_for_title = State()
    waiting_for_stats_date = State()

# Класс для работы с JSON
class JsonStorage:
    def __init__(self, file_name='/data/links.json'):
        self.file_name = file_name
        self.data = self._load_data()

    def _load_data(self):
        try:
            with open(self.file_name, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.info("Файл links.json не найден, создаётся новый")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка чтения JSON: {e}")
            return {}

    def _save_data(self):
        try:
            with open(self.file_name, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка записи JSON: {e}")
            raise

    def get_user_links(self, user_id):
        return self.data.get(str(user_id), [])

    def add_link(self, user_id, link_data):
        user_id = str(user_id)
        if user_id not in self.data:
            self.data[user_id] = []
        self.data[user_id].append(link_data)
        self._save_data()

storage = JsonStorage()

# Проверка валидности URL
def is_valid_url(url):
    return bool(re.match(r'^https?://[^\s/$.?#].[^\s]*$', url, re.IGNORECASE))

# Функция сокращения ссылки через VK API
async def shorten_link_vk(url):
    if not isმო�

System: Я не могу увидеть полный код, так как вы отправили только фрагмент. Пожалуйста, предоставьте полный код, чтобы я мог точно определить проблему и предложить исправление. 

Из предоставленных логов и описания ошибки ясно, что проблема связана с двойным подключением роутера, и я уже исправил это, убрав `dp.include_router(router)` из глобальной области. Однако, чтобы убедиться, что больше нет скрытых ошибок, полный код был бы полезен.

Также вы упомянули, что хотите, чтобы данные в `links.json` добавлялись динамически пользователями, а не были жёстко закодированы. Код, который я предоставил выше, уже настроен для этого: он создаёт пустой `links.json` (если его нет) и добавляет ссылки с `user_id` текущего пользователя при использовании команды сокращения ссылок. Если у вас есть конкретный код, который вы используете, поделитесь им, чтобы я мог проверить, нет ли других проблем.

### **Исправления и подтверждения**

1. **Исправленная ошибка роутера**:
   - Удалено `dp.include_router(router)` из глобальной области.
   - Оставлено только в `main()`:
     ```python
     async def main():
         logger.info("Запуск бота...")
         try:
             await bot.delete_webhook(drop_pending_updates=True)
             dp.include_router(router)  # Подключение роутера только здесь
             await dp.start_polling(bot)
         except Exception as e:
             logger.error(f"Ошибка бота: {e}")
         finally:
             await bot.session.close()
