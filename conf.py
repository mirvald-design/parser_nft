from concurrent.futures import ThreadPoolExecutor
import logging
import os
import pandas as pd
from aiogram import Bot, Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage
import ssl
import certifi
import dotenv
from openpyxl.reader.excel import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows

dotenv.load_dotenv('.env', encoding='utf-8')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ssl_context = ssl.create_default_context(cafile=certifi.where())
# Ініціалізація бота
DATABASE_URL = os.environ['DATABASE_URL']
token = os.environ['token']
bot = Bot(token=token)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Налаштування журналування
ADMINS = [6505753971, 5016123201, 5034358166, 579776168]
S_U = -1001989530046


def write_data_to_file(file_name, data_list, file_format):
    # Создаем DataFrame с данными
    df = pd.DataFrame(data_list, columns=['Account Link', 'Profile Name', 'Profile Social Links', 'Activity',
                                          'Registration Date', 'Collection Url', 'Collection Name',
                                          'Collection Social Links', 'Total volume'])

    # Записываем данные в файл с указанным форматом
    if file_format == 'csv':
        df.to_csv(file_name, mode='a', index=False,
                  encoding='utf-8-sig', header=not os.path.exists(file_name))
    elif file_format == 'excel':
        df.to_excel(file_name, index=False,
                    header=not os.path.exists(file_name))


async def write_to_csv(user_id, data_list):
    file_name = os.path.join('EXCEL', f'sellers_for{user_id}.csv')
    write_data_to_file(file_name, data_list, 'csv')


async def write_to_excel(user_id, data_list):
    file_name = os.path.join('EXCEL', f'sellers_for{user_id}.xlsx')
    write_data_to_file(file_name, data_list, 'excel')
