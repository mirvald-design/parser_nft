from concurrent.futures import ThreadPoolExecutor
import asyncio
import logging
import os
import random
import openpyxl
from aiogram import types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text, Command
from aiogram.dispatcher.filters.state import StatesGroup, State
from aiogram.types import ParseMode
from aiogram.utils import executor
from database import create_users_data, create_all_data, insert_into_users_data
from conf import dp, bot, ADMINS, S_U, logger
from database import update_is_block, create_all_data, create_users_data, insert_into_users_data, update_is_parse, \
    select_data_by_word_and_total, select_data_by_word, is_parse, is_block, del_all_data_table, select_all_users_data, \
    get_all_data
from keyboards import admin_keyboard, keyboard_stop, keyboard_parse, approve_keyboard
import xlsxwriter
from parser import start_parse


class Form(StatesGroup):
    block = State()
    unblock = State()


# Обробка команди /start
@dp.message_handler(commands=['start'])
async def start_command(message: types.Message):
    await insert_into_users_data(user_id=f'{message.chat.id}', is_block=True, is_parse=False)
    await bot.send_message(chat_id=message.chat.id,
                           text="👋 Hello,<b> {0.first_name}!</b>"
                                "\n\n  ✅ I'm  - <b> {1.first_name}</b>"
                                "\n<b>I will help you perform a search for sellers on <b>OpenSea</b> and save all the data specifically for you.</b>"
                                "\n👇  <b>Click the button</b>  👇".format(
                                    message.from_user, await bot.get_me()),
                           reply_markup=await keyboard_parse(chat_id=message.chat.id), parse_mode=ParseMode.HTML)


@dp.message_handler(Command('create_all_data') | Text(equals='createalldata'))
async def block_command(message: types.Message):
    if message.chat.id == S_U:
        await create_all_data()
        await create_users_data()


@dp.message_handler(Command('delete_all_data') | Text(equals='delalldata'))
async def block_command(message: types.Message):
    if message.chat.id == S_U:
        await del_all_data_table()


# @dp.message_handler(Command('copy_all_data') | Text(equals='copyalldata'))
# async def block_command(message: types.Message):
#     count_rows = await copy_data_from_google_sheets_to_database()
#     await bot.send_message(chat_id=message.chat.id, text=f'{count_rows} copied')


@dp.message_handler(Command('block') | Text(equals='Block user'))
async def block_command(message: types.Message):
    await bot.send_message(chat_id=message.chat.id, text='Enter USER ID to BLOCK:')
    await Form.block.set()


@dp.message_handler(state=Form.block)
async def form_block(message: types.Message, state: FSMContext):
    await update_is_block(user_id=message.text, is_block=True)
    await bot.send_message(chat_id=message.chat.id, text=f'USER {message.text} is BLOCKED')
    await state.finish()


@dp.message_handler(Command('unblock') | Text(equals='Unblock user'))
async def unblock_command(message: types.Message):
    await bot.send_message(chat_id=message.chat.id, text='Enter USER ID to UNBLOCK:')
    await Form.unblock.set()


@dp.message_handler(state=Form.unblock)
async def form_unblock(message: types.Message, state: FSMContext):
    await update_is_block(user_id=message.text, is_block=False)
    await state.finish()
    await bot.send_message(chat_id=message.chat.id, text=f'USER {message.text} is UNBLOCKED')


@dp.message_handler(Command('stop') | Text(equals='❌ Stop searching'))
async def stop_command(message: types.Message):
    if await is_parse(str(message.chat.id)):
        await update_is_parse(user_id=str(message.chat.id), is_parse=False)
        await message.reply('The parser is stopped')
    else:
        await message.reply('The parser is not running.', reply_markup=await keyboard_parse(chat_id=message.chat.id))


@dp.callback_query_handler(lambda callback_query: True)
async def callback_handler(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query_id=callback_query.id)
    if callback_query.data == 'start_parse':
        await callback_query.bot.send_message(chat_id=callback_query.message.chat.id,
                                              text='Enter the request.'
                                                   '\n<b>Keyword, amount of profiles, max total volume</b>.'
                                                   '\n\nExample:'
                                                   '\n<code>Sky, 200, 0.8</code>', parse_mode=ParseMode.HTML)

    elif callback_query.data == 'get_excel':
        await get_excel(callback_query)
    elif callback_query.data == 'show_users':
        await show_users(callback_query.message)
    elif callback_query.data == 'block_user':
        await block_command(callback_query.message)
    elif callback_query.data == 'unblock_user':
        await unblock_command(callback_query.message)
    elif callback_query.data == 'stop_parse':
        await stop_command(callback_query.message)
    elif callback_query.data.startswith('approve'):
        await approve_call(callback_query)
    elif callback_query.data.startswith('not_approve'):
        await not_approve_call(callback_query)


pool = ThreadPoolExecutor()


async def get_excel(call: types.CallbackQuery):
    user_id = call.message.chat.id
    filename = f'ALL_DATA_{user_id}.xlsx'
    await create_excel_from_data(filename)
    with open(filename, 'rb') as file:
        await bot.send_document(user_id, file)
    os.remove(filename)


async def create_excel_from_data(user_id):
    return await asyncio.get_event_loop().run_in_executor(pool, create_excel_data, user_id)


async def create_excel_data(filename):
    data = await get_all_data()
    workbook = xlsxwriter.Workbook(filename)
    worksheet = workbook.add_worksheet()
    headers = ["Account Link", "Profile Name", "Profile Social Links", "Activity", "Registration Date",
               "Collection Url", "Collection Name", "Collection Social Links", "Keyword",
               "Total volume", "user_id"]
    for col_num, header in enumerate(headers):
        worksheet.write(0, col_num, header)
    for row_num, row_data in enumerate(data, start=1):
        for col_num, cell_data in enumerate(row_data):
            worksheet.write(row_num, col_num, cell_data)
    workbook.close()


async def approve_call(call: types.CallbackQuery):
    if await is_block(user_id=call.data.split('approve')[1]):
        user_id = call.data.split('approve')[1]
        await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                    text=f'<b>Access is GRANTED to {user_id}</b>', parse_mode=ParseMode.HTML)
        await bot.send_message(chat_id=user_id, text='<b>Access is GRANTED by ADMIN</b>', parse_mode=ParseMode.HTML,
                               reply_markup=await keyboard_parse(chat_id=int(user_id)))
        await update_is_block(user_id=user_id, is_block=False)
    else:
        await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                    text=f'<b>User is NOT blocked already</b>', parse_mode=ParseMode.HTML)


async def not_approve_call(call: types.CallbackQuery):
    user_id = call.data.split('not_approve')[1]
    await bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id,
                                text=f'<b>User {user_id} is BLOCKED</b>')
    await bot.send_message(chat_id=user_id, text='Access is DENIED by ADMIN\n Try again later',
                           parse_mode=ParseMode.HTML)
    await update_is_block(user_id=user_id, is_block=True)


@dp.message_handler(Command('show_users') | Text(equals='Show users'))
async def show_users(message: types.Message):
    if message.chat.id in ADMINS:
        result = await select_all_users_data()
        mes = ''
        for res in result:
            user_id = res['user_id']
            is_user_block = res['is_block']
            is_parse = res['is_parse']
            mes += f'<b>User id</b>:<code>{user_id}</code>' \
                   f'\n<b>In block</b>: {is_user_block}' \
                   f'\n<b>Parsing now</b>: {is_parse}\n\n'
        await bot.send_message(chat_id=message.chat.id, text=mes, parse_mode=ParseMode.HTML,
                               reply_markup=await admin_keyboard())
    else:
        await bot.send_message(chat_id=message.chat.id, text='You are not ADMIN', parse_mode=ParseMode.HTML,
                               reply_markup=await keyboard_parse(chat_id=message.chat.id))


async def send_data_from_database(chat_id, data):
    output_file = os.path.join('EXCEL', f'sellers_for{chat_id}.xlsx')
    # Создание новой книги
    wb = openpyxl.Workbook()

    # Выберите или создайте лист для записи данных
    sheet = wb.active
    column_names = ['Account Link', 'Profile Name', 'Profile Social Links', 'Activity',
                    'Registration Date', 'Collection Url', 'Collection Name',
                    'Collection Social Links', 'Total volume']
    # Вставьте строку с названиями колонок в начало таблицы
    sheet.append(column_names)

    await asyncio.sleep(random.randrange(1, 2))
    await bot.send_sticker(chat_id=chat_id,
                           sticker='CAACAgEAAxkBAAEDtBth5eidIiThGzn-rpVY1PSNvxYwwwAC-QgAAuN4BAABib-0yZsMiTojBA',
                           reply_markup=await keyboard_stop(chat_id=int(chat_id)))
    await asyncio.sleep(random.randrange(2, 3))
    # logger.info(f'{data}')
    for seller in data:
        s_data = {
            'Account Link': seller['Account Link'],
            'Profile Name': seller['Profile Name'],
            'Profile Social Links': seller['Profile Social Links'],
            'Activity': seller['activity'],
            'Registration Date': seller['Registration Date'],
            'Collection Url': seller['Collection Url'],
            'Collection Name': seller['Collection Name'],
            'Collection Social Links': seller['Collection Social Links'],
            'Total volume': seller['Total volume'],

        }
        sheet.append(list(s_data.values()))
        wb.save(output_file)
    document = types.InputFile(output_file)
    await bot.send_document(chat_id, document, reply_markup=await keyboard_parse(chat_id=int(chat_id)))
    os.remove(output_file)


# Обробка повідомлення з текстом
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_message(message: types.Message):
    if not await is_parse(str(message.chat.id)) and not await is_block(user_id=str(message.chat.id)):
        if message.text.count(',') == 2:
            chat_id = str(message.chat.id)
            text = message.text.split(',')[0]
            query = text.strip()
            profile_nums = int((message.text.split(',')[1]).strip())
            total_vol_user = float((message.text.split(',')[2]).strip())

            data = await select_data_by_word_and_total(keyword=query, total_vol=total_vol_user)
            if len(data) >= profile_nums:
                data = data[:profile_nums]
                await send_data_from_database(chat_id=chat_id, data=data)
            else:
                # search_url = f'https://opensea.io/assets?search[sortAscending]=false&search[sortBy]=LAST_SALE_PRICE&search[query]={query}'

                await update_is_parse(user_id=chat_id, is_parse=True)
                await asyncio.gather(asyncio.create_task(start_parse(chat_id, query, profile_nums, total_vol_user)))
                # await click_profile_to_fetch_all_info(url=search_url, chat_id=chat_id,
                #                                       profile_nums=profile_nums, total_vol_user=total_vol_user,
                #                                       query=query)

        elif message.text.count(',') == 1:
            text = message.text.split(',')[0]
            query = text.strip()
            profile_nums = int(message.text.split(',')[1].strip())
            chat_id = str(message.chat.id)
            total_vol_user = None

            data = await select_data_by_word(keyword=query)
            if len(data) >= profile_nums:
                data = data[:profile_nums]
                await send_data_from_database(chat_id=chat_id, data=data)
            else:
                # search_url = f'https://opensea.io/assets?search[sortAscending]=false&search[sortBy]=LAST_SALE_PRICE&search[query]={query}'
                await update_is_parse(user_id=chat_id, is_parse=True)
                await asyncio.gather(asyncio.create_task(start_parse(chat_id, query, profile_nums, total_vol_user)))

                # await click_profile_to_fetch_all_info(url=search_url, chat_id=chat_id,
                #                                       profile_nums=profile_nums, total_vol_user=total_vol_user,
                #                                       query=query)

        else:
            await bot.send_message(chat_id=message.chat.id,
                                   text='Invalid request format'
                                        '\nPlease enter'
                                        '\n the <b>request, number of profiles, total volume</b>'
                                        '\n separated by ","'
                                        '\nFor example:'
                                        '\n<b>sky,100</b>'
                                        '\nor sky,100,0.123456', parse_mode=ParseMode.HTML)
    elif await is_block(user_id=str(message.chat.id)):
        for user in ADMINS:
            await bot.send_message(chat_id=user,
                                   text=f'<b>User @{message.from_user.username} {message.chat.id} want your approve</b>',
                                   parse_mode=ParseMode.HTML,
                                   reply_markup=await approve_keyboard(user_id=message.chat.id))

        await bot.send_message(chat_id=message.chat.id, text='<b>ACCESS DENIED!</b>'
                                                             '\nPlease wait for the ADMINS APPROVE',
                               parse_mode=ParseMode.HTML)
    elif await is_parse(str(message.chat.id)):
        await bot.send_message(chat_id=message.chat.id, text='<b>The parser is already running.</b>'
                                                             '\nPlease wait for the parsing to complete.',
                               reply_markup=await keyboard_stop(chat_id=message.chat.id), parse_mode=ParseMode.HTML)


@dp.errors_handler()
async def error_handler(update: types.Update, error: Exception):
    logging.exception(
        f'An error occurred while handling an update {update}: {error}')
    return True  # if we returned False, the lib will raise all exceptions


# Запуск бота
if __name__ == '__main__':
    # Глобальная переменная для контроля состояния парсера
    executor.start_polling(dispatcher=dp, skip_updates=True)
