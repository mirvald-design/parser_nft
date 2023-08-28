from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup

from conf import ADMINS

show_usersov = InlineKeyboardButton(text="Show users", callback_data='show_users')
block_usersa = InlineKeyboardButton(text="Block user", callback_data='block_user')
unblock_usersa = InlineKeyboardButton(text="Unblock user", callback_data='unblock_user')
get_excel = InlineKeyboardButton(text="Get all data in excel", callback_data='get_excel')



start_parse = InlineKeyboardButton(text="🔍Search for sellers", callback_data='start_parse')
stop_parse = InlineKeyboardButton(text="❌ Stop searching", callback_data='stop_parse')




async def approve_keyboard(user_id):
    keyboard = InlineKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    approve = InlineKeyboardButton(text="✅ Approve", callback_data=f'approve{user_id}')
    not_approve = InlineKeyboardButton(text="❌ Not Approve", callback_data=f'not_approve{user_id}')
    keyboard.add(approve).add(not_approve)
    return keyboard


async def admin_keyboard():
    keyboard = InlineKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    keyboard.add(block_usersa).add(show_usersov).add(unblock_usersa, get_excel)
    return keyboard


async def keyboard_parse(chat_id:int):
    keyboard1 = InlineKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    if chat_id in ADMINS:
        keyboard1.add(start_parse).add(block_usersa).add(show_usersov).add(unblock_usersa, get_excel)
    else:
        keyboard1.add(start_parse)
    return keyboard1


async def keyboard_stop(chat_id:int):
    keyboard1 = ReplyKeyboardMarkup(resize_keyboard=True)
    if chat_id in ADMINS:
        keyboard1.add(stop_parse).add(block_usersa).add(show_usersov).add(unblock_usersa, get_excel)
    else:

        keyboard1.add(stop_parse)
    return keyboard1
