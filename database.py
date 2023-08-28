import asyncpg
import gspread
from google.oauth2 import service_account

from conf import DATABASE_URL, logger


class DatabaseManager:
    async def __aenter__(self):
        self.con = await asyncpg.connect(DATABASE_URL, ssl='require')
        return self.con

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.con.close()


async def create_users_data():
    async with DatabaseManager() as con:
        query = """
            CREATE TABLE IF NOT EXISTS users_data (
                user_id varchar PRIMARY KEY,
                is_block bool,
                is_parse bool
            )
        """
        await con.execute(query)


async def create_all_data():
    async with DatabaseManager() as con:
        query = """
            CREATE TABLE IF NOT EXISTS all_data (
                "Account Link" VARCHAR  PRIMARY KEY,
                "Profile Name" VARCHAR ,
                "Profile Social Links" VARCHAR ,
                Activity VARCHAR ,
                "Registration Date" VARCHAR ,
                "Collection Url" VARCHAR ,
                "Collection Name" VARCHAR ,
                "Collection Social Links" VARCHAR ,
                Keyword VARCHAR ,
                "Total volume" VARCHAR,
                user_id VARCHAR 
            )
        """
        await con.execute(query)


async def get_all_data():
    con = await asyncpg.connect(DATABASE_URL, ssl='require')
    query = "SELECT * FROM all_data"
    rows = await con.fetch(query)
    await con.close()
    return rows


async def insert_into_all_data(data):
    con = await asyncpg.connect(DATABASE_URL, ssl='require')
    query = """
        INSERT INTO all_data("Account Link", "Profile Name", "Profile Social Links", Activity,
                        "Registration Date", "Collection Url", "Collection Name", "Collection Social Links", Keyword, "Total volume", user_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT ("Account Link") DO NOTHING
    """
    result = await con.execute(query, data['Account Link'], data['Profile Name'], data['Profile Social Links'],
                               data['Activity'],
                               data['Registration Date'], data['Collection Url'], data['Collection Name'],
                               data['Collection Social Links'], data['Keyword'], data['Total volume'], data['user_id'])
    await con.close()
    return result != 'INSERT 0 0'


async def select_data_by_word_and_total(keyword: str, total_vol: float):
    async with DatabaseManager() as con:
        query = """
            SELECT *
            FROM all_data
            WHERE "keyword" = $1 AND CAST("Total volume" AS FLOAT) <= $2
        """
        rows = await con.fetch(query, keyword, total_vol)
        result = []
        for row in rows:
            result.append(dict(row))
        return result


async def select_data_by_word(keyword):
    con = await asyncpg.connect(DATABASE_URL, ssl='require')
    query = """
    SELECT * FROM all_data WHERE Keyword = $1
    """
    rows = await con.fetch(query, keyword)
    await con.close()
    result = []
    for row in rows:
        result.append(dict(row))
    return result


async def select_all_users_data():
    con = await asyncpg.connect(DATABASE_URL, ssl='require')  # , ssl='require'
    rows = await con.fetch(
        "SELECT * FROM users_data")
    await con.close()

    result = []
    for row in rows:
        result.append(dict(row))

    return result


async def insert_into_users_data(user_id, is_block: bool, is_parse: bool):
    con = await asyncpg.connect(DATABASE_URL, ssl='require')
    query = """
        INSERT INTO users_data(user_id, is_block, is_parse)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_id) DO NOTHING
    """
    await con.execute(query, user_id, is_block, is_parse)
    await con.close()


async def update_is_block(user_id: str, is_block: bool):
    con = await asyncpg.connect(DATABASE_URL, ssl='require')
    query = """
        UPDATE users_data SET is_block = $1 WHERE user_id = $2
    """
    await con.execute(query, is_block, user_id)
    await con.close()


async def update_is_parse(user_id: str, is_parse: bool):
    con = await asyncpg.connect(DATABASE_URL, ssl='require')
    query = """
        UPDATE users_data SET is_parse = $1 WHERE user_id = $2
    """
    await con.execute(query, is_parse, user_id)
    await con.close()


async def del_all_data_table():
    con = await asyncpg.connect(DATABASE_URL, ssl='require')  # , ssl='require'
    q = await con.execute(
        f"DROP TABLE IF EXISTS all_data")
    await con.close()


async def del_users_data_table():
    con = await asyncpg.connect(DATABASE_URL, ssl='require')  # , ssl='require'
    q = await con.execute(
        f"DROP TABLE IF EXISTS users_data")
    await con.close()


async def is_block(user_id):
    con = await asyncpg.connect(DATABASE_URL, ssl='require')
    query = """
        SELECT is_block FROM users_data WHERE user_id = $1
    """
    row = await con.fetchval(query, user_id)
    await con.close()

    return row


async def is_parse(user_id: str):
    con = await asyncpg.connect(DATABASE_URL, ssl='require')
    query = """
        SELECT is_parse FROM users_data WHERE user_id = $1
    """
    row = await con.fetchval(query, user_id)
    await con.close()

    return row


async def select_all_user_data(user_id: str):
    con = await asyncpg.connect(DATABASE_URL, ssl='require')
    query = """
        SELECT * FROM users_data WHERE user_id = $1
    """
    row = await con.fetchrow(query, user_id)
    await con.close()

    return dict(row)


async def copy_data_from_google_sheets_to_database():
    # Установите требуемые области доступа и учетные данные
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = service_account.Credentials.from_service_account_file(
        'credentials.json', scopes=scopes)
    # Авторизация и получение экземпляра клиента для работы с Google Sheets
    client = gspread.authorize(credentials)
    # Откройте существующую таблицу
    spreadsheet = client.open_by_key(
        '1HiW2rMa__OflQw4AU-4XVEpwRcC20OZyeSP3vdtm41I')
    # Выберите или создайте лист для чтения данных
    worksheet = spreadsheet.get_worksheet(0)  # Получите первый лист таблицы
    # Прочитайте все значения из таблицы
    all_values = worksheet.get_all_values()
    # Установите соответствие между столбцами таблицы и данными из Google Sheets
    columns_mapping = {
        0: "Account Link",
        1: "Profile Name",
        2: "Social Links",
        3: "Activity",
        4: "Registration Date",
        5: "Collection Url",
        6: "Keyword",
        7: "Total volume"
    }
    # Установите соединение с базой данных
    con = await asyncpg.connect(DATABASE_URL, ssl='require')
    c = 0
    # Вставьте данные в базу данных
    for row in all_values:
        row = [value if value != "" else ' ' for value in row]
        await con.execute(
            """
                INSERT INTO all_data ("Account Link", "Profile Name", "Social Links", Activity,
                                      "Registration Date", "Collection Url", Keyword, "Total volume")
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT ("Account Link") DO NOTHING
                """,
            *row
        )
        c += 1


async def main():
    await create_users_data()
    await create_all_data()

if __name__ == "__main__":
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
