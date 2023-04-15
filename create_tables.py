from typing import List
import itertools
import random
import datetime
import sqlite3


def add_users(connection: sqlite3.Connection) -> List[int]:
    """Create table `users` with fields: `id`, `name`, `age`,
        and fill it with random values
    """
    cursor = connection.cursor()

    # clear table if exists
    cursor.execute('''DROP TABLE IF EXISTS users''')
    # create new table
    cursor.execute(
        '''CREATE TABLE users
        (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)'''
    )

    # random name generator
    for parts in itertools.product(
            ["beebo", "babuleh", "muffin"], ["1", "2", "3", "4"],
            repeat=2
    ):
        name = "".join(parts)
        age = random.randint(14, 99)

        # add new row (`id` is auto incremented, hence do not need to forward it)
        cursor.execute(
            '''INSERT INTO users (name, age) VALUES (?, ?)''',
            (name, age)
        )

    # save changes in database
    connection.commit()

    print("users:")
    # select 5 first users from `users` table
    for user in cursor.execute("SELECT * from users LIMIT 5").fetchall():
        print(user)
    print()

    # select row `id` from `users` table
    user_ids = [user_id for (user_id,) in cursor.execute("SELECT id FROM users").fetchall()]
    cursor.close()

    return user_ids


def add_services(connection: sqlite3.Connection) -> List[int]:
    cursor = connection.cursor()

    cursor.execute('''DROP TABLE IF EXISTS services''')
    cursor.execute(
        '''CREATE TABLE services
        (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, description TEXT)'''
    )

    names = ["water", "fire", "earth", "air", "oil"]
    descriptions = [
        "gives user some water", "fires user up", "feeds a user",
        "lets user breathe", "makes user engaged"
    ]

    for name, description in zip(names, descriptions):
        cursor.execute(
            '''INSERT INTO services (name, description) VALUES (?, ?)''',
            (name, description)
        )
    connection.commit()

    print("services:")
    for user in cursor.execute("SELECT * from services LIMIT 3").fetchall():
        print(user)
    print()

    service_ids = [service_id for (service_id,) in cursor.execute("SELECT id from services").fetchall()]
    cursor.close()

    return service_ids


def add_usages(connection: sqlite3.Connection,
               user_ids: List[int], service_ids: List[int],
               min_usages: int = 0, max_usages: int = 22,
               min_period_days: int = 1, max_period_days: int = 199):
    cursor = connection.cursor()

    cursor.execute('''DROP TABLE IF EXISTS usages''')
    cursor.execute(
        '''CREATE TABLE usages
        (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, service_id INTEGER, timestamp TIMESTAMP)'''
    )

    now = datetime.datetime(2021, 7, 1)
    for user_id in user_ids:
        n_usages = random.randint(min_usages, max_usages)
        period = random.randint(min_period_days, max_period_days)

        delta = datetime.timedelta(days=period)
        begin_ts = random.uniform(
            (now - delta).timestamp(),
            (now + delta).timestamp()
        )
        begin_date = datetime.datetime.fromtimestamp(begin_ts)

        for date, service_id in zip(
                [begin_date + datetime.timedelta(days=i) for i in range(n_usages)],
                [random.choice(service_ids) for _ in range(n_usages)]
        ):
            cursor.execute(
                '''INSERT INTO usages (user_id, service_id, timestamp) VALUES (?, ?, ?)''',
                (user_id, service_id, date)
            )
    connection.commit()

    print("usages:")
    for usage in cursor.execute("SELECT * from usages LIMIT 10").fetchall():
        print(usage)
    print()

    cursor.close()


def add_week_numbers(connection: sqlite3.Connection):
    cursor = connection.cursor()

    cursor.execute('''DROP TABLE IF EXISTS week_numbers''')
    cursor.execute('''CREATE TABLE week_numbers (week_num INTEGER PRIMARY KEY AUTOINCREMENT)''')
    cursor.executemany('''INSERT INTO week_numbers (week_num) VALUES (?)''',
                       list(zip(range(54))))
    connection.commit()

    print("week_numbers:")
    print(cursor.execute('''SELECT * from week_numbers LIMIT 3''').fetchall())
    print()

    cursor.close()


def add_sales(connection: sqlite3.Connection):
    cursor = connection.cursor()

    cursor.execute('''DROP TABLE IF EXISTS sales''')
    cursor.execute('''CREATE TABLE sales (id INTEGER PRIMARY KEY AUTOINCREMENT,
            week_begin INTEGER, week_end INTEGER, sale INTEGER)''')

    for _ in range(20):
        week_begin = random.randint(0, 53)
        week_end = week_begin + random.randint(1, 3)
        sale = random.randint(1, 20)
        cursor.execute(
            '''INSERT INTO sales (week_begin, week_end, sale) VALUES (?, ?, ?)''',
            (week_begin, week_end, sale)
        )
    connection.commit()

    print("sales:")
    for week_begin, week_end, sale in cursor.execute(
            "SELECT week_begin, week_end, sale FROM sales LIMIT 3"
    ).fetchall():
        print(f"{week_begin}-{week_end}: sale {sale}%")
    print()


def filter_usages(connection: sqlite3.Connection):
    cursor = connection.cursor()

    # delete rows that match condition
    cursor.execute(
        '''DELETE FROM usages WHERE usages.timestamp > "2021-09-01 00:00:01"'''
    )

    connection.commit()
    cursor.close()


def update_usages(connection: sqlite3.Connection):
    cursor = connection.cursor()

    # replace rows that match conditions
    cursor.execute(
        '''UPDATE usages set service_id = 1 WHERE
        usages.service_id = 2 AND usages.timestamp < "2021-08-01 00:00:01"'''
    )

    connection.commit()
    cursor.close()


def main():
    connection = sqlite3.connect("service_usage.db")
    cursor = connection.cursor()

    user_ids = add_users(connection)
    service_ids = add_services(connection)
    add_usages(connection, user_ids, service_ids)
    add_week_numbers(connection)
    add_sales(connection)

    # count number of rows in each table
    print("n_users:", cursor.execute("SELECT COUNT(*) FROM users").fetchone()[0])
    print("n_services:", cursor.execute("SELECT COUNT(*) FROM services").fetchone()[0])
    print("n_usages:", cursor.execute("SELECT COUNT(*) FROM usages").fetchone()[0])
    print("max usage date:", cursor.execute("SELECT MAX(usages.timestamp) FROM usages").fetchone()[0])
    print()

    filter_usages(connection)
    print("n_usages after filtering:", cursor.execute("SELECT COUNT(*) FROM usages").fetchone()[0])
    print("max usage date:", cursor.execute("SELECT MAX(usages.timestamp) FROM usages").fetchone()[0])
    print()

    service_1_usages_query = '''SELECT COUNT(*) FROM usages WHERE
          usages.timestamp < "2021-08-01 00:00:01"
          AND usages.service_id = 1'''
    print("count of service `1` before update:", cursor.execute(service_1_usages_query).fetchone()[0])
    update_usages(connection)
    print("count of service `1` after update:", cursor.execute(service_1_usages_query).fetchone()[0])

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
