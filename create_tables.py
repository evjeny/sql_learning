from typing import List
import itertools
import random
import datetime
import sqlite3


def add_users(connection: sqlite3.Connection) -> List[int]:
    cursor = connection.cursor()

    cursor.execute('''DROP TABLE IF EXISTS users''')
    cursor.execute(
        '''CREATE TABLE users
        (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)'''
    )

    for parts in itertools.product(
            ["beebo", "babuleh", "muffin"], ["1", "2", "3", "4"],
            repeat=2
    ):
        name = "".join(parts)
        age = random.randint(14, 99)
        cursor.execute(
            '''INSERT INTO users (name, age) VALUES (?, ?)''',
            (name, age)
        )
    connection.commit()

    print("users:")
    for user in cursor.execute("SELECT * from users LIMIT 5").fetchall():
        print(user)
    print()

    return [user_id for (user_id, ) in cursor.execute("SELECT id from users").fetchall()]


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

    return [service_id for (service_id,) in cursor.execute("SELECT id from services").fetchall()]


def add_usage(connection: sqlite3.Connection,
              user_ids: List[int], service_ids: List[int],
              min_usages: int = 0, max_usages: int = 22,
              min_period_days: int = 1, max_period_days: int = 199):
    cursor = connection.cursor()

    cursor.execute('''DROP TABLE IF EXISTS usages''')
    cursor.execute(
        '''CREATE TABLE usages
        (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, service_id INTEGER, timestamp TIMESTAMP)'''
    )

    now = datetime.datetime.now()
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


def main():
    connection = sqlite3.connect("service_usage.db")

    user_ids = add_users(connection)
    service_ids = add_services(connection)
    add_usage(connection, user_ids, service_ids)

    cursor = connection.cursor()
    print("n_users:", cursor.execute("SELECT COUNT(*) FROM users").fetchone()[0])
    print("n_services:", cursor.execute("SELECT COUNT(*) FROM services").fetchone()[0])
    print("n_usages:", cursor.execute("SELECT COUNT(*) FROM usages").fetchone()[0])

    connection.close()


if __name__ == "__main__":
    main()
