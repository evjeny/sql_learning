import datetime
import sqlite3


def get_table_info(cursor: sqlite3.Cursor, table_name: str):
    print(f"===={table_name}====")
    columns = [column for (column, *_) in cursor.execute(f"SELECT * FROM {table_name} LIMIT 1").description]
    print("columns:", columns)
    print("n_rows:", cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
    print()


def tables_info(cursor: sqlite3.Cursor):
    get_table_info(cursor, "users")
    get_table_info(cursor, "services")
    get_table_info(cursor, "usages")


def name_lengths(cursor: sqlite3.Cursor):
    for pretty_name, fun_name in [("Average", "AVG"), ("Minimal", "MIN"), ("Maximal", "MAX")]:
        print(f"{pretty_name} name length:",
              cursor.execute(f"SELECT {fun_name}(LENGTH(users.name)) FROM users").fetchone()[0],
              "symbols")
    print()


def beebo_counter(cursor: sqlite3.Cursor):
    print("`beebo` appears in user names:",
          cursor.execute("SELECT COUNT(*) FROM users WHERE users.name LIKE '%beebo%'").fetchone()[0],
          "times")
    print()


def most_recent_uses(cursor: sqlite3.Cursor, n: int = 5):
    last_services = cursor.execute(
        f'''WITH last AS (
                    SELECT * FROM usages ORDER BY timestamp DESC
                )
                SELECT last.timestamp, services.name FROM last JOIN services
                ON last.service_id = services.id LIMIT {n}'''
    ).fetchall()
    print("Last used services:")
    for (timestamp, service_name, ) in last_services:
        print(f"{timestamp}: {service_name}")
    print()


def datetime_to_week_day(d: str) -> int:
    return datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S.%f").weekday()


def week_day_usage(cursor: sqlite3.Cursor):
    day_stats = cursor.execute(
        '''WITH week_days as (
            SELECT week_day(timestamp) as week_day FROM usages
        )
        SELECT
            CASE
                WHEN week_day = 0 THEN "Monday"
                WHEN week_day = 1 THEN "Tuesday"
                WHEN week_day = 2 THEN "Wednesday"
                WHEN week_day = 3 THEN "Thursday"
                WHEN week_day = 4 THEN "Friday"
                WHEN week_day = 5 THEN "Saturday"
                WHEN week_day = 6 THEN "Sunday"
                ELSE "Very cool out-of-week day"
            END,
            COUNT(*)
        FROM week_days GROUP BY week_day'''
    ).fetchall()

    print("Week day usage:")
    for day, count in day_stats:
        print(f"{day}: {count} usages")
    print()


def main():
    connection = sqlite3.connect("service_usage.db")
    connection.create_function("week_day", 1, datetime_to_week_day)
    cursor = connection.cursor()

    tables_info(cursor)
    name_lengths(cursor)
    beebo_counter(cursor)

    most_recent_uses(cursor)
    week_day_usage(cursor)

    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
