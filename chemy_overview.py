from chemy_base import (
    get_engine,
    users_table,
    services_table,
    usages_table,
    week_numbers_table,
    sales_table
)
from sqlalchemy import Connection, Table, select, func, case, literal_column


def get_table_info(connection: Connection, table: Table):
    print(f"===={table.name}====")

    columns = [key for key in connection.execute(select(table).limit(1)).keys()]
    print("columns:", columns)

    n_rows = connection.execute(select(func.count()).select_from(table)).fetchone()[0]
    print("n_rows:", n_rows)

    print()


def tables_info(connection: Connection):
    get_table_info(connection, users_table)
    get_table_info(connection, services_table)
    get_table_info(connection, usages_table)


def name_lengths(connection: Connection):
    for pretty_name, stat_function in [
        ("Average", func.avg),
        ("Minimal", func.min),
        ("Maximal", func.max)
    ]:
        metric = connection.execute(
            select(stat_function(func.char_length(users_table.c.name)))
        ).fetchone()[0]
        print(f"{pretty_name} name length:", metric, "symbols")
    print()


def beebo_counter(connection: Connection):
    beebo_count = connection.execute(
        select(func.count()).select_from(
            users_table
        ).where(
            users_table.c.name.like("%beebo%")
        )
    ).fetchone()[0]
    print("`beebo` appears in user names:", beebo_count, "times")
    print()


def most_recent_uses(connection: Connection, n: int = 5):
    last = select(usages_table).order_by(usages_table.c.timestamp.desc()).cte("last")
    last_services = select(
        last.c.timestamp, services_table.c.name
    ).select_from(
        last.join(
            services_table,
            last.c.service_id == services_table.c.id
        )
    ).limit(n)
    print("Last used services:")

    for (timestamp, service_name, ) in connection.execute(last_services).fetchall():
        print(f"{timestamp}: {service_name}")
    print()


def week_day_usage(connection: Connection):
    week_days = select(
        (
            (
                func.strftime(
                    "%w",
                    usages_table.c.timestamp
                ) + 7 - 1
            ) % 7
        ).label("week_day")
    ).cte("week_days")
    
    stats = select(
        case(
            (week_days.c.week_day == 0, literal_column("'Monday'")),
            (week_days.c.week_day == 1, literal_column("'Tuesday'")),
            (week_days.c.week_day == 2, literal_column("'Wednesday'")),
            (week_days.c.week_day == 3, literal_column("'Thursday'")),
            (week_days.c.week_day == 4, literal_column("'Friday'")),
            (week_days.c.week_day == 5, literal_column("'Saturday'")),
            (week_days.c.week_day == 6, literal_column("'Sunday'")),
            else_=literal_column("'Very cool out-of-week day'")
        ),
        func.count(week_days.c.week_day)
    ).group_by(week_days.c.week_day)

    print("Week day usage:")
    for day, count in connection.execute(stats).fetchall():
        print(f"{day}: {count} usages")
    print()


engine = get_engine()
with engine.connect() as conn:
    tables_info(conn)
    name_lengths(conn)
    beebo_counter(conn)

    most_recent_uses(conn)
    week_day_usage(conn)
