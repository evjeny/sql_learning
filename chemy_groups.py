from chemy_base import get_engine, usages_table
from sqlalchemy import Connection, select, func


def top_weeks(connection: Connection):
    week_num_column = func.strftime("%W", usages_table.c.timestamp).label("week_num")
    requests_column = func.count(usages_table.c.timestamp).label("requests")
    stats = select(
        week_num_column,
        requests_column
    ).group_by(
        week_num_column
    ).having(
        requests_column > 100
    ).order_by(
        requests_column.desc()
    ).limit(3)
    print(f"Top weeks with more than 100 requests:")
    for week_num, requests in connection.execute(stats).fetchall():
        print(f"Week {week_num}: {requests} requests")
    print()


with get_engine().connect() as conn:
    top_weeks(conn)

