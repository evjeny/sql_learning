from chemy_base import get_engine, sales_table, week_numbers_table 
from sqlalchemy import Connection, func, select, and_, case


def most_sales(connection: Connection):
    week_sales = select(
        sales_table, week_numbers_table
    ).where(
        and_(
            sales_table.c.week_begin <= week_numbers_table.c.week_num,
            week_numbers_table.c.week_num <= sales_table.c.week_end
        )
    ).cte("week_sales")

    week_usage = select(
        func.sum(week_sales.c.sale).label("total_sale"),
        week_sales.c.week_num.label("week_num")
    ).group_by(
        week_sales.c.week_num
    ).cte("week_usage")

    stats = select(
        week_numbers_table.c.week_num,
        case(
            (week_usage.c.total_sale == None, 0),
            else_=week_usage.c.total_sale
        )
    ).select_from(
        week_numbers_table.outerjoin(
            week_usage,
            week_numbers_table.c.week_num == week_usage.c.week_num
        )
    )
    print("Week sales:")
    for week_num, total_sale in connection.execute(stats).fetchall():
        print(f"Week {week_num + 1}: total sale {total_sale}%")
    print()


with get_engine().connect() as conn:
    most_sales(conn)
