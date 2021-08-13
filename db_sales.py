import sqlite3


def most_sales(cursor: sqlite3.Cursor):
    sales_stats = cursor.execute(
        '''WITH week_sales AS (
            SELECT * FROM sales CROSS JOIN week_numbers
            WHERE sales.week_begin <= week_numbers.week_num AND week_numbers.week_num <= sales.week_end
        ), week_usage AS (
            SELECT SUM(week_sales.sale) as total_sale, week_sales.week_num as week_num FROM week_sales
            GROUP BY week_sales.week_num
        )
        SELECT week_numbers.week_num,
            CASE
            WHEN week_usage.total_sale is NULL THEN 0
            ELSE week_usage.total_sale
            END
        FROM week_numbers
        LEFT JOIN week_usage ON week_numbers.week_num = week_usage.week_num
        '''
    ).fetchall()
    print("Week sales:")
    for week_num, total_sale in sales_stats:
        print(f"Week {week_num + 1}: total sale {total_sale}%")
    print()


def main():
    connection = sqlite3.connect("service_usage.db")
    cursor = connection.cursor()

    most_sales(cursor)

    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
