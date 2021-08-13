import sqlite3


def top_weeks(cursor: sqlite3.Cursor):
    # count usages per weeks and select top 3 weeks (by usage) with more than 100 requests
    week_stats = cursor.execute(
        f'''SELECT strftime('%W', timestamp) AS week_num, COUNT(*) AS requests FROM usages
        GROUP BY week_num HAVING requests > 100 ORDER BY requests DESC LIMIT 3'''
    ).fetchall()

    print(f"Top weeks with more than 100 requests:")
    for week_num, requests in week_stats:
        print(f"Week {week_num}: {requests} requests")
    print()


def main():
    connection = sqlite3.connect("service_usage.db")
    cursor = connection.cursor()

    top_weeks(cursor)

    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
