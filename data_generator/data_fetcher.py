import psycopg2

connection_params = {
    "host": "localhost",
    "port": "5678",
    "database": "db",
    "user": "user",
    "password": "password"
}


def fetch_distinct_cnum(connection_params):
    try:
        conn = psycopg2.connect(**connection_params)
        cur = conn.cursor()

        # Execute the SELECT DISTINCT query
        cur.execute("SELECT DISTINCT cnum FROM offer_nomination")

        # Fetch all the distinct cnum values
        distinct_cnums = [row[0] for row in cur.fetchall()]

        # Close the cursor and connection
        cur.close()
        conn.close()

        return distinct_cnums
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None
    finally:
        if conn:
            conn.close()


def get_offer_nominations_by_cnum(connection_params, cnum, select_columns):
    try:
        conn = psycopg2.connect(**connection_params)
        cur = conn.cursor()

        # {', '.join(select_columns)}
        cur.execute(f"""
        SELECT 
            {', '.join(select_columns)}
        FROM offer_nomination
        where cnum = '{cnum}'
        and now() between start_ts and end_ts
        """)

        result = cur.fetchall()

        cur.close()
        conn.close()

        for row in result:
            yield row

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None
    finally:
        if conn:
            conn.close()


select_columns = [
    "id",
    "created_ts",
    "event_id",
    "cnum",
    "camp_code",
    "action_type",
    "start_ts",
    "end_ts",
]

distinct_cnums = fetch_distinct_cnum(connection_params)[:100]

print(distinct_cnums)

for cnum in distinct_cnums:
    ondata = get_offer_nominations_by_cnum(connection_params, cnum, select_columns)
    for r in ondata:
        print(r)


