from pyflink.common import Row
from pyflink.table.udf import udtf
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
import psycopg2

connection_params = {
    "host": "kotel-pg",
    "port": "5432",
    "database": "db",
    "user": "user",
    "password": "password"
}


def fetch_distinct_cnum(connection_params):
    conn = psycopg2.connect(**connection_params)
    cur = conn.cursor()
    try:
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
    conn = psycopg2.connect(**connection_params)
    cur = conn.cursor()
    try:

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

        return result

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None
    finally:
        if conn:
            conn.close()


select_columns = [
    "id",
    #"created_ts",
    "event_id",
    "cnum",
    "camp_code",
    "action_type",
    #"start_ts",
    #"end_ts",
]

# distinct_cnums = fetch_distinct_cnum(connection_params)[:100]
# table_cnum_data = []
# x = 0
# for cnum in distinct_cnums:
#     x += 1
#     table_cnum_data.append(
#         (x, cnum)
#     )


env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_cnum_data = [
    (1, 'Aa0BUS9'),
    (2, 'aa0dub9'),
    (3, 'aa0dXe3'),
]

table = table_env.from_elements(table_cnum_data, ['id', 'cnum'])


@udtf(result_types=['STRING','STRING','STRING','STRING','STRING'])
def get_offers(x: Row) -> Row:
    print(f"{x.cnum=}")
    yield x.cnum, 'col2', 'col3', 'col4', 'col5'
    # yield get_offer_nominations_by_cnum(connection_params, x.cnum, select_columns)


result_table = table.flat_map(get_offers).alias(
    "id",
    "event_id",
    "cnum",
    "camp_code",
    "action_type",
)
result_table.execute().print()
