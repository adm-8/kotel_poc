import random

from pyflink.common import Row
from pyflink.table.udf import udtf
from pyflink.table import EnvironmentSettings, TableEnvironment
import psycopg2
import psycopg2.extras
from datetime import datetime
import uuid

connection_params = {
    "host": "kotel-pg",
    "port": "5432",
    "database": "db",
    "user": "user",
    "password": "password"
}

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

# cnums = ['Aa0BUS9', 'aa0dub9', 'aa0dXe3']
# data = []
# for i in range(100):
#     data.append((i, random.choice(cnums), str(uuid.uuid4())))
# table = table_env.from_elements(data, ['id', 'cnum', 'job_uuid'])

table = table_env.from_elements([
    (1, 'Aa0BUS9'),
    (2, 'aa0dub9'),
    (3, 'aa0dXe3'),
], ['id', 'cnum'])

def get_offer_nominations_by_cnum(connection_params, cnum):
    start_time = datetime.now()
    try:
        with psycopg2.connect(**connection_params) as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor_time = datetime.now()

            cur.execute(f"""
            SELECT 
                id
                , created_ts::text
                , event_id
                , cnum
                , camp_code
                , action_type
                , start_ts::text
                , end_ts::text
            FROM offer_nomination
            where cnum = '{cnum}'
            and now() between start_ts and end_ts
            """)

            result = cur.fetchall()
            end_time = datetime.now()

            # print(f"conn_create_time : {cursor_time - start_time}")
            # print(f"data_fetch_time : {end_time - cursor_time}")
            # print(f"total_time : {end_time - start_time}")
            # print(f"\n")

            cur.close()

            return result

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None


@udtf(result_types=[
    'STRING',
    'STRING',
    'STRING',
    'STRING',
    'STRING',
    'INT',
    'STRING',
    'STRING',
])
def get_offer_nominations_from_pg(x: Row) -> Row:
    rows = get_offer_nominations_by_cnum(connection_params, x.cnum)

    for row in rows:
        yield row['id'], row['created_ts'], row['event_id'], row['cnum'], row['camp_code'], row['action_type'], row['start_ts'], row['end_ts']


offer_nominations_table = table.flat_map(get_offer_nominations_from_pg).alias(
    "id",
    "created_ts",
    "event_id",
    "cnum",
    "camp_code",
    "action_type",
    "start_ts",
    "end_ts"
)

# result_table.execute().print()
table_env.create_temporary_view("offer_nominations_table", offer_nominations_table)

result = table_env.execute_sql(f"""
    select 
        cnum, count(*) as camp_count
    from offer_nominations_table
    group by cnum
""")

result.print()
