import uuid

from pyflink.common import Row, Types
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings, Schema
from pyflink.datastream import FlatMapFunction, StreamExecutionEnvironment
import psycopg2
import psycopg2.extras

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode


class CustomFlatMapFunction(FlatMapFunction):
    def __init__(self):
        self.conn = None

    def open(self, runtime_context):
        self.conn = psycopg2.connect(
            host="kotel-pg",
            port="5432",
            database="db",
            user="user",
            password="password"
        )
        print("Opened CustomFlatMapFunction connection ...")

    def flat_map(self, row: Row):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(f"""
            SELECT 
                id::text as id
                , '{row[0]}' as queue_job_id
                , cast('{row[2]}' as TIMESTAMP(3)) as queue_job_created_ts
                , created_ts as created_ts
                , event_id::text as event_id
                , cnum::text as cnum
                , camp_code::text as camp_code
                , action_type
                , start_ts as start_ts
                , end_ts as end_ts
            FROM offer_nomination
            where cnum = '{row[1]}'
            and now() between start_ts and end_ts
        """)

        result = cur.fetchall()
        cur.close()
        for row in result:
            yield Row(
                row['id'],
                row['queue_job_id'],
                row['queue_job_created_ts'],
                row['created_ts'],
                row['event_id'],
                row['cnum'],
                row['camp_code'],
                row['action_type'],
                row['start_ts'],
                row['end_ts']
            )


env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars(
    "file:///opt/sql-client/lib/flink-sql-connector-kafka-3.3.0-1.19.jar",
    "file:///opt/sql-client/lib/flink-connector-jdbc-3.2.0-1.19.jar",
    "file:///opt/sql-client/lib/postgresql-42.7.4.jar",
    "file:///opt/sql-client/lib/mysql-connector-java-8.0.30.jar"
)

# env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_parallelism(1)
t_env = StreamTableEnvironment.create(env)

data = [
    (str(uuid.uuid4()), 'Aa0BUS9', '2025-03-06 11:49:40.261 +03:00'),
    (str(uuid.uuid4()), 'aa0dub9', '2025-03-06 11:49:40.261 +03:00'),
    (str(uuid.uuid4()), 'aa0dXe3', '2025-03-06 11:49:40.261 +03:00'),
    (str(uuid.uuid4()), 'Aa0B2US', '2025-03-06 11:49:40.261 +03:00'),
    (str(uuid.uuid4()), 'aa0dusb', '2025-03-06 11:49:40.261 +03:00'),
    (str(uuid.uuid4()), 'aa0dXed', '2025-03-06 11:49:40.261 +03:00'),
    (str(uuid.uuid4()), 'A0B2US9', '2025-03-06 11:49:40.261 +03:00'),
    (str(uuid.uuid4()), 'a0dusb9', '2025-03-06 11:49:40.261 +03:00'),
    (str(uuid.uuid4()), 'a0dXed3', '2025-03-06 11:49:40.261 +03:00'),
    (str(uuid.uuid4()), 'Aa0BUS9', '2025-03-06 11:47:40.261 +03:00'),
    (str(uuid.uuid4()), 'aa0dub9', '2025-03-06 11:47:40.261 +03:00'),
    (str(uuid.uuid4()), 'aa0dXe3', '2025-03-06 11:47:40.261 +03:00'),
    (str(uuid.uuid4()), 'Aa0B2US', '2025-03-06 11:47:40.261 +03:00'),
    (str(uuid.uuid4()), 'aa0dusb', '2025-03-06 11:47:40.261 +03:00'),
    (str(uuid.uuid4()), 'aa0dXed', '2025-03-06 11:47:40.261 +03:00'),
    (str(uuid.uuid4()), 'A0B2US9', '2025-03-06 11:47:40.261 +03:00'),
    (str(uuid.uuid4()), 'a0dusb9', '2025-03-06 11:47:40.261 +03:00'),
    (str(uuid.uuid4()), 'a0dXed3', '2025-03-06 11:47:40.261 +03:00'),
]

cnum_processor_ds = env.from_collection(
    collection=data,
    type_info=Types.ROW([Types.STRING(), Types.STRING(), Types.STRING()])
)
# cnum_processor_ds.print() # WORKS

offer_nominations_schema = Types.ROW([
    Types.STRING(),
    Types.STRING(),
    Types.SQL_TIMESTAMP(),
    Types.SQL_TIMESTAMP(),
    Types.STRING(),
    Types.STRING(),
    Types.STRING(),
    Types.INT(),
    Types.SQL_TIMESTAMP(),
    Types.SQL_TIMESTAMP()
])

flat_map_func = CustomFlatMapFunction()

offer_nominations_ds = cnum_processor_ds.flat_map(flat_map_func, output_type=offer_nominations_schema)  # CustomFlatMapFunction()
# wm_offer_nominations_ds = offer_nominations_ds.assign_timestamp_and_watermarks() # TODO: check this
# offer_nominations_ds.print() # WORKS

offer_nominations_t = t_env.from_data_stream(offer_nominations_ds).alias(
    "id",
    "queue_job_id",
    "queue_job_created_ts",
    "created_ts",
    "event_id",
    "cnum",
    "camp_code",
    "action_type",
    "start_ts",
    "end_ts",
)

t_env.create_temporary_view("offer_nominations_t", offer_nominations_t)
t_env.sql_query("select * from offer_nominations_t").execute().print()

print(offer_nominations_t.get_schema())

# debug_t = t_env.from_data_stream(offer_nominations_ds).alias("id")
# res = debug_t.execute()
# with res.collect() as results:
#     for row in results:
#         print(row)

t_env.execute_sql(f"""
CREATE TABLE customer_offer_state_sink (
    id STRING,
    queue_job_id STRING,
    queue_job_created_ts TIMESTAMP(3),
    cnum STRING,
    camp_code STRING,
    camp_action STRING,
    PRIMARY KEY (cnum, camp_code, queue_job_id) NOT ENFORCED
)
WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://kotel-pg:5432/db',
    'table-name' = 'customer_offer_state',
    'username' = 'user',
    'password' = 'password'
)
""")

# sql_query
t_env.execute_sql("""
INSERT INTO customer_offer_state_sink (
    queue_job_id,
    queue_job_created_ts,
    cnum,
    camp_code,
    camp_action
)
SELECT
    queue_job_id,
    queue_job_created_ts,
    cnum,
    camp_code,
    camp_action
FROM (
    SELECT
        --window_start, window_end,
        cnum,
        camp_code,
        queue_job_id,
        queue_job_created_ts,
        CASE WHEN min(action_type) = 0 THEN 'disabled' ELSE 'enabled' END AS camp_action
    FROM offer_nominations_t
/*
    TABLE (
        TUMBLE(TABLE offer_nominations_with_rowtime_t, DESCRIPTOR(queue_job_created_ts), INTERVAL '1' SECOND)
    )
*/
    where LOCALTIMESTAMP between start_ts and end_ts
    GROUP BY
        -- window_start, window_end,
        cnum, camp_code, queue_job_id, queue_job_created_ts
)
""")

# Execute the job
env.execute("DataStream from List Job")
