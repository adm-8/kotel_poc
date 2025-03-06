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
                , created_ts::text as created_ts
                , event_id::text as event_id
                , cnum::text as cnum
                , camp_code::text as camp_code
                , action_type::text as action_type
                , start_ts::text as start_ts
                , end_ts::text as end_ts
            FROM offer_nomination
            where cnum = '{row[1]}'
            and now() between start_ts and end_ts
        """)

        result = cur.fetchall()
        cur.close()
        for row in result:
            yield Row(
                row['id'],
                row['created_ts'],
                row['event_id'],
                row['cnum'],
                row['camp_code'],
                row['action_type'],
                row['start_ts'],
                row['end_ts']
            )

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)
t_env = StreamTableEnvironment.create(env)

data = [
    (1, 'Aa0BUS9'),
    (2, 'aa0dub9'),
    (3, 'aa0dXe3'),
    (4, 'Aa0B2US'),
    (5, 'aa0dusb'),
    (6, 'aa0dXed'),
    (7, 'A0B2US9'),
    (8, 'a0dusb9'),
    (9, 'a0dXed3'),
]

cnum_processor_ds = env.from_collection(
    collection=data,
    type_info=Types.ROW([Types.INT(), Types.STRING()])
)
#cnum_processor_ds.print() # WORKS

offer_nominations_schema = Types.ROW([
    Types.STRING(),
    Types.STRING(),
    Types.STRING(),
    Types.STRING(),
    Types.STRING(),
    Types.STRING(),
    Types.STRING(),
    Types.STRING()
])

flat_map_func = CustomFlatMapFunction()

offer_nominations_ds = cnum_processor_ds.flat_map(flat_map_func, output_type=offer_nominations_schema) # CustomFlatMapFunction()
# offer_nominations_ds.print() # WORKS
offer_nominations_t = t_env.from_data_stream(offer_nominations_ds).alias(
    "id",
    "created_ts",
    "event_id",
    "cnum",
    "camp_code",
    "action_type",
    "start_ts",
    "end_ts",
)

# debug_t = t_env.from_data_stream(offer_nominations_ds).alias("id")
# res = debug_t.execute()
# with res.collect() as results:
#     for row in results:
#         print(row)


t_env.create_temporary_view("offer_nominations_t", offer_nominations_t)
t_env.sql_query("""
    select 
        cnum, count(*)
    from offer_nominations_t
    group by cnum
""").execute().print()


# Execute the job
env.execute("DataStream from List Job")

