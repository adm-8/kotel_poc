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
            yield row['id'] , row['created_ts'], row['event_id'], row['cnum'], row['camp_code'], row['action_type'], row['start_ts'], row['end_ts']
            # yield '|'.join(row)


env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)
t_env = StreamTableEnvironment.create(env)

data = [
    (1, 'Aa0BUS9'),
    (2, 'aa0dub9'),
    (3, 'aa0dXe3'),
]

cnum_processor_ds = env.from_collection(
    collection=data,
    type_info=Types.ROW([Types.INT(), Types.STRING()])
)
#cnum_processor_ds.print() # WORKS

offer_nominations_ds = cnum_processor_ds.flat_map(CustomFlatMapFunction())
# offer_nominations_ds.print() # WORKS

# offer_nominations_schema = DataTypes.ROW([
#     DataTypes.FIELD("id", DataTypes.STRING()),
#     DataTypes.FIELD("created_ts", DataTypes.STRING()),
#     DataTypes.FIELD("event_id", DataTypes.STRING()),
#     DataTypes.FIELD("cnum", DataTypes.STRING()),
#     DataTypes.FIELD("camp_code", DataTypes.STRING()),
#     DataTypes.FIELD("action_type", DataTypes.INT()),
#     DataTypes.FIELD("start_ts", DataTypes.STRING()),
#     DataTypes.FIELD("end_ts", DataTypes.STRING())
# ])

# offer_nominations_schema = (
# Schema.new_builder()
# .column("id", "STRING")
# .column("created_ts", "STRING")
# .column("event_id", "STRING")
# .column("cnum", "STRING")
# .column("camp_code", "STRING")
# .column("action_type", "INT")
# .column("start_ts", "STRING")
# .column("end_ts", "STRING")
# .build()
# )


debug_t = t_env.from_data_stream(offer_nominations_ds)
res = debug_t.execute()
with res.collect() as results:
    for row in results:
        print(row[0])


# schema = DataTypes.ROW((
    # DataTypes.FIELD("id", DataTypes.STRING()),
    # DataTypes.FIELD("created_ts", DataTypes.STRING()),
    # DataTypes.FIELD("event_id", DataTypes.STRING()),
    # DataTypes.FIELD("cnum", DataTypes.STRING()),
    # DataTypes.FIELD("camp_code", DataTypes.STRING()),
    # DataTypes.FIELD("action_type", DataTypes.STRING()),
    # DataTypes.FIELD("start_ts", DataTypes.STRING()),
    # DataTypes.FIELD("end_ts", DataTypes.STRING())
# ))

# type_info = Types.ROW_NAMED(
#     ['id', 'created_ts', 'event_id', 'cnum', 'camp_code', 'action_type', 'start_ts', 'end_ts'],
#     [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]
# )
# offer_nominations_ds = offer_nominations_ds.map(lambda x: x, output_type=type_info)
# offer_nominations_ds.print()

# offer_nominations_t = t_env.from_data_stream(offer_nominations_ds)

# ("id", "created_ts", "event_id", "cnum", "camp_code", "action_type", "start_ts", "end_ts")

# t_env.create_temporary_view("offer_nominations_t", offer_nominations_t)
# WORKS

# result_table = t_env.sql_query("select * from offer_nominations_t")
# result_table.execute().print()

# Execute the job
env.execute("DataStream from List Job")








# offer_nominations_table = t_env.from_data_stream(offer_nominations_ds)  # , offer_nominations_schema
# offer_nominations_table.create_temporary_view("offer_nominations_table")

# offer_nominations_table.execute().print()

# Register the DataStream as a table
# env.create_temporary_view(
#     "offer_nominations_table",
#     offer_nominations_ds,
#     "id, created_ts, event_id, cnum, camp_code, action_type, start_ts, end_ts"
# )

# result = t_env.execute_sql(f"""
#     select
#         cnum, count(*) as camp_count
#     from offer_nominations_table
#     group by cnum
# """)

# result = env.execute_sql("select * from offer_nominations_table")
# result.print()
