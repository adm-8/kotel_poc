from pyflink.common import Row, Types
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes, Schema
from pyflink.datastream import FlatMapFunction, StreamExecutionEnvironment
import psycopg2
import psycopg2.extras
import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy



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
                id
                , created_ts::text
                , event_id
                , cnum
                , camp_code
                , action_type
                , start_ts::text
                , end_ts::text
            FROM offer_nomination
            where cnum = '{row[1]}'
            and now() between start_ts and end_ts
        """)

        result = cur.fetchall()
        cur.close()
        for row in result:
            yield row['id'], row['created_ts'], row['event_id'], row['cnum'], row['camp_code'], row['action_type'], row[
                'start_ts'], row['end_ts']


env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)

data = [
    (1, 'Aa0BUS9'),
    (2, 'aa0dub9'),
    (3, 'aa0dXe3'),
]

cnum_processor_ds = env.from_collection(
    collection=data,
    type_info=Types.ROW([Types.INT(), Types.STRING()])
)
cnum_processor_ds.print()

offer_nominations_ds = cnum_processor_ds.flat_map(CustomFlatMapFunction())
offer_nominations_ds.print()

# env.create_temporary_view(
#     "offer_nominations_table",
#     offer_nominations_table,
# )
# #
# # DataTypes.ROW([
# #     DataTypes.FIELD("id", DataTypes.STRING()),
# #     DataTypes.FIELD("created_ts", DataTypes.STRING()),
# #     DataTypes.FIELD("event_id", DataTypes.STRING()),
# #     DataTypes.FIELD("cnum", DataTypes.STRING()),
# #     DataTypes.FIELD("camp_code", DataTypes.STRING()),
# #     DataTypes.FIELD("action_type", DataTypes.INT()),
# #     DataTypes.FIELD("start_ts", DataTypes.STRING()),
# #     DataTypes.FIELD("end_ts", DataTypes.STRING())
# # ])
#
#
#
# result = table_env.execute_sql(f"""
#     select
#         cnum, count(*) as camp_count
#     from offer_nominations_table
#     group by cnum
# """)

# result = env.execute_sql("select * from offer_nominations_table")
# result.print()

# Execute the job
env.execute("DataStream from List Job")