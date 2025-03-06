from pyflink.common import Row, Types
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings, Schema
from pyflink.datastream import StreamExecutionEnvironment, FlatMapFunction
import json
import psycopg2
import psycopg2.extras

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode



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

cnum_processor_schema = Schema.new_builder()\
    .column("f0", "INT")\
    .column("f1", "STRING")\
    .build()

cnum_processor_t = t_env.from_data_stream(cnum_processor_ds, cnum_processor_schema).alias("id", "cnum")
t_env.create_temporary_view("cnum_processor_t", cnum_processor_t)
# t_env.sql_query("select * from cnum_processor_t").execute().print()


class GetDataFromDB(FlatMapFunction):
    def flat_map(self, row: Row):
        yield Row(row[0], row[1], 'after_flatmap')


cnum_processor_flatmap_ds = cnum_processor_ds.flat_map(
    GetDataFromDB(),
    output_type=Types.ROW([
        Types.INT(),
        Types.STRING(),
        Types.STRING(),
    ])
)
cnum_processor_flatmap_ds.print()

cnum_processor_flatmap_t = t_env.from_data_stream(cnum_processor_flatmap_ds).alias("id", "cnum", "operation")
print(cnum_processor_flatmap_t.get_schema())

t_env.create_temporary_view("cnum_processor_flatmap_t", cnum_processor_flatmap_t)
t_env.sql_query("select * from cnum_processor_flatmap_t").execute().print()

# Execute the job
env.execute("DataStream from List Job")

