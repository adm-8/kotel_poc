import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from faker import Faker
import uuid
from datetime import datetime, timedelta
import random
from concurrent.futures import ThreadPoolExecutor
import time
import statistics
import threading
from tqdm import tqdm

# Конфигурация
DB_CONFIG = {
    'dbname': 'db',
    'user': 'user',
    'password': 'password',
    'host': 'localhost',
    'port': '5678'
}
TABLE_NAME = 'offer_nomination'
ROWS_TO_INSERT = 1_000_000  # Количество строк для вставки, можно изменить на XX миллионов
BATCH_SIZE = 10_000  # Размер батча для вставки, оптимизируется по необходимости

CAMP_COUNT = 50
CNUM_COUNT = 100_000

Faker.seed(0)  # Для воспроизводимости данных
fake = Faker()

print(f"Формируем {CAMP_COUNT} кампаний")
camp_code_list = []
for x in range(CAMP_COUNT):
    camp_code_list.append(f"CAMP_{10 + x}")

print(f"Формируем {CNUM_COUNT} CNUMов")
cnum_list = []
for _ in range(CNUM_COUNT):
    cnum_list.append(fake.bothify(text='??#???#'))

# Генерация данных
def generate_data(batch_size):
    data = []
    for _ in range(batch_size):
        record = (
            str(uuid.uuid4()),
            fake.date_time_this_decade().isoformat(),
            str(uuid.uuid4()),
            random.choice(cnum_list),
            random.choice(camp_code_list),
            random.randint(0, 1),
            (datetime.now() + timedelta(days=random.randint(-365, 365))).isoformat(),
            (datetime.now() + timedelta(days=random.randint(1, 365))).isoformat()
        )
        data.append(record)
    return data

# Подключение к базе данных
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

print(f"Начинаем вставку данных ...")
# Вставка данных с обработкой ошибок
try:
    for start in range(0, ROWS_TO_INSERT, BATCH_SIZE):
        end = min(start + BATCH_SIZE, ROWS_TO_INSERT)
        data = generate_data(end - start)
        insert_query = sql.SQL("""
            INSERT INTO {table} (id, created_ts, event_id, cnum, camp_code, action_type, start_ts, end_ts)
            VALUES %s
        """).format(table=sql.Identifier(TABLE_NAME))
        execute_values(cursor, insert_query, data)
        conn.commit()  # Обязательно после каждого успешного батча
        print(f"Вставлено {end} строк из {ROWS_TO_INSERT}")
except Exception as e:
    print(f"Ошибка при вставке данных: {e}")
    conn.rollback()  # Откат в случае ошибки
finally:
    cursor.close()
    conn.close()
    print("Скрипт завершен.")


# # Запрос для тестирования
# def create_customer_offer_state_query(days):
#     QUERY = sql.SQL("""
#     insert into customer_offer_state (
#         id,
#         queue_job_id,
#         created_ts,
#         cnum,
#         camp_code,
#         camp_action
#     )
#     select
#         id,
#         queue_job_id,
#         created_ts,
#         cnum,
#         camp_code,
#         camp_action
#     from (
#         SELECT
#             uuid_generate_v4() as id,
#             %s::uuid as queue_job_id,
#             now() as created_ts,
#             cnum,
#             camp_code,
#             CASE WHEN min(action_type) = %s THEN 'disabled' ELSE 'enabled' END AS camp_action
#         FROM offer_nomination
#         WHERE
#             cnum = %s
#             AND now() + interval '{} days' BETWEEN start_ts AND end_ts
#         GROUP BY cnum, camp_code
#     )
#     where camp_action = 'enabled'
#     """).format(sql.SQL(str(days)))
#
#     return QUERY
#
# def create_action_query(queue_job_id, cnum):
#     QUERY = sql.SQL(f"""
#         with prev_job_id as (
#             select
#                 queue_job_id
#             from customer_offer_state
#             where cnum = '{cnum}'
#             and created_ts < (
#                 select created_ts from customer_offer_state
#                 where queue_job_id = '{queue_job_id}'::uuid
#                 limit 1
#             )
#             order by created_ts desc
#             limit 1
#         )
#         , prev_state as (
#             select *
#             from customer_offer_state
#             where queue_job_id = (select queue_job_id from prev_job_id)
#         )
#         , curr_state as (
#             select * from customer_offer_state
#             where queue_job_id = '{queue_job_id}'::uuid
#         )
#         , full_joined as (
#             select
#                 cs.cnum as c_cnum
#                 , cs.camp_code as c_camp_code
#                 , '|'
#                 , ps.cnum as p_cnum
#                 , ps.camp_code as p_camp_code
#             from curr_state cs
#             full join prev_state ps
#                 on cs.camp_code = ps.camp_code
#         )
#         , act as (
#             select
#                 coalesce(c_cnum, p_cnum) as cnum
#                 , coalesce(c_camp_code, p_camp_code) as camp_code
#                 , case
#                     when p_cnum is null then 'enable'
#                     when c_cnum is null then 'disable'
#                     else 'do_nothing'
#                 end as action_type
#             from full_joined fj
#             where c_cnum is null or p_cnum is null
#         )
#         insert into offer_action (
#             id,
#             created_ts,
#             cnum,
#             camp_code,
#             action_type,
#             camp_action
#         )
#         select
#             uuid_generate_v4() as id
#             , now() as created_ts
#             , cnum
#             , camp_code
#             , action_type
#             , lower(camp_code || '__' || action_type) as camp_action
#         from act
#         """)
#
#     return QUERY
#
#
# # Настройки нагрузки
# TARGET_RPS = 1000  # Пример: 1000 RPS
# THREAD_COUNT = 100  # Количество потоков для выполнения запросов
# TOTAL_DURATION = 10  # Продолжительность теста в секундах
# ITERATIONS_PER_THREAD = TARGET_RPS * TOTAL_DURATION // THREAD_COUNT
#
# # Функция для выполнения запроса и измерения времени
# def execute_query(action_type, cnum):
#     with psycopg2.connect(**DB_CONFIG) as conn:
#         with conn.cursor() as cursor:
#             start_time = time.time()
#
#             queue_job_id = str(uuid.uuid4())
#             cnum_r = random.choice(cnum_list)
#
#             # insert customer_offer_state data
#             QUERY = create_customer_offer_state_query(random.randint(-100, 100))
#             cursor.execute(QUERY, (
#                 queue_job_id,
#                 action_type,
#                 cnum_r
#             ))
#
#             # insert offer_action data
#             QUERY = create_action_query(queue_job_id, cnum_r)
#             cursor.execute(QUERY)
#
#             # cursor.fetchall()  # Выполнение запроса и получение результатов
#             elapsed_time = time.time() - start_time
#             return elapsed_time
#
# # Функция для многопоточного выполнения запроса с контролем RPS и прогресс-баром
# def test_query_performance(action_type, cnum, target_rps, iterations):
#     times = []
#     lock = threading.Lock()
#     start_time = time.time()
#     for _ in tqdm(range(iterations), desc=f"Thread {threading.get_ident()}", leave=False):
#         start_loop = time.time()
#         elapsed_time = execute_query(action_type, cnum)
#         with lock:
#             times.append(elapsed_time)
#         # Контроль времени для поддержания RPS
#         elapsed_loop = time.time() - start_loop
#         sleep_time = 1 / target_rps - elapsed_loop
#         if sleep_time > 0:
#             time.sleep(sleep_time)
#     return times, time.time() - start_time
#
# # Запуск тестирования
# if __name__ == '__main__':
#     action_type = 1  # Пример значения для action_type
#     cnum = 'oBXRahX'  # Пример значения для cnum
#     all_times = []
#     total_duration = 0
#
#     start_time = time.time()
#
#     with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
#         futures = [executor.submit(test_query_performance, action_type, cnum, TARGET_RPS, ITERATIONS_PER_THREAD) for _ in range(THREAD_COUNT)]
#         for future in futures:
#             times, duration = future.result()
#             all_times.extend(times)
#             total_duration += duration
#
#     # Вывод статистики
#     avg_time = statistics.mean(all_times)
#     min_time = min(all_times)
#     max_time = max(all_times)
#
#     total_duration = time.time() - start_time
#
#     print("\nРезультаты тестирования:")
#     print(f"Среднее время выполнения запроса: {avg_time:.3f} секунд")
#     print(f"Минимальное время выполнения запроса: {min_time:.3f} секунд")
#     print(f"Максимальное время выполнения запроса: {max_time:.3f} секунд")
#     print(f"Общее количество выполнений: {len(all_times)}")
#     print(f"Стандартное отклонение: {statistics.stdev(all_times):.3f} секунд")
#     print(f"Общая продолжительность теста: {total_duration:.2f} секунд")
#     print(f"Эффективная RPS: {len(all_times) / total_duration:.2f}")



