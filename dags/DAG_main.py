import logging
import os
from pathlib import Path

import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

from lib import ConnectionBuilder, MongoConnect
from stg_loaders.ranks_loader import RankLoader
from stg_loaders.users_load import UsersLoader
from stg_loaders.schema_init import SchemaDdl
from stg_loaders.event_load import EventsLoader
from stg_loaders.restaurant_mongo_loader import RestaurantMongoLoader
from stg_loaders.users_mongo_loader import UserMongoLoader
from stg_loaders.order_mongo_loader import OrderMongoLoader
from stg_loaders.mongo_reader_dr.mongo_reader import MongoReader
from stg_loaders.courier_loader import CourierAPILoader
from stg_loaders.deliveries_loader import DeliveriesAPILoader
from stg_loaders.mongo_saver_dr.pg_saver import PgSaver
from dds_loader.user_load_in_ddl import DDS_users_Loader
from dds_loader.restaurant_load_in_ddl import DDS_restaurant_Loader
from dds_loader.timestamps_load_in_ddl import DDS_timestamp_Loader
from dds_loader.couriers_loader_ddl import DDS_couriers_Loader
from dds_loader.deliveries_load_in_ddl import DDS_deliveries_Loader
from dds_loader.orders_load_in_ddl import DDS_orders_Loader
from dds_loader.products_load_in_ddl import DDS_products_Loader
from dds_loader.fct_product_sales_load_in_ddl import DDS_events_Loader

log = logging.getLogger(__name__)


@dag(
    dag_id='full_project_DAG',
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=['project'],
    default_args={
        'retries': 4
    }
)
def full_dags():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    # Забираем путь до каталога с SQL-файлами из переменных Airflow.
    ddl_path = Variable.get("STG_DDL_FILES_PATH")
    cdm_path = Variable.get("CDM_PATH_SCRIPTS")

# ------------------------------------------TASKS НАЧАЛО--------------------------------------------------------------------------------------------------------------------

    @task(task_id="creation_all_schemes_and_tables_in_stg")
    def schemes_and_tables_init():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.init_schema(ddl_path)

    # Загрузка ранга из источника postgreSQL.
    @task(task_id="ranks_load")
    def load_ranks():
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()

    # Загрузка пользователей из источника postgreSQL
    @task(task_id="user_from_postgr_load_to_stg")
    def load_users_from_postgr():
        rest_loader = UsersLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()

    # Загрузка таблицы событий из источника postgreSQL
    @task(task_id="events_from_postgr_load_to_str")
    def load_events_from_postgr():
        rest_loader = EventsLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_events()

    # Загрузка таблицы ресторанов из источника Mongo
    @task(task_id="restaurants_from_Mongo_load_to_stg")
    def load_restaurants_from_Mongo():
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = MongoReader(mongo_connect)

        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        rest_loader = RestaurantMongoLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        rest_loader.run_copy()

    # Загрузка таблицы users из источника Mongo
    @task(task_id="users_from_Mongo_load_to_stg")
    def load_users_from_Mongo():
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = MongoReader(mongo_connect)

        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        rest_loader = UserMongoLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        rest_loader.run_copy()

    # Загрузка таблицы orders из источника Mongo
    @task(task_id="orders_from_Mongo_load_to_stg")
    def load_orders_from_Mongo():
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = MongoReader(mongo_connect)

        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        rest_loader = OrderMongoLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        rest_loader.run_copy()

    # Task загружает данные по курьерам в БД.
    @task(task_id="load_couriesAPI_to_stg")
    def load_couriers_to_stg():
        rest_loader = CourierAPILoader(dwh_pg_connect, log)
        rest_loader.load_couriers()

    # Task Загружает доставки в БД
    @task(task_id="load_deliveriesAPI_to_stg")
    def load_deliveries_to_stg():
        rest_loader = DeliveriesAPILoader(dwh_pg_connect, log)
        rest_loader.load_deliveries()

    # ---------------------------------------НАЧИНАЕМ ПЕРЕНОСИТЬ ДАННЫЕ ИЗ STG в DDS слой---------------------------------------------------------------------------
    # Task Промежуточный таск чтобы соединять списки тасков
    @task(task_id="stg_all_loaded")
    def stg_all_loaded():
        pass

    # Task Загружаем пользователей в DDS
    @task(task_id="load_users_to_dds")
    def load_users_to_dds():
        rest_loader = DDS_users_Loader(dwh_pg_connect, log)
        rest_loader.load_users()

    # Task Загружаем рестораны в DDS
    @task(task_id="load_restaurants_to_dds")
    def load_restaurants_to_dds():
        rest_loader = DDS_restaurant_Loader(dwh_pg_connect, log)
        rest_loader.load_restaurant()

    # Task Переносим все данные по датае заказов в DDS
    @task(task_id="load_timestamp_to_dds")
    def load_timestamp_to_dds():
        rest_loader = DDS_timestamp_Loader(dwh_pg_connect, log)
        rest_loader.load_timestamps()

    # Task Переносим все данные по курьерам в DDS
    @task(task_id="load_couriers_to_dds")
    def load_couriers_to_dds():
        rest_loader = DDS_couriers_Loader(dwh_pg_connect, log)
        rest_loader.load_couriers()

    # Task Промежуточный таск чтобы соединять списки тасков
    @task(task_id="ddl_measurements_stage_two")
    def ddl_measur_stage_two():
        pass

    # Task Переносим все данные по доставкам в DDS
    @task(task_id="load_deliveries_to_dds")
    def load_deliveries_to_dds():
        rest_loader = DDS_deliveries_Loader(dwh_pg_connect, log)
        rest_loader.load_deliveries()

    # Task Переносим все данные по заказам в DDS
    @task(task_id="load_orders_to_dds")
    def load_orders_to_dds():
        rest_loader = DDS_orders_Loader(dwh_pg_connect, log)
        rest_loader.load_orders()

    # Task Переносим все продукты в DDS
    @task(task_id="load_products_to_dds")
    def load_products_to_dds():
        rest_loader = DDS_products_Loader(dwh_pg_connect, log)
        rest_loader.load_products()

    # Task Переносим все ивенты в таблицу фактов в DDS
    @task(task_id="load_events_to_ddsFK")
    def load_events_to_ddsFK():
        rest_loader = DDS_events_Loader(dwh_pg_connect, log)
        rest_loader.load_events()

    @task(task_id="recount_CDM_tables")
    def recount_CDM_tables():
        # Выполняем все скрипты по заполнению витрин.
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.init_schema(cdm_path)

# ------------------------------------------TASKS КОНЕЦ----------------------------------------------------------------------------------------------------------------

    # Инициализируем объявленные таски.
    # STG таски по переносу данных из источников
    task_creation_all_schemes_and_tables_in_stg = schemes_and_tables_init()
    task_ranks_dict = load_ranks()
    task_load_users_from_postgr = load_users_from_postgr()
    task_load_events_from_postgr = load_events_from_postgr()
    task_load_restaurants_from_Mongo = load_restaurants_from_Mongo()
    task_load_users_from_Mongo = load_users_from_Mongo()
    task_load_orders_from_Mongo = load_orders_from_Mongo()
    task_load_couriesAPI_to_stg = load_couriers_to_stg()
    task_load_deliveriesAPI_to_stg = load_deliveries_to_stg()

    # DDS таски переноса данных из STG
    task_load_users_to_dds = load_users_to_dds()
    task_stg_all_loaded = stg_all_loaded()
    task_load_restaurants_to_dds = load_restaurants_to_dds()
    task_load_timestamp_to_dds = load_timestamp_to_dds()
    task_load_couriers_to_dds = load_couriers_to_dds()

    # DDS Второй этап, загружаем измерения, у которых есть внешние ключи
    task_ddl_measur_stage_two = ddl_measur_stage_two()
    task_load_deliveries_to_dds = load_deliveries_to_dds()
    task_load_orders_to_dds = load_orders_to_dds()
    task_load_products_to_dds = load_products_to_dds()

    # DDS переносим все данные из измерений в таблицу фактов
    task_load_events_to_ddsFK = load_events_to_ddsFK()

    # CDM скрипты по подсчёту всех витрин
    task_recount_CDM_tables = recount_CDM_tables()

    # Далее задаем последовательность выполнения тасков.
    parallel_tasks_stg = [
        task_ranks_dict,
        task_load_users_from_postgr,
        task_load_events_from_postgr,
        task_load_restaurants_from_Mongo,
        task_load_users_from_Mongo,
        task_load_orders_from_Mongo,
        task_load_couriesAPI_to_stg,
        task_load_deliveriesAPI_to_stg
    ]

    parallel_tasks_dds = [
        task_load_users_to_dds,
        task_load_restaurants_to_dds,
        task_load_timestamp_to_dds,
        task_load_couriers_to_dds
    ]

    parallel_tasks_dds_stage_2 = [
        task_load_deliveries_to_dds,
        task_load_orders_to_dds,
        task_load_products_to_dds
    ]

    task_creation_all_schemes_and_tables_in_stg >> parallel_tasks_stg
    parallel_tasks_stg >> task_stg_all_loaded >> parallel_tasks_dds
    parallel_tasks_dds >> task_ddl_measur_stage_two >> parallel_tasks_dds_stage_2
    parallel_tasks_dds_stage_2 >> task_load_events_to_ddsFK >> task_recount_CDM_tables

dag = full_dags()
