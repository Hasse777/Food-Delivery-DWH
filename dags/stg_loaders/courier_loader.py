import json
from logging import Logger
from typing import List, Dict
import time

from psycopg import Connection
from airflow.models import Variable

from API_code.object_for_API import YandexAPI
from lib import PgConnect


class CourierAPILoader:
    """
     Класс для получения курьеров из источника и дальнейшей загрузки их
     в базу данных.
    """
    # Лимит курьеров, которых мы будем принимать
    LIMIT_LOAD = 1000

    # Лимит ответа по API
    LIMIT_API_SIZE = 50

    def __init__(self, pg_dest: PgConnect, log: Logger):
        self._pg_dest = pg_dest
        self._object_yan_api = YandexAPI(nickname=Variable.get('NICKNAME'), cohort=Variable.get('COHORT'), api_key=Variable.get('API_KEY'))
        self.log = log

    def load_couriers(self):
        """
        Загружает курьеров из API и сохраняет в STG-таблицу.
        """
        list_couriers = list()
        offset = 0

        while len(list_couriers) < self.LIMIT_LOAD:
            data_list = self._object_yan_api.get_couriers(
                offset=offset,
                limit=self.LIMIT_API_SIZE
            )

            if not data_list:
                break

            offset += self.LIMIT_API_SIZE
            list_couriers.extend(data_list)

            # Чтобы не спамить источник поставил задержку между запросами.
            time.sleep(2)

        if not list_couriers:
            self.log.info('Список из источника пуст.')
            return

        self.log.info(f'Получено {len(list_couriers)} курьеров из источника.')

        # Загружаем курьеров в БД.
        self.load_in_stg(list_couriers)

    def load_in_stg(self, list_couriers: List[Dict]):
        """
        Метод устанавливает соединение с БД и загружает курьеров.
        """
        with self._pg_dest.connection() as conn:
            with conn.cursor() as cur:
                for courier in list_couriers:
                    cur.execute(
                        """
                            INSERT INTO stg.yandex_cloud_couriers(courier_id, name, full_info_about_couriers)
                            VALUES(%(courier_id)s, %(name)s, %(full_info_about_couriers)s::json)
                            ON CONFLICT(courier_id) DO NOTHING;
                        """,
                        {
                            'courier_id': courier['_id'],
                            'name': courier['name'],
                            'full_info_about_couriers': json.dumps(courier)
                        }
                    )

                self.log.info('Курьеры загружены в базу данных.')
