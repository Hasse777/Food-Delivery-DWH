from datetime import datetime, timedelta
import json
from logging import Logger
from typing import List, Dict
import time

from psycopg import Connection
from airflow.models import Variable

from API_code.object_for_API import YandexAPI
from lib import PgConnect, EtlSetting, StgEtlSettingsRepository, json2str


class DeliveriesAPILoader:
    """
     Класс для получения доставок из источника и дальнейшей загрузки их
     в базу данных.
    """
    # Лимит доставок, которых мы будем принимать
    LIMIT_LOAD = 5000

    # Лимит ответа по API
    LIMIT_API_SIZE = 50

    # Уникальный ключ данного запроса в служебной таблице
    WF_KEY = 'deliveries_origin_to_stg_workflow'

    LAST_LOADED_TS_KEY = "last_loaded_order_ts"

    def __init__(self, pg_dest: PgConnect, log: Logger):
        self._pg_dest = pg_dest
        self._object_yan_api = YandexAPI(nickname=Variable.get('NICKNAME'), cohort=Variable.get('COHORT'), api_key=Variable.get('API_KEY'))
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        """
        Загружает доставки из API и сохраняет в STG-таблицу.
        """
        with self._pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY, 'stg')

        if not wf_setting:
            wf_setting = EtlSetting(
                id=0,
                workflow_key=self.WF_KEY,
                workflow_settings={
                    self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                }
            )

        # Смотрим время последней загрузки доставок.
        last_load = datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY])
        
        # Записываем текущею дату
        now = datetime.now()

        # Разница между текущей датой и датой последней загрузки.
        delta = now - last_load

        # В задании сказано брать данные только за предыдущие 7 дней.
        # Если прошло больше 7 суток с момента последней загрузки,
        # То загружаем данные с now - timedelta(days=7) иначе
        # загружать данные начиная с last_load
        if delta > timedelta(days=7):
            from_ts = now - timedelta(days=7)
        else:
            from_ts = last_load

        # Начинаем загрузку данных.
        list_deliveries = list()
        offset = 0
        from_ts = from_ts.strftime('%Y-%m-%d %H:%M:%S')
        to_ts = now.strftime('%Y-%m-%d %H:%M:%S')
        while len(list_deliveries) < self.LIMIT_LOAD:
            data_list = self._object_yan_api.get_deliveries(
                sort_field='date',
                sort_direction='asc',
                offset=offset,
                from_date=from_ts,
                to_date=to_ts,
                limit=self.LIMIT_API_SIZE
            )

            if not data_list:
                break

            offset += self.LIMIT_API_SIZE
            list_deliveries.extend(data_list)

            # Чтобы не спамить источник поставил задержку между запросами.
            time.sleep(2)

        if not list_deliveries:
            self.log.info('Список из источника доставок пуст.')

        self.log.info(f'Получено {len(list_deliveries)} доставок из источника.')

        with self._pg_dest.connection() as conn:
            # Загружаем курьеров в БД.
            last_order_ts = self.load_in_stg(conn, list_deliveries)

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_order_ts
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, 'stg')

    def load_in_stg(self, conn: Connection, list_deliveries: List[Dict]) -> str:
        """
        Метод устанавливает соединение с БД и загружает доставки.
        """
        last_order_ts = '2022-01-01 00:00:00'
        with conn.cursor() as cur:
            for delivery in list_deliveries:
                cur.execute(
                    """
                        INSERT INTO stg.yandex_cloud_deliveries(
                            delivery_id,
                            delivery_ts,
                            courier_id,
                            order_id,
                            order_ts,
                            tip_sum,
                            rate,
                            full_info_about_deliveries
                        )
                        VALUES(
                            %(delivery_id)s,
                            %(delivery_ts)s,
                            %(courier_id)s,
                            %(order_id)s,
                            %(order_ts)s,
                            %(tip_sum)s,
                            %(rate)s,
                            %(full_info_about_deliveries)s
                        ) ON CONFLICT (delivery_id) DO UPDATE
                        SET
                            delivery_ts = EXCLUDED.delivery_ts,
                            courier_id = EXCLUDED.courier_id,
                            order_id = EXCLUDED.order_id,
                            order_ts = EXCLUDED.order_ts,
                            tip_sum = EXCLUDED.tip_sum,
                            rate = EXCLUDED.rate,
                            full_info_about_deliveries = EXCLUDED.full_info_about_deliveries,
                            load_ts = NOW();
                    """,
                    {
                        'delivery_id': delivery['delivery_id'],
                        'delivery_ts': delivery['delivery_ts'],
                        'courier_id': delivery['courier_id'],
                        'order_id': delivery['order_id'],
                        'order_ts': delivery['order_ts'],
                        'tip_sum': delivery['tip_sum'],
                        'rate': delivery['rate'],
                        'full_info_about_deliveries': json.dumps(delivery)
                    }
                )

                last_order_ts = max(last_order_ts, delivery['order_ts'])

            self.log.info('Курьеры загружены в базу данных.')
            return last_order_ts













#АААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААААА
    # def load_in_stg(self, list_couriers: List[Dict]):
    #     """
    #     Метод устанавливает соединение с БД и загружает доставки.
    #     """
    #     with self._pg_dest.connection() as conn:
    #         with conn.cursor() as cur:
    #             for courier in list_couriers:
    #                 cur.execute(
    #                     """
    #                         INSERT INTO stg.yandex_cloud_couriers(courier_id, name, full_info_about_couriers)
    #                         VALUES(%(courier_id)s, %(name)s, %(full_info_about_couriers)s::json)
    #                         ON CONFLICT(courier_id) DO NOTHING;
    #                     """,
    #                     {
    #                         'courier_id': courier['_id'],
    #                         'name': courier['name'],
    #                         'full_info_about_couriers': json.dumps(courier)
    #                     }
    #                 )

    #             self.log.info('Курьеры загружены в базу данных.')
