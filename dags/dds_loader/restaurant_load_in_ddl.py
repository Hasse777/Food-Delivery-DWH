from logging import Logger
from typing import List

from lib import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DDS_restaurant_obj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_info: str
    update_ts: datetime


class DDS_restaurant_Loader:
    WF_KEY = 'stg_restaurant_to_dm_restaurant'
    LAST_LOADED_ID_KEY = 'last_loaded_ts'
    BATCH_LIMIT = 100

    def __init__(self, pg_DWH: PgConnect, log: Logger):
        self.pg_DWH = pg_DWH
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_restaurant(self):
        """
        Загружаем данные о ресторанах из STG в DDS слой.
        """
        with self.pg_DWH.connection() as conn:
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY, 'dds')
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0, 
                    workflow_key=self.WF_KEY, 
                    workflow_settings={self.LAST_LOADED_ID_KEY: datetime(2022, 1, 1).isoformat()}
                )

            # Вычитываем очередную пачку объектов.
            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            load_queue = self.dwh_list_restaurant(conn, last_loaded_ts, self.BATCH_LIMIT)
            self.log.info(f'starting to load from last checkpoint: {last_loaded_ts}')

            if not load_queue:
                self.log.info('Quitting')
                return 0
            
            i = 0
            for d in load_queue:
                self.dwh_load_restaurant(conn, d)

                i += 1

            self.log.info(f'loaded {i} restaurant from {len(load_queue)}')
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.update_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, 'dds')

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)

    def dwh_list_restaurant(self, conn: Connection, restaurant_threshold: datetime, limit: int) -> List[DDS_restaurant_obj]:
        """
        Инкрементально загружаем список ресторанов, которых еще нет в DDS слое.
        """
        with conn.cursor(row_factory=class_row(DDS_restaurant_obj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id AS restaurant_id,
                        object_value AS restaurant_info,
                        update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE update_ts > %(threshold)s
                    ORDER BY update_ts
                    LIMIT %(limit)s;
                """, {
                    'threshold': restaurant_threshold,
                    'limit': limit
                }
            )
            objs = cur.fetchall()
        return objs

    def dwh_load_restaurant(self, conn: Connection, restaurant: DDS_restaurant_obj):
        """
        Переносим данные по ресторанам в БД.

        Если ресторан уже есть и его имя изменилось - закрываем запись.
        Затем вставляем новую запись, если она сущетсвует.
        """
        with conn.cursor() as cur:
            restaurant_full_info = str2json(restaurant.restaurant_info)
            # Сначало обновляем историческую дату если ресторан уже есть в БД
            cur.execute(
                """
                    UPDATE dds.dm_restaurants
                    SET active_to = %(end)s
                    WHERE restaurant_id = %(rest_id)s AND active_to = '2099-12-31 00:00:00' AND restaurant_name != %(rest_name)s;
                """, {
                    'end': restaurant.update_ts,
                    'rest_id': restaurant.restaurant_id,
                    'rest_name': restaurant_full_info['name']
                }
            )

            cur.execute(
            """
                INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                SELECT 
                    %(restaurant_id)s::varchar, 
                    %(restaurant_name)s::varchar, 
                    %(active_from)s::timestamp, 
                    '2099-12-31 00:00:00'::timestamp
                WHERE NOT EXISTS (
                    SELECT 1 FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s
                    AND restaurant_name = %(restaurant_name)s
                    AND active_to = '2099-12-31 00:00:00'
                );
            """, {
                'restaurant_id': restaurant.restaurant_id,
                'restaurant_name': restaurant_full_info['name'],
                'active_from': restaurant.update_ts
            }
            )
