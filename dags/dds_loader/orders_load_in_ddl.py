from logging import Logger
from typing import List

from lib import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DDS_orders_obj(BaseModel):
    order_id: str
    order_info: str
    update_ts: datetime


class DDS_orders_Loader:
    WF_KEY = 'stg_order_to_dm_orders'
    LAST_LOADED_ID_KEY = 'last_loaded_ts'
    BATCH_LIMIT = 20000

    def __init__(self, pg_DWH: PgConnect, log: Logger):
        self.pg_DWH = pg_DWH
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_orders(self):
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
            load_queue = self.dwh_list_orders(conn, last_loaded_ts, self.BATCH_LIMIT)
            self.log.info(f'starting orders to load from last checkpoint: {last_loaded_ts}')

            if not load_queue:
                self.log.info('Quitting')
                return 0

            i = 0
            for d in load_queue:
                self.dwh_load_orders(conn, d)

                i += 1

            self.log.info(f'loaded {i} orders from {len(load_queue)}')
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.update_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, 'dds')

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
       


    def dwh_list_orders(self, conn: Connection, orders_threshold: datetime, limit: int) -> List[DDS_orders_obj]:
        with conn.cursor(row_factory=class_row(DDS_orders_obj)) as cur:
            cur.execute(
                """
                    SELECT
                        object_id AS order_id,
                        object_value AS order_info,
                        update_ts
                    FROM stg.ordersystem_orders
                    WHERE update_ts > %(threshold)s
                    ORDER BY update_ts
                    LIMIT %(limit)s;
                """, {
                    'threshold': orders_threshold,
                    'limit': limit
                }
            )
            objs = cur.fetchall()
        return objs


    def dwh_load_orders(self, conn: Connection, order: DDS_orders_obj):
        with conn.cursor() as cur:
            order_full_info = str2json(order.order_info)
            order_dt = datetime.fromisoformat(order_full_info['date'])
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, user_id, restaurant_id, timestamp_id, order_status, delivery_id)
                    SELECT
                        %(order_id)s::varchar,
                        du.id AS user_id,
                        dr.id AS restaurant_id,
                        dt.id AS timestamp_id,
                        %(status)s::varchar AS order_status,
                        dd.id AS delivery_id
                    FROM dds.dm_users AS du
                    LEFT JOIN dds.dm_restaurants AS dr ON dr.restaurant_id = %(restaurant_id)s
                    LEFT JOIN dds.dm_timestamps AS dt ON dt.ts = %(order_dt)s
                    LEFT JOIN dds.dm_deliveries AS dd ON dd.order_key = %(order_id)s
                    WHERE du.user_id = %(user_id)s
                    ON CONFLICT(order_key) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        delivery_id = EXCLUDED.delivery_id,
                        order_status = EXCLUDED.order_status;
                """, {
                    'order_id': order.order_id,
                    'status': order_full_info['final_status'],
                    'restaurant_id': order_full_info['restaurant']['id'],
                    'order_dt': order_dt,
                    'user_id': order_full_info['user']['id']
                }
            )
