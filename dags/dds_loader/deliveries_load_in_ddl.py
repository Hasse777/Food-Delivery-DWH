from logging import Logger
from typing import List

from lib import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime

class DDS_deliveries_obj(BaseModel):
    delivery_id: str
    order_id: str
    courier_id: str
    rate: int  # В базе данных я присвоил на всякий случай NUMERIC(3, 2), но в источнике приходит int
    tip_sum: int  # tip_sum тоже приходит int
    order_ts: datetime


class DDS_deliveries_Loader:
    WF_KEY = 'stg_deliveries_to_dm_deliveries'
    LAST_LOADED_ID_KEY = 'last_loaded_ts'
    BATCH_LIMIT = 10000

    def __init__(self, pg_DWH: PgConnect, log: Logger):
        self.pg_DWH = pg_DWH
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_deliveries(self):
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
            load_queue = self.dwh_list_deliveries(conn, last_loaded_ts, self.BATCH_LIMIT)
            self.log.info(f'starting to load deliveries from last checkpoint: {last_loaded_ts}')

            if not load_queue:
                self.log.info('Quitting')
                return 0

            i = 0
            for d in load_queue:
                self.dwh_load_deliveries(conn, d)

                i += 1

            self.log.info(f'loaded {i} deliveries from {len(load_queue)}')
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.order_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, 'dds')

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)

    def dwh_list_deliveries(self, conn: Connection, delivery_threshold: datetime, limit: int) -> List[DDS_deliveries_obj]:
        """
        Инкрементально загружаем список достаовк, которых еще нет в DDS слое.
        """
        with conn.cursor(row_factory=class_row(DDS_deliveries_obj)) as cur:
            cur.execute(
                """
                    SELECT
                        delivery_id,
                        courier_id,
                        order_id,
                        rate,
                        tip_sum,
                        order_ts
                    FROM stg.yandex_cloud_deliveries
                    WHERE order_ts > %(threshold)s
                    ORDER BY order_ts
                    LIMIT %(limit)s;
                """, {
                    'threshold': delivery_threshold,
                    'limit': limit
                }
            )
            objs = cur.fetchall()
        return objs

    def dwh_load_deliveries(self, conn: Connection, delivery: DDS_deliveries_obj):
        """
        Переносим данные по доставкам в БД.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_deliveries(delivery_id, order_key, courier_id, rate, tip_sum)
                SELECT
                    %(delivery_id)s::VARCHAR,
                    %(order_id)s::VARCHAR,
                    dc.id::INT,
                    %(rate)s::NUMERIC(3, 2),
                    %(tip_sum)s::NUMERIC(19, 2)
                FROM dds.dm_couriers as dc
                WHERE %(courier_id)s = dc.courier_id
                ON CONFLICT(delivery_id) DO UPDATE
                SET
                    rate = EXCLUDED.rate,
                    tip_sum = EXCLUDED.tip_sum;
                """, {
                    'delivery_id': delivery.delivery_id,
                    'courier_id': delivery.courier_id,
                    'order_id': delivery.order_id,
                    'rate': delivery.rate,
                    'tip_sum': delivery.tip_sum
                }
            )
