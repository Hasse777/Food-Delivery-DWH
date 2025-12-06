from logging import Logger
from typing import List

from lib import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DDS_timestamps_obj(BaseModel):
    id: int
    timestamps_info: str
    update_ts: datetime


class DDS_timestamp_Loader:
    WF_KEY = 'stg_timestamp_to_dm_timestamp'
    LAST_LOADED_ID_KEY = 'last_loaded_ts'
    BATCH_LIMIT = 10000

    def __init__(self, pg_DWH: PgConnect, log: Logger):
        self.pg_DWH = pg_DWH
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_timestamps(self):
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
            load_queue = self.dwh_list_timestamps(conn, last_loaded_ts, self.BATCH_LIMIT)
            self.log.info(f'starting to load from last checkpoint: {last_loaded_ts}')

            if not load_queue:
                self.log.info('Quitting')
                return 0

            i = 0
            for d in load_queue:
                self.dwh_load_timestamps(conn, d)

                i += 1

            self.log.info(f'loaded {i} timestamps from {len(load_queue)}')
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.update_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, 'dds')

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)

    def dwh_list_timestamps(self, conn: Connection, timestamps_threshold: datetime, limit: int) -> List[DDS_timestamps_obj]:
        with conn.cursor(row_factory=class_row(DDS_timestamps_obj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_value AS timestamps_info,
                        update_ts
                    FROM stg.ordersystem_orders
                    WHERE update_ts > %(threshold)s
                    ORDER BY update_ts
                    LIMIT %(limit)s;
                """, {
                    'threshold': timestamps_threshold,
                    'limit': limit
                }
            )
            filtered_objs = cur.fetchall()
            objs = list()
            for obj in filtered_objs:
                parsed = str2json(obj.timestamps_info)
                if parsed['final_status'] in ['CLOSED', 'CANCELLED']:
                    objs.append(obj)
        return objs

    def dwh_load_timestamps(self, conn: Connection, timestamps: DDS_timestamps_obj):
        with conn.cursor() as cur:
            timestamps_full_info = str2json(timestamps.timestamps_info)
            ts_str = timestamps_full_info['date']
            ts_dt = datetime.fromisoformat(ts_str)

            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES(%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT(ts) DO NOTHING;
                """, {
                    'ts': ts_dt,
                    'year': ts_dt.year,
                    'month': ts_dt.month,
                    'day': ts_dt.day,
                    'time': ts_dt.time(),
                    'date': ts_dt.date()
                }
            )
