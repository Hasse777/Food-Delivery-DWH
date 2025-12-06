from logging import Logger
from typing import List
from datetime import datetime

from lib import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


# Класс для работы с источником
class EventOriginRepository:
    def __init__(self, pg_origin: PgConnect) -> None:
        self._db = pg_origin

    def list_event(self, event_threshold: int, limit: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        event_ts,
                        event_type,
                        event_value
                    FROM outbox
                    WHERE id > %(threshold)s
                    ORDER BY id
                    LIMIT %(limit)s;
                """, {
                    'threshold': event_threshold,
                    'limit': limit
                }
            )

            objs = cur.fetchall()

        return objs


# Класс для работы с DWH
class EventDestRepository:

    def insert_event(self, conn: Connection, event: EventObj):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                    VALUES(%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        event_ts = EXCLUDED.event_ts,
                        event_type = EXCLUDED.event_type,
                        event_value = EXCLUDED.event_value;
                """,
                {
                    'id': event.id,
                    'event_ts': event.event_ts,
                    'event_type': event.event_type,
                    'event_value': event.event_value,
                },
            )


class EventsLoader:
    WF_KEY = 'events_origin_to_stg_workflow'
    LAST_LOADED_ID_KEY = 'last_loaded_id'
    BATCH_LIMIT = 20000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = EventOriginRepository(pg_origin=pg_origin)
        self.stg = EventDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_events(self) -> None:
        with self.pg_dest.connection() as conn:

            # Читаем состояние загрузки
            # Если настройки еще нет, заводим ее
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY, 'stg')
            if wf_setting is None:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_event(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} events to load.")
            if not load_queue:
                self.log.info('Quitting')
                return
            for event in load_queue:
                self.stg.insert_event(conn, event)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, 'stg')

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
