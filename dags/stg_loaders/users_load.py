from logging import Logger
from typing import List

from lib import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class UserObj(BaseModel):
    id: int
    order_user_id: str

# Класс для работы с источником
class UsersOriginRepository:
    def __init__(self, pg_origin: PgConnect) -> None:
        self._db = pg_origin

    def list_users(self, user_threshold: int, limit: int) -> List[UserObj]:
        with self._db.client().cursor(row_factory=class_row(UserObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        order_user_id
                    FROM users
                    WHERE id > %(threshold)s
                    ORDER BY id
                    LIMIT %(limit)s;
                """, {
                    'threshold': user_threshold,
                    'limit': limit
                }
            )

            objs = cur.fetchall()

        return objs


# Класс для работы с DWH
class UsersDestRepository:

    def insert_user(self, conn: Connection, user: UserObj):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_users(id, order_user_id)
                    VALUES(%(id)s, %(order_user_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_user_id = EXCLUDED.order_user_id;
                """,
                {
                    'id': user.id,
                    'order_user_id': user.order_user_id
                },
            )



class UsersLoader:
    WF_KEY = 'users_origin_to_stg_workflow'
    LAST_LOADED_ID_KEY = 'last_loaded_id'
    BATCH_LIMIT = 1000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = UsersOriginRepository(pg_origin=pg_origin)
        self.stg = UsersDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_users(self) -> None:
        with self.pg_dest.connection() as conn:

            # Читаем состояние загрузки
            # Если настройки еще нет, заводим ее
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY, 'stg')
            if wf_setting is None:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_users(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info('Quitting')
                return
            for user in load_queue:
                self.stg.insert_user(conn, user)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, 'stg')

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
