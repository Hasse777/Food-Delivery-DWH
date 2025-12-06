from logging import Logger
from typing import List

from lib import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DDS_user_obj(BaseModel):
    id: int
    user_id: str
    user_info: str


class DDS_users_Loader:
    WF_KEY = 'stg_users_to_DDS_dm_users'
    LAST_LOADED_ID_KEY = 'last_loaded_id'
    BATCH_LIMIT = 1000

    def __init__(self, pg_DWH: PgConnect, log: Logger):
        self.pg_DWH = pg_DWH
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_users(self):
        """
        Созздаем транзакцию через conn в БД и инкрементально загружаем
        пользователей, которые еще не присутствуют в dds.
        """
        with self.pg_DWH.connection() as conn:
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY, 'dds')
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.dwh_list_users(conn, last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for user in load_queue:
                self.dwh_load_users(conn, user)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, 'dds')

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

    def dwh_list_users(self, conn: Connection, user_threshold: int, limit: int) -> List[DDS_user_obj]:
        """
        Возвращает список пользователей, которых еще нет в витрине DDS

        Извлекает пользователей из STG слоя, у которых ID больше порогового значения.
        Порогового значения, который указан в таблице инкрементальных загрузок.
        """
        with conn.cursor(row_factory=class_row(DDS_user_obj)) as cur:
            cur.execute(
                # На самом деле здесь я немного в замешательстве.
                # По моей логике я достаю пользователей,
                # которые существуют в двух источниках.
                # Но что-то мне подсказывает, что добавлять в DDS нужно абсолютно всех.
                # Если это критично, исправлю.
                """
                    SELECT
	                    ou.id,
	                    ou.object_id as user_id,
	                    ou.object_value as user_info
                    FROM stg.bonussystem_users AS bu
                    JOIN stg.ordersystem_users AS ou ON bu.order_user_id = ou.object_id
                    WHERE ou.id > %(threshold)s
                    ORDER BY ou.id
                    LIMIT %(limit)s;
                """, {
                    'threshold': user_threshold,
                    'limit': limit
                }
            )
            objs = cur.fetchall()
        return objs

    def dwh_load_users(self, conn: Connection, user: DDS_user_obj):
        """
        Загружает полученный список пользователей в BD.

        Если пользователь с таким user_id уже сузествует, данные обновляются.
        """
        with conn.cursor() as cur:
            user_value = str2json(user.user_info)
            cur.execute(
                """
                    INSERT INTO dds.dm_users(user_id, user_name, user_login)
                    VALUES(%(user_id)s, %(user_name)s, %(user_login)s)
                    ON CONFLICT(user_id) DO UPDATE
                    SET
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login;
                """, {
                    'user_id': user.user_id,
                    'user_name': user_value['name'],
                    'user_login': user_value['login']
                }
            )
