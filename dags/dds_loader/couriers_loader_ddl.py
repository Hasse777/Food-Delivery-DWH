from logging import Logger
from typing import List

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from lib import PgConnect


class DDS_couriers_obj(BaseModel):
    courier_id: str
    courier_name: str


class DDS_couriers_Loader:
    BATCH_LIMIT = 1000

    def __init__(self, pg_DWH: PgConnect, log: Logger):
        self.pg_DWH = pg_DWH
        self.log = log

    def load_couriers(self):
        with self.pg_DWH.connection() as conn:
            # Вычитываем очередную пачку объектов.
            load_queue = self.dwh_list_couriers(conn, self.BATCH_LIMIT)
            self.log.info('starting to load couriers')

            if not load_queue:
                self.log.info('Quitting')
                return 0

            i = 0
            for d in load_queue:
                self.dwh_load_couriers(conn, d)

                i += 1

            self.log.info(f'loaded {i} couriers from {len(load_queue)}')
            return len(load_queue)

    def dwh_list_couriers(self, conn: Connection, limit: int) -> List[DDS_couriers_obj]:
        with conn.cursor(row_factory=class_row(DDS_couriers_obj)) as cur:
            cur.execute(
                """
                    SELECT
                        courier_id,
                        name AS courier_name
                    FROM stg.yandex_cloud_couriers
                    LIMIT %(limit)s;
                """, {
                    'limit': limit
                }
            )
            objs = cur.fetchall()
        return objs

    def dwh_load_couriers(self, conn: Connection, courier: DDS_couriers_obj):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES(%(courier_id)s, %(courier_name)s)
                    ON CONFLICT(courier_id) DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name;
                """, {
                    'courier_id': courier.courier_id,
                    'courier_name': courier.courier_name,
                }
            )
