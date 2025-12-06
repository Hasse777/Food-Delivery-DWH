from logging import Logger
from typing import List

from lib import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DDS_events_obj_get(BaseModel):
    id: int
    event_type: str
    event_value: str


class DDS_events_obj_insert(BaseModel):
    order_id: str
    product_id: str
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class DDS_events_Loader:
    WF_KEY = 'stg_events_to_fct_product_sales'
    LAST_LOADED_ID_KEY = 'last_loaded_id'
    BATCH_LIMIT = 30000

    def __init__(self, pg_DWH: PgConnect, log: Logger):
        self.pg_DWH = pg_DWH
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_events(self):
        with self.pg_DWH.connection() as conn:
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY, 'dds')
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.dwh_list_events(conn, last_loaded, self.BATCH_LIMIT)
            load_queue_insert = self.dwh_transformation_date(load_queue)

            self.log.info(f'starting events to load from last checkpoint: {last_loaded}')

            if not load_queue_insert:
                self.log.info('Quitting')
                return 0

            i = 0
            for d in load_queue_insert:
                self.dwh_load_events(conn, d)

                i += 1

            self.log.info(f'loaded {i} events from {len(load_queue)}')
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, 'dds')

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)

    def dwh_list_events(self, conn: Connection, events_threshold: int, limit: int) -> List[DDS_events_obj_get]:
        with conn.cursor(row_factory=class_row(DDS_events_obj_get)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        event_type,
                        event_value
                    FROM stg.bonussystem_events
                    WHERE id > %(threshold)s AND event_type = 'bonus_transaction'
                    ORDER BY id
                    LIMIT %(limit)s;
                """, {
                    'threshold': events_threshold,
                    'limit': limit
                }
            )
            obj = cur.fetchall()
        return obj

    def dwh_transformation_date(self, date: List[DDS_events_obj_get]) -> List[DDS_events_obj_insert]:
        """
        Выполняет трансформацию строк событий, содержащих JSON в текстовом формате,
        в структуру объектов DDS_events_obj_insert.

        Каждое событие с типом 'bonus_transaction' содержит поле event_value,
        которое является строкой с JSON структурой. В этой структуре нужные нам поля.
        """
        obj = list()
        for event in date:
            full_info = str2json(event.event_value)

            for product in full_info['product_payments']:
                obj.append(
                    DDS_events_obj_insert(
                        order_id=full_info['order_id'],
                        product_id=product['product_id'],
                        count=product['quantity'],
                        price=product['price'],
                        total_sum=product['product_cost'],
                        bonus_payment=product['bonus_payment'],
                        bonus_grant=product['bonus_grant']
                    )
                )
        return obj

    def dwh_load_events(self, conn: Connection, event: DDS_events_obj_insert):
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.fct_product_sales(
                    order_id,
                    product_id,
                    count,
                    price,
                    total_sum,
                    bonus_payment,
                    bonus_grant
                )
                SELECT
                    o.id::BIGINT AS order_id,
                    dp.id::BIGINT AS product_id,
                    %(count)s::INT AS count,
                    %(price)s::NUMERIC(14, 2) AS price,
                    %(total_sum)s::NUMERIC(14, 2) AS total_sum,
                    %(bonus_payment)s::NUMERIC(14, 2) AS bonus_payment,
                    %(bonus_grant)s::NUMERIC(14, 2) AS bonus_grant
                FROM dds.dm_products AS dp
                INNER JOIN dds.dm_orders AS o ON o.order_key = %(order_key)s
                WHERE dp.product_id = %(product_id)s AND active_to = '2099-12-31 00:00:00.000'
                ON CONFLICT(order_id, product_id) DO UPDATE
                SET
                    count = EXCLUDED.count,
                    price = EXCLUDED.price,
                    total_sum = EXCLUDED.total_sum,
                    bonus_payment = EXCLUDED.bonus_payment,
                    bonus_grant = EXCLUDED.bonus_grant;
                """, {
                    'order_key': event.order_id,
                    'product_id': event.product_id,
                    'count': event.count,
                    'price': event.price,
                    'total_sum': event.total_sum,
                    'bonus_payment': event.bonus_payment,
                    'bonus_grant': event.bonus_grant
                }
            )
