from logging import Logger
from typing import List

from lib import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DDS_products_obj(BaseModel):
    restaurant_id: str
    products_info: str
    update_ts: datetime
    referenc_on_restaran_id: int


class DDS_products_Loader:
    WF_KEY = 'stg_products_to_dm_products'
    LAST_LOADED_ID_KEY = 'last_loaded_ts'
    BATCH_LIMIT = 10000

    def __init__(self, pg_DWH: PgConnect, log: Logger):
        self.pg_DWH = pg_DWH
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    def load_products(self):
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
            load_queue = self.dwh_list_products(conn, last_loaded_ts, self.BATCH_LIMIT)
            self.log.info(f'starting products to load from last checkpoint: {last_loaded_ts}')

            if not load_queue:
                self.log.info('Quitting')
                return 0

            i = 0
            for d in load_queue:
                self.dwh_load_products(conn, d)

                i += 1

            self.log.info(f'loaded {i} products from {len(load_queue)}')
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.update_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json, 'dds')

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
                


    def dwh_list_products(self, conn: Connection, products_threshold: datetime, limit: int) -> List[DDS_products_obj]:
        with conn.cursor(row_factory=class_row(DDS_products_obj)) as cur:
            cur.execute(
                """
                    SELECT
                        dmr.restaurant_id AS restaurant_id,
                        object_value AS products_info,
                        update_ts,
                        dmr.id AS referenc_on_restaran_id
                    FROM stg.ordersystem_restaurants AS oo
                    INNER JOIN dds.dm_restaurants as dmr ON dmr.restaurant_id = oo.object_id AND oo.update_ts BETWEEN dmr.active_from AND dmr.active_to
                    WHERE update_ts > %(threshold)s
                    ORDER BY update_ts
                    LIMIT %(limit)s;
                """, {
                    'threshold': products_threshold,
                    'limit': limit
                }
            )
            objs = cur.fetchall()
        return objs


    def dwh_load_products(self, conn: Connection, product: DDS_products_obj):
        with conn.cursor() as cur:
            product_full_info = str2json(product.products_info)
            for item in product_full_info['menu']:
                cur.execute(
                    """
                        UPDATE dds.dm_products
                        SET active_to = %(end)s
                        WHERE product_id = %(product_id)s AND active_to = '2099-12-31 00:00:00' AND (product_name != %(product_name)s OR product_price != %(product_price)s)
                    """, {
                        'end': product.update_ts,
                        'product_id': item['_id'],
                        'product_name': item['name'],
                        'product_price': item['price']
                    }
                )

                cur.execute(
                """
                    INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    SELECT
                        %(restaurant_id)s::int,
                        %(product_id)s::varchar,
                        %(product_name)s::varchar,
                        %(product_price)s::numeric(14, 2),
                        %(active_from)s::timestamp,
                        '2099-12-31 00:00:00'::timestamp
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dds.dm_products
                        WHERE product_id = %(product_id)s
                        AND product_name = %(product_name)s
                        AND product_price = %(product_price)s
                        AND active_to = '2099-12-31 00:00:00'
                    );
                """, {
                    'restaurant_id': product.referenc_on_restaran_id,
                    'product_id': item['_id'],
                    'product_name': item['name'],
                    'product_price': item['price'],
                    'active_from': product.update_ts
                }
                )


# {"_id": "ef8c42c19b7518a9aebec106", "menu": [{"_id": "22744fcdb947be9aa795e797", "category": "Закуски", "name": "Бомбей Тикки", "price": 290}, {"_id": "1878439998067c1905f455b3", "category": "Закуски", "name": "Чиккен Тикка", "price": 590}, {"_id": "95f74625be0428e4b2380310", "category": "Закуски", "name": "Проун Пакора", "price": 790}, {"_id": "55084ea193269b89bc1686c4", "category": "Закуски", "name": "Машрум Тикка", "price": 390}, {"_id": "e86d440f87ba45c8280310dc", "category": "Закуски", "name": "Чили Проун", "price": 790}, {"_id": "927b4ba0b5494778ba576fd3", "category": "Закуски", "name": "Микс Ведж Пакора", "price": 450}, {"_id": "413c484db69df84af89cab35", "category": "Закуски", "name": "Чили Машрум", "price": 399}, {"_id": "6f30486386ee88e590de632b", "category": "Закуски", "name": "Панир Тикка", "price": 590}, {"_id": "1bda42a3819f598a052d2966", "category": "Закуски", "name": "Чили Чикен", "price": 499}, {"_id": "6bb54650b9fef09054c545ef", "category": "Закуски", "name": "Чили Панир", "price": 450}, {"_id": "1c6d4a019b936d6209c3fbe8", "category": "Закуски", "name": "Чикен Сиксти Файв", "price": 490}, {"_id": "dd234386b138dfd863c7c7c5", "category": "Закуски", "name": "Луковые Бхаджи", "price": 290}, {"_id": "e9214c6eab68e7625e6948c8", "category": "Закуски", "name": "Панир Пакора", "price": 449}, {"_id": "cf0e4699b6ab70ad3df5d162", "category": "Закуски", "name": "Мург Малай Тикка", "price": 550}, {"_id": "c0d44e02bbe3fa8bf7fcdf85", "category": "Супы", "name": "Чикен Шорба", "price": 380}, {"_id": "ca8f449eadf0832b800fa62b", "category": "Супы", "name": "Овощной суп", "price": 349}, {"_id": "b551423ca4daabd9e219ee47", "category": "Супы", "name": "Томатный суп", "price": 299}, {"_id": "353d4799b8cb9891c9f32aeb", "category": "Супы", "name": "Суп с креветками", "price": 599}, {"_id": "35dd4493974018de1ed0e036", "category": "Гарниры", "name": "Рис с шафраном", "price": 200}, {"_id": "54ff4abd913cfd8a0e5b75b0", "category": "Гарниры", "name": "Простой рис Басмати", "price": 120}, {"_id": "b8014b42b9e36df9befeffd3", "category": "Гарниры", "name": "Рис с кумином", "price": 175}, {"_id": "aeca4d08abab78869c20128c", "category": "Гарниры", "name": "Рис с лимоном", "price": 175}, {"_id": "32ed48df80469389087c3e53", "category": "Основные блюда", "name": "Палак Панир", "price": 499}, {"_id": "dadb4f10954de26867b687b0", "category": "Основные блюда", "name": "Машрум Маттар", "price": 450}, {"_id": "7bbd451a9ccb03e2f52badc9", "category": "Основные блюда", "name": "Фиш Карри", "price": 595}, {"_id": "62a9453ca09d774bcea1ad45", "category": "Основные блюда", "name": "Бриани с ягненком", "price": 650}, {"_id": "fa9e41cab58e5c525a74c326", "category": "Основные блюда", "name": "Муттон Роган Джош", "price": 749}, {"_id": "080d4248a4673012342b116a", "category": "Основные блюда", "name": "Муттон Карри", "price": 649}, {"_id": "800d4b1283e01fb243124225", "category": "Основные блюда", "name": "Бриани с овощами", "price": 499}, {"_id": "7b26444b845e2740c8669b46", "category": "Основные блюда", "name": "Чикке Карри", "price": 499}, {"_id": "3f7848ea94b146dedf030c2a", "category": "Основные блюда", "name": "Дал Макхни", "price": 499}, {"_id": "e2cf449a9a96e33881fe50a5", "category": "Основные блюда", "name": "Дал Тарка", "price": 390}, {"_id": "36d54f529ef3ac5e05210178", "category": "Основные блюда", "name": "Баттер Чикке", "price": 599}, {"_id": "aab045a1b82908e59d225e9a", "category": "Основные блюда", "name": "Бриани с курицей", "price": 550}, {"_id": "760244ac855a44c5287cb2ac", "category": "Основные блюда", "name": "Панир Баттер Масала", "price": 550}, {"_id": "523f456e836a4c64203ef81e", "category": "Основные блюда", "name": "Чиккен Тикка Масала", "price": 649}, {"_id": "9df848fe96dd0b097380d456", "category": "Основные блюда", "name": "Бриани с креветками", "price": 690}, {"_id": "48b94736be0c02a1b84da6b4", "category": "Основные блюда", "name": "Чанна Масала", "price": 390}, {"_id": "9228453584d3056edaca3e90", "category": "Выпечка", "name": "Чиз Наан", "price": 225}, {"_id": "99654af1bbb100533109759e", "category": "Выпечка", "name": "Чиз Гарлик Наан", "price": 250}, {"_id": "a2f74e5090c92413957b318b", "category": "Выпечка", "name": "Амритсари Кулча", "price": 150}, {"_id": "47b947298daea892c7ae6c5f", "category": "Выпечка", "name": "Баттер Наан", "price": 120}, {"_id": "0c92483792353f5e2773d75a", "category": "Выпечка", "name": "Гарлик Наан", "price": 150}, {"_id": "ba57488684df2dd926883ca7", "category": "Выпечка", "name": "Алу Кулча", "price": 150}, {"_id": "2160406ba7c026fd829ea738", "category": "Чай", "name": "Чай Масала", "price": 150}, {"_id": "2c0b49dd92d3e411f8f5fabd", "category": "Напитки", "name": "Сладкий Ласси", "price": 150}, {"_id": "7105441f860b1124a3a24fe9", "category": "Напитки", "name": "Банана Ласси", "price": 250}, {"_id": "aec44b7b9da0e68d7b850fea", "category": "Напитки", "name": "Джира Ласси", "price": 150}], "name": "Вкус Индии", "update_ts": "2025-07-23 12:30:44"}