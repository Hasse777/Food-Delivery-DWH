-- Служебная таблица для инкрементальной загрузки
CREATE TABLE IF NOT EXISTS stg.srv_wf_settings (
    id INTEGER CONSTRAINT pk_srv_wf_settings PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key VARCHAR NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);

-- Таблица для сохранения ranks из источника postgrSQL
CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
    id INTEGER CONSTRAINT pk_bonussystem_ranks PRIMARY KEY,
    name VARCHAR NOT NULL,
    bonus_percent NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (bonus_percent >= 0),
    min_payment_threshold NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (min_payment_threshold >= 0)
);

-- Таблица для хранения пользователей из источника postgrSQL
CREATE TABLE IF NOT EXISTS stg.bonussystem_users(
	id INTEGER NOT NULL PRIMARY KEY,
	order_user_id VARCHAR NOT NULL
);


-- Таблица для хранения ивентов из источника postgrSQL
CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
    id INTEGER CONSTRAINT pk_bonussystem_events PRIMARY KEY,
    event_ts TIMESTAMP NOT NULL,
    event_type VARCHAR NOT NULL,
    event_value VARCHAR NOT NULL
);


-- Таблица для хранение пользователей из источника Mongo
CREATE TABLE IF NOT EXISTS stg.ordersystem_users
(
    id INTEGER CONSTRAINT ordersystem_users_pkey PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id VARCHAR NOT NULL CONSTRAINT ordersystem_users_object_id_key UNIQUE,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
);


-- Таблица для хранения ресторанов из источника Mongo
CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
    id INTEGER CONSTRAINT pk_ordersystem_restaurants PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id varchar NOT NULL UNIQUE,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL
);


-- Таблица для хранение заказов из источника Mongo
CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
	id INTEGER CONSTRAINT pk_ordersystem_orders PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	object_id VARCHAR(100) UNIQUE NOT NULL,
	object_value TEXT NOT NULL,
	update_ts TIMESTAMP NOT NULL
);


-- Таблицы из источника API по курьерам и доставкам
---------------------------------------------------------------------------------------------------------------------------------------------
-- По заданию просят сохранять именно ответ от источника то есть JSON.
-- Я решил в каждой таблице хранить JSON файл и вытащить ключевые поля.

CREATE TABLE IF NOT EXISTS stg.yandex_cloud_couriers(
	courier_id VARCHAR PRIMARY KEY,
	name VARCHAR NOT NULL,
	full_info_about_couriers JSON NOT NULL,
	load_ts TIMESTAMP DEFAULT(NOW())
);

-- Ответ API
--{'_id(VARCHAR)': 'zxy2s4q2gj8ngvpvnkgyln9', 'name(VARCHAR)': 'Олег Смирнов'}

CREATE TABLE IF NOT EXISTS stg.yandex_cloud_deliveries(
	delivery_id VARCHAR PRIMARY KEY,
	delivery_ts TIMESTAMP NOT NULL,
	courier_id VARCHAR NOT NULL,
	order_id VARCHAR NOT NULL,
	order_ts TIMESTAMP NOT NULL,
	tip_sum NUMERIC(19, 2) NOT NULL CHECK(tip_sum >= 0),
	rate NUMERIC(3, 2) CHECK(rate BETWEEN 0 AND 5) NOT NULL,
	full_info_about_deliveries JSON NOT NULL,
	load_ts TIMESTAMP DEFAULT NOW()
);

-- Ответ API
--{'order_id(VARCHAR)': '68863372e5a83b68a178ba82', 'order_ts(VARCHAR)': '2025-07-27 14:10:58.632000', 'delivery_id(VARCHAR)': 'lg77ng2wgte1zwqxua9au6c', 'courier_id(VARCHAR)': 't5jkgewkpaqn9s10y3vj4q9', 
--'address': 'Ул. Новая, 5, кв. 58', 'delivery_ts(VARCHAR)': '2025-07-27 14:40:05.594000', 'rate': 5(int), 'sum': 11698, 'tip_sum(int)': 1169}