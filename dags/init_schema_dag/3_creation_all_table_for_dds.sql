-- dds.srv_wf_settings определение
CREATE TABLE IF NOT EXISTS dds.srv_wf_settings (
	id INTEGER CONSTRAINT pk_srv_wf_settings PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);


-- Создаем таблицу измерения для курьеров
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
	id INTEGER CONSTRAINT pk_dm_couriers PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	courier_id VARCHAR NOT NULL,
	courier_name VARCHAR(150) NOT NULL,
	CONSTRAINT uk_dm_couriers_id UNIQUE (courier_id)
);


-- dds.dm_deliveries определение
CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
	id BIGINT CONSTRAINT pk_dm_deliveries PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	delivery_id VARCHAR NOT NULL,
	order_key VARCHAR NOT NULL, -- Изначально я не хотел добавлять это поле в таблицу.
	-- Позже, при соединении заказов с доставками мне стало очень больно.
	courier_id INTEGER NOT NULL,
	rate NUMERIC(3, 2) NOT NULL,
	tip_sum NUMERIC(19, 2) NOT NULL,
	CONSTRAINT dm_deliveries_rate_check CHECK (((rate >= (0)::NUMERIC) AND (rate <= (5)::NUMERIC))),
	CONSTRAINT dm_deliveries_tip_sum_check CHECK ((tip_sum >= (0)::NUMERIC)),
	CONSTRAINT uk_dm_deliveries_delivery_id UNIQUE (delivery_id),
	CONSTRAINT dm_deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id) ON DELETE RESTRICT
);


-- dds.dm_timestamps определение
CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
	id BIGINT CONSTRAINT pk_dm_timestamps PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	ts TIMESTAMP NOT NULL,
	year SMALLINT NOT NULL,
	month SMALLINT NOT NULL,
	day SMALLINT NOT NULL,
	time TIME NOT NULL,
	date DATE NOT NULL,
	CONSTRAINT dm_timestamps_day_check CHECK (day BETWEEN 1 AND 31),
	CONSTRAINT dm_timestamps_month_check CHECK (month BETWEEN 1 AND 12),
	CONSTRAINT dm_timestamps_year_check CHECK (year >= 2022 AND year < 2500),
	CONSTRAINT uk_dm_timestamps_ts UNIQUE (ts)
);


-- dds.dm_users определение
CREATE TABLE IF NOT EXISTS dds.dm_users (
	id BIGINT CONSTRAINT pk_dm_users PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	user_id VARCHAR NOT NULL,
	user_name VARCHAR(150) NOT NULL,
	user_login VARCHAR(150) NOT NULL,
	CONSTRAINT uk_dm_users_user_id UNIQUE(user_id)
);



--Буря мглою небо кроет, Вихри сне
-- dds.dm_restaurants определение
CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
	id BIGINT CONSTRAINT pk_dm_restaurants PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	restaurant_id VARCHAR NOT NULL,
	restaurant_name VARCHAR(150) NOT NULL,
	active_from TIMESTAMP NOT NULL,
	active_to TIMESTAMP NOT NULL,
	CONSTRAINT uk_dm_restaurants_restaurant_id_from UNIQUE(restaurant_id, active_from),
	CONSTRAINT ck_dm_restaurants_dates CHECK(active_from <= active_to)
);


-- жные крутя; То, как зверь, она завоет...
-- dds.dm_orders определение
CREATE TABLE IF NOT EXISTS dds.dm_orders (
	id BIGINT CONSTRAINT pk_dm_orders PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	order_key VARCHAR NOT NULL,
	user_id INTEGER NOT NULL,
	restaurant_id INTEGER NOT NULL,
	timestamp_id BIGINT NOT NULL,
	order_status VARCHAR(40) NOT NULL,
	delivery_id BIGINT DEFAULT NULL,
	CONSTRAINT uk_dm_orders_order_key UNIQUE (order_key),
	CONSTRAINT dm_orders_rest_id_fk FOREIGN KEY(restaurant_id) REFERENCES dds.dm_restaurants(id) ON DELETE RESTRICT,
	CONSTRAINT dm_orders_timest_id_fk FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id) ON DELETE RESTRICT,
	CONSTRAINT dm_orders_user_id_fk FOREIGN KEY (user_id) REFERENCES dds.dm_users(id) ON DELETE RESTRICT,
	CONSTRAINT fk_dm_orders_deliveries FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries(id) ON DELETE RESTRICT
);


-- dds.dm_products определение
CREATE TABLE IF NOT EXISTS dds.dm_products (
	id BIGINT CONSTRAINT pk_dm_products PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	restaurant_id INTEGER NOT NULL,
	product_id VARCHAR NOT NULL,
	product_name VARCHAR(100) NOT NULL,
	product_price NUMERIC(14, 2) DEFAULT 0 NOT NULL,
	active_from TIMESTAMP NOT NULL,
	active_to TIMESTAMP NOT NULL,
	CONSTRAINT dm_products_product_price_check CHECK (product_price >= 0),
	CONSTRAINT dm_dm_products_from_to CHECK(active_from <= active_to),
	CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id) ON DELETE RESTRICT,
	CONSTRAINT uk_dm_products_product_id_from UNIQUE(product_id, active_from)
);



-- ТАБЛИЦА ФАКТОВ
-- dds.fct_product_sales определение
CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
	id BIGINT CONSTRAINT pk_fct_product_sales PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	order_id BIGINT NOT NULL,
	product_id BIGINT NOT NULL,
	count INTEGER DEFAULT 0 NOT NULL,
	price NUMERIC(14, 2) DEFAULT 0 NOT NULL,
	total_sum NUMERIC(14, 2) DEFAULT 0 NOT NULL,
	bonus_payment NUMERIC(14, 2) DEFAULT 0 NOT NULL,
	bonus_grant NUMERIC(14, 2) DEFAULT 0 NOT NULL,
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK (bonus_grant >= 0),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK (bonus_payment >= 0),
	CONSTRAINT fct_product_sales_count_check CHECK (count >= 0),
	CONSTRAINT fct_product_sales_price_check CHECK (price >= 0),
	CONSTRAINT fct_product_sales_total_sum_check CHECK (total_sum >= 0),
	CONSTRAINT uk_fct_product_sales_order_id_product_id UNIQUE (order_id, product_id),
	CONSTRAINT fct_product_sales_order_id_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id) ON DELETE RESTRICT,
	CONSTRAINT fct_product_sales_product_id_fk FOREIGN KEY (product_id) REFERENCES dds.dm_products(id) ON DELETE RESTRICT
);

