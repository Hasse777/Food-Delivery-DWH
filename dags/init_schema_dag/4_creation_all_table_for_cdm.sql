-- cdm.dm_settlement_report определение
CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
	id INTEGER CONSTRAINT pk_dm_settlement_report PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	restaurant_id BIGINT NOT NULL,
	restaurant_name VARCHAR(50) NOT NULL,
	settlement_date DATE NOT NULL,
	orders_count INTEGER DEFAULT 0 NOT NULL,
	orders_total_sum NUMERIC(14, 2) DEFAULT 0 NOT NULL,
	orders_bonus_payment_sum NUMERIC(14, 2) DEFAULT 0 NOT NULL,
	orders_bonus_granted_sum NUMERIC(14, 2) DEFAULT 0 NOT NULL,
	order_processing_fee NUMERIC(14, 2) DEFAULT 0 NOT NULL,
	restaurant_reward_sum NUMERIC(14, 2) DEFAULT 0 NOT NULL,
	CONSTRAINT dm_settlement_report_settlement_date_check CHECK (settlement_date >= '2022-01-01'::DATE AND settlement_date < '2500-01-01'::DATE),
	CONSTRAINT order_processing_fee_not_negative CHECK (order_processing_fee >= 0),
	CONSTRAINT orders_bonus_granted_sum_not_negative CHECK (orders_bonus_granted_sum >= 0),
	CONSTRAINT orders_bonus_payment_sum_not_negative CHECK (orders_bonus_payment_sum >= 0),
	CONSTRAINT orders_count_not_negative CHECK (orders_count >= 0),
	CONSTRAINT orders_total_sum_not_negative CHECK (orders_total_sum >= 0),
	CONSTRAINT restaurant_reward_sum_not_negative CHECK (restaurant_reward_sum >= 0),
	CONSTRAINT unique_restaurant_id_settlement_date UNIQUE (restaurant_id, settlement_date)
);

-- cdm.dm_courier_ledger определение
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger
(
	id INTEGER CONSTRAINT pk_dm_courier_ledger PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	courier_id INT NOT NULL,
	courier_name VARCHAR(150) NOT NULL,
	settlement_year SMALLINT CHECK(settlement_year >= 2022) NOT NULL,
	settlement_month SMALLINT CHECK(settlement_month BETWEEN 1 AND 12) NOT NULL,
	orders_count INT CHECK(orders_count >= 0) NOT NULL,
	orders_total_sum NUMERIC(19, 2) CHECK(orders_total_sum >= 0) NOT NULL,
	rate_avg NUMERIC(3, 2) CHECK(rate_avg BETWEEN 0 AND 5),
	order_processing_fee NUMERIC(19, 2) CHECK(order_processing_fee >= 0) NOT NULL,
	courier_order_sum NUMERIC(19, 2) CHECK(courier_order_sum >= 0) NOT NULL,
	courier_tips_sum NUMERIC(19, 2) CHECK(courier_tips_sum >= 0) NOT NULL,
	courier_reward_sum NUMERIC(19, 2) CHECK(courier_reward_sum >= 0) NOT NULL,
	CONSTRAINT uk_dm_courier_ledger_courier_year_month UNIQUE(courier_id, settlement_year, settlement_month)
);