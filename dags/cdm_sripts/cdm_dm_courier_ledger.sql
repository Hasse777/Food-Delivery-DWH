--Состав витрины:
--id — идентификатор записи.
--courier_id — ID курьера, которому перечисляем.
--courier_name — Ф. И. О. курьера.
--settlement_year — год отчёта.
--settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
--orders_count — количество заказов за период (месяц).
--orders_total_sum — общая стоимость заказов.
--rate_avg — средний рейтинг курьера по оценкам пользователей.
--order_processing_fee — сумма, удержанная компанией за обработ2ку заказов, которая высчитывается как orders_total_sum * 0.25.
--courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
--courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
--courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).
--Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в расчётном месяце:
--r < 4 — 5% от заказа, но не менее 100 р.;
--4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
--4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
--4.9 <= r — 10% от заказа, но не менее 200 р.

-- Скрипт для расчёта витрины курьеров.
INSERT INTO cdm.dm_courier_ledger(
	courier_id,
	courier_name,
	settlement_year,
	settlement_month,
	orders_count,
	orders_total_sum,
	rate_avg,
	order_processing_fee,
	courier_order_sum,
	courier_tips_sum,
	courier_reward_sum
)
WITH total_sum_table AS(
	SELECT
		fps.order_id AS order_id,
		SUM(fps.total_sum) AS total_sum,
		-- Чтобы избежать подзапросы решил сразу рассчитывать
		-- все возможные варианты выплат курьеру.
		-- Так как у каждой выплаты есть минимальный порог к одному заказу,
		-- я использовал функцию GREATEST.
		GREATEST(SUM(fps.total_sum) * 0.05, 100) AS five_percent,
		GREATEST(SUM(fps.total_sum) * 0.07, 150) AS seven_percent,
		GREATEST(SUM(fps.total_sum) * 0.08, 175) AS eight_percent,
		GREATEST(SUM(fps.total_sum) * 0.1, 200) AS ten_percent
	FROM dds.fct_product_sales AS fps
	GROUP BY fps.order_id
),
part_two AS (
	SELECT 
		dc.id AS courier_id,
		dc.courier_name AS courier_name,
		dt."year" AS settlement_year,
		dt."month" AS settlement_month,
		COUNT(*) AS orders_count,
		SUM(tst.total_sum) AS orders_total_sum,
		AVG(dd.rate) AS rate_avg,
		SUM(tst.total_sum) * 0.25 AS order_processing_fee,
		CASE
			WHEN AVG(dd.rate) < 4 THEN SUM(five_percent)
			WHEN AVG(dd.rate) >= 4 AND AVG(dd.rate) < 4.5 THEN SUM(seven_percent)
			WHEN AVG(dd.rate) >= 4.5 AND AVG(dd.rate) < 4.9 THEN SUM(eight_percent)
			WHEN AVG(dd.rate) >= 4.9 THEN SUM(ten_percent)
		END AS courier_order_sum,
		SUM(tip_sum) AS courier_tips_sum
	FROM dds.dm_orders AS o
	INNER JOIN dds.dm_deliveries AS dd ON o.delivery_id = dd.id
	INNER JOIN dds.dm_couriers AS dc ON dd.courier_id = dc.id 
	INNER JOIN dds.dm_timestamps AS dt ON o.timestamp_id = dt.id 
	INNER JOIN total_sum_table AS tst ON tst.order_id = o.id
	WHERE o.order_status = 'CLOSED'
	GROUP BY dc.id, courier_name, settlement_year, settlement_month
)
SELECT 
	*,
	courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
FROM part_two ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
SET
	courier_name = EXCLUDED.courier_name,
	orders_count = EXCLUDED.orders_count,
	orders_total_sum = EXCLUDED.orders_total_sum,
	rate_avg = EXCLUDED.rate_avg,
	order_processing_fee = EXCLUDED.order_processing_fee,
	courier_order_sum = EXCLUDED.courier_order_sum,
	courier_tips_sum = EXCLUDED.courier_tips_sum,
	courier_reward_sum = EXCLUDED.courier_reward_sum;
