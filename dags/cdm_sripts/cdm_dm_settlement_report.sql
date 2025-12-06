INSERT INTO cdm.dm_settlement_report (
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
)
WITH daily_metrics AS (
    SELECT
        dr.id AS restaurant_id,
        dr.restaurant_name AS restaurant_name,
        dt."date" AS settlement_date,
        COUNT(DISTINCT do2.id) AS orders_count,
        SUM(fps.total_sum) AS orders_total_sum,
        SUM(fps.bonus_payment) AS orders_bonus_payment_sum,
        SUM(fps.bonus_grant) AS orders_bonus_granted_sum,
        SUM(fps.total_sum) * 0.25 AS order_processing_fee
    FROM dds.fct_product_sales AS fps
    JOIN dds.dm_orders do2 ON fps.order_id = do2.id
    JOIN dds.dm_restaurants dr ON do2.restaurant_id = dr.id
    JOIN dds.dm_timestamps dt ON do2.timestamp_id = dt.id
    WHERE do2.order_status = 'CLOSED'
    GROUP BY dr.id, dr.restaurant_name, dt."date"
)
SELECT
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    orders_total_sum - orders_bonus_payment_sum - order_processing_fee AS restaurant_reward_sum
FROM daily_metrics
ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
SET
    restaurant_name = EXCLUDED.restaurant_name,
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
