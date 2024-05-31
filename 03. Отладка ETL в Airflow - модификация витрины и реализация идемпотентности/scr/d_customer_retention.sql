-- Миграция данных из витрины f_sales в customer_retention

-- на всякий случай удостоверимся что таблица есть
CREATE TABLE IF NOT EXISTS mart.customer_retention(
    new_customers_count int4 NULL,
    returning_customers_count int4 NULL,
    refunded_customer_count int4 NULL,
    period_name VARCHAR DEFAULT 'weekly',
    period_id VARCHAR NULL,
    item_id int4 NULL REFERENCES mart.d_item (item_id),
    new_customers_revenue numeric(10, 2) NULL,
    returning_customers_revenue numeric(10, 2) NULL,
    customers_refunded int4 NULL,
    day date,
   PRIMARY KEY (period_id, item_id, day));

-- если таблица есть то добавим поле дни для витрины customer_retention для дальнейшей агрегации тк это поле удаляется в конце скрипта
ALTER TABLE mart.customer_retention ADD COLUMN IF NOT EXISTS day date;

-- произведем миграцию данных согласно ТЗ
INSERT INTO mart.customer_retention (item_id, period_id, new_customers_count, returning_customers_count, refunded_customer_count, period_name, new_customers_revenue, returning_customers_revenue, customers_refunded, day)
WITH 
first_t AS (SELECT customer_id ,
            count(CASE WHEN flag = 'False' THEN 1 END) AS new_customers_count,
            count(CASE WHEN flag = 'False' THEN 1 END) AS returning_customers_count,
            count(CASE WHEN flag = 'True' THEN 1 END) AS refunded_customer_count,
            'weekly' AS period_name,
            left(week_of_year_iso, 8) AS week_of_year_iso,
            item_id,
            sum(CASE WHEN flag = 'False' THEN payment_amount end) AS new_customers_revenue,
            sum(CASE WHEN flag = 'False' THEN payment_amount end) AS returning_customers_revenue,
            sum(CASE WHEN flag = 'TRUE' THEN abs(quantity) END) AS customers_refunded,
            date_actual
FROM mart.f_sales s JOIN (SELECT week_of_year_iso, date_id, date_actual, first_day_of_week , last_day_of_week 
                          FROM mart.d_calendar
                          WHERE date_actual::date BETWEEN now()::date - 7 AND now()::date -1) AS q -- выберем данные только за 7 дней отсчет от нынешней даты 
                    ON (s.date_id = q.date_id)
GROUP BY customer_id, 6, 7, date_actual)

SELECT item_id,
       week_of_year_iso as period_id ,
       sum(CASE WHEN new_customers_count = 1 THEN new_customers_count END) AS new_customers_count,
       sum(CASE WHEN returning_customers_count > 1 THEN returning_customers_count END) AS returning_customers_count,
       sum(refunded_customer_count) AS refunded_customer_count,
       'weekly' AS period_name,
       sum(CASE WHEN new_customers_count = 1 THEN new_customers_revenue END) AS new_customers_revenue,
       sum(CASE WHEN new_customers_count > 1 THEN new_customers_revenue END) AS returning_customers_revenue,
       sum(customers_refunded) AS customers_refunded,
       date_actual as day
FROM first_t
GROUP BY item_id, 2, 10;

-- повторно обозначим данные только за 7 дней отсчет от нынешней даты 
DELETE FROM mart.customer_retention WHERE day :: date NOT BETWEEN now()::date - 7 AND now()::date -1;

-- создадим новую временную таблицу и перенесем в нее все данные из customer_retention так как в customer_retention нам необходимо положить агрегат
CREATE TEMPORARY TABLE temp_customer_retention AS TABLE mart.customer_retention;

-- очистим таблицу customer_retention, для насыщения ее агрегированными данными (уберем поле день) 
TRUNCATE TABLE mart.customer_retention;

-- для вставки агрегата , необходимо убрать поле день, так как агрегация была по item_id, period_id, period_name
ALTER TABLE mart.customer_retention DROP COLUMN IF EXISTS day;

-- вставляем данные
INSERT INTO mart.customer_retention (item_id, period_id, new_customers_count, returning_customers_count, refunded_customer_count, period_name, new_customers_revenue, returning_customers_revenue, customers_refunded)
SELECT item_id,
       period_id,
       sum(new_customers_count),
       sum(returning_customers_count),
       sum(refunded_customer_count),
       period_name,
       sum(new_customers_revenue),
       sum(returning_customers_revenue),
       sum(customers_refunded)
FROM temp_customer_retention
GROUP BY item_id, period_id, period_name;

-- на всякий случай удаляем врменную таблицу
DROP TABLE temp_customer_retention;