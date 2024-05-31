-- реализация идемпотентности: из таблицы f_sales будут удаляться данные, того дня, когда даг будет выполнять вставку
delete from mart.f_sales where date_id = (select date_id 
                                          from mart.d_calendar 
                                          where cast(date_actual as date) = '{{ds}}');

-- добавление поля флаг в таблицу, т.к. ранее его там не было
alter table mart.f_sales ADD COLUMN IF NOT EXISTS flag BOOLEAN;

-- добавление данных (дата - день выполнения дага)/т.к. у макетологов есть дашборд, который выводит график с покупками, 
-- считать строки с статусом refunded нет необходимости, поэтому выставим знак -
insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, flag)
select dc.date_id, item_id, customer_id, city_id, 
      quantity * (case when uol.status = 'refunded' then -1 else 1 end) quantity,
      payment_amount * (case when uol.status = 'refunded' then -1 else 1 end) payment_amount,
      (uol.status = 'refunded')

from staging.user_order_log uol 
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';