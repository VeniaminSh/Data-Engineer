## Основная задача проекта:

Произвести миграцию данных из источника данных в новую модель данных. 

Техно стек: <u>Postgres & Dbever</u>


> <details>
> <summary><span style="font-size: 14pt;">(clik) Источником данных выступает таблица с следующим описанием</span> </summary>
> 
>Таблица shipping, которая представляет собой последовательность действий при доставке, перечисленную ниже.
>
> shippingid — уникальный идентификатор доставки.
>
> saleid — уникальный идентификатор заказа. К одному заказу может быть привязано несколько строчек 
>
> shippingid, то есть логов, с информацией о доставке.
>
> vendorid — уникальный идентификатор вендора. К одному вендору может быть привязано множество saleid и множество строк доставки.
>
> payment — сумма платежа (то есть дублирующаяся информация).
>
> shipping_plan_datetime — плановая дата доставки.
>
> status — статус доставки в таблице shipping по данному shippingid. Может принимать значения in_progress — доставка в процессе, либо finished — доставка завершена.
>
> state — промежуточные точки заказа, которые изменяются в соответствии с обновлением информации о доставке по времени state_datetime.
> 1. booked (пер. «заказано»);
> 2. fulfillment — заказ доставлен на склад отправки;
> 3. queued (пер. «в очереди») — заказ в очереди на запуск доставки;
> 4. transition (пер. «передача») — запущена доставка заказа;
> 5. pending (пер. «в ожидании») — заказ доставлен в пункт выдачи и ожидает получения;
> 6. received (пер. «получено») — покупатель забрал заказ;
> 7. returned (пер. «возвращено») — покупатель возвратил заказ после того, как его забрал.
> 8. state_datetime — время обновления состояния заказа.
> 9. shipping_transfer_description — строка со значениями transfer_type и transfer_model, записанными через :. 
> 
> Пример записи — 1p:car.
> 
> transfer_type — тип доставки. 1p означает, что компания берёт ответственность за доставку на себя, 3p — что за отправку ответственен вендор.
> 
> transfer_model — модель доставки, то есть способ, которым заказ доставляется до точки: car — машиной, train — поездом, ship — кораблем, airplane — самолетом, multiple — комбинированной доставкой.
> 
> shipping_transfer_rate — процент стоимости доставки для вендора в зависимости от типа и модели доставки, который взимается интернет-магазином для покрытия расходов.
> 
> shipping_country — страна доставки, учитывая описание тарифа для каждой страны.
> 
> shipping_country_base_rate — налог на доставку в страну, который является процентом от стоимости payment_amount.
> 
> vendor_agreement_description — строка, в которой содержатся данные agreementid, agreement_number, agreement_rate, agreement_commission, записанные через разделитель :. Пример записи — 12:vsp-34:0.02:0.023.
> 
> agreementid — идентификатор договора. 
> 
> agreement_number — номер договора в бухгалтерии. 
> 
> agreement_rate — ставка налога за стоимость доставки товара для вендора. 
> 
> agreement_commission — комиссия, то есть доля в платеже являющаяся доходом компании от сделки.
> 
> [Источник](https://disk.yandex.ru/i/i7rhUgjG486gRA "описание источника")

> </details>

<br>

Формат результата должен быть следующим:
[Модель данных](https://disk.yandex.ru/i/irJEW4Ea5dL2rA)


## Код миграции.
```
  ____________________________________________
 /* 1 Справочник стоимости доставки в страны*/
/___________________________________________/
DROP TABLE IF EXISTS shipping_country_rates cascade;
CREATE TABLE shipping_country_rates (
    id serial4 PRIMARY key,
    shipping_country Varchar(20),
    shipping_country_base_rate NUMERIC(14, 3) NULL);

-- Заполнение справочника
INSERT INTO shipping_country_rates(shipping_country, shipping_country_base_rate)
SELECT 
       shipping_country,
       shipping_country_base_rate
FROM shipping
GROUP BY 1,2;

--- select * from shipping_country_rates

  ____________________________________________________
 /*2 Cправочник тарифов доставки вендора по договору*/
/__________________________________________________*/
DROP TABLE IF EXISTS shipping_agreement cascade;
CREATE TABLE shipping_agreement  (
    agreementid int4 PRIMARY KEY,
    agreement_number Varchar(20),
    agreement_rate NUMERIC(14, 2) NULL,
    agreement_commission NUMERIC(14, 2));

-- Заполнение справочника    
INSERT INTO shipping_agreement (agreementid, agreement_number, agreement_rate, agreement_commission)
SELECT DISTINCT 
       (regexp_split_to_array(vendor_agreement_description, ':'))[1] :: int4,
       (regexp_split_to_array(vendor_agreement_description, ':'))[2] :: Varchar(20),
       (regexp_split_to_array(vendor_agreement_description, ':'))[3] :: NUMERIC(14, 2),
       (regexp_split_to_array(vendor_agreement_description, ':'))[4] :: NUMERIC(14, 2)
FROM shipping;
/*WHERE (regexp_split_to_array(shipping_transfer_description, ':'))[1] = '3p'*/;

-- select * from shipping_agreement

  _________________________________
 /* 3 Справочник о типах доставки*/
/_______________________________*/
DROP TABLE IF EXISTS shipping_transfer cascade;
CREATE TABLE shipping_transfer (
    id serial4 PRIMARY KEY,
    transfer_type Varchar(5),
    transfer_model Varchar(20),
    shipping_transfer_rate NUMERIC(14, 3) NULL);

-- Заполнение справочника    
INSERT INTO shipping_transfer(transfer_type, transfer_model, shipping_transfer_rate)
SELECT DISTINCT 
       (regexp_split_to_array(shipping_transfer_description, ':'))[1],
       (regexp_split_to_array(shipping_transfer_description, ':'))[2],
       shipping_transfer_rate
FROM shipping;
    
-- select * from shipping_transfer

  _________________________________________
 /*4 Создайте справочник о типах доставки*/
/________________________________________/
DROP TABLE IF EXISTS shipping_info cascade;
CREATE TABLE shipping_info (
    shippingid int8 PRIMARY KEY,
    vendorid int8,
    payment_amount numeric(14, 2),
    shipping_plan_datetime timestamp,
    transfer_id int8,
    shipping_country_id int8,    
    agreementid int8);
ALTER TABLE shipping_info ADD CONSTRAINT transfer_type_fkey FOREIGN KEY (transfer_id) REFERENCES shipping_transfer(id);
ALTER TABLE shipping_info ADD CONSTRAINT shipping_countr_fkey FOREIGN KEY (shipping_country_id) REFERENCES shipping_country_rates(id);
ALTER TABLE shipping_info ADD CONSTRAINT agreemen_fkey FOREIGN KEY (agreementid) REFERENCES shipping_agreement(agreementid);
   
-- Заполнение справочника    
INSERT INTO shipping_info (shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_id, shipping_country_id, agreementid)
SELECT s.shippingid,
       s.vendorid,
       sum(s.payment_amount),
       s.shipping_plan_datetime,
       st.id,
       scr.id,
       (regexp_split_to_array(vendor_agreement_description, ':'))[1] :: int8
FROM shipping AS s
           LEFT JOIN shipping_country_rates scr
           using(shipping_country)
           LEFT JOIN shipping_transfer st 
           ON(concat((regexp_split_to_array(s.shipping_transfer_description, ':'))[1],
              (regexp_split_to_array(s.shipping_transfer_description, ':'))[2],
              s.shipping_transfer_rate) :: TEXT = concat(st.transfer_type, st.transfer_model, st.shipping_transfer_rate):: TEXT)
GROUP BY 1, 2,4,5,6,7;

-- select * from shipping_info

  __________________________________________
 /* 5 Создайте таблицу статусов о доставке*/
/*_______________________________________*/
DROP TABLE IF EXISTS shipping_status CASCADE;
CREATE TABLE shipping_status (
    shippingid int8,
    status varchar(15),
    state varchar(15),
    shipping_start_fact_datetime timestamp null,
    shipping_end_fact_datetime timestamp NULL,
    shipping_end_plan_datetime timestamp NULL);

-- Заполнение справочника 
INSERT INTO shipping_status
WITH 
        time_end as(SELECT DISTINCT 
        shippingid,
        (CASE WHEN status = 'finished' AND state = 'recieved' THEN state_datetime END) AS shipping_end_fact_datetime,
        (CASE WHEN status = 'finished' AND state = 'recieved' THEN shipping_plan_datetime END ) AS shipping_end_plan_datetime
        FROM shipping s
        WHERE (CASE WHEN status = 'finished' AND state = 'recieved' THEN state_datetime END) IS NOT NULL),

        time_start as(SELECT DISTINCT 
        shippingid,
        (CASE WHEN status = 'in_progress' AND state = 'booked' THEN state_datetime END) AS shipping_start_fact_datetime
        FROM shipping s
        WHERE (CASE WHEN status = 'in_progress' AND state = 'booked' THEN state_datetime END) IS NOT NULL),

        state_status_1 as(SELECT 
        shippingid,
        status,
        state,
        state_datetime,
        ROW_NUMBER() over(PARTITION BY orderid ORDER BY state_datetime) AS wert_1
        FROM shipping s),  
        
        state_status_2 as(SELECT  
                  shippingid,
                  max(wert_1) AS wer_2
         FROM state_status_1
         GROUP BY shippingid),
        
        state_status AS(
              SELECT 
                  st2.shippingid AS shippingid,
                  status,
                  state
              FROM state_status_1 AS st1
                       right JOIN state_status_2 AS st2
                       ON (st1.shippingid = st2.shippingid and st1.wert_1 = st2.wer_2))

SELECT 
      shippingid,
      status,
      state,
      shipping_start_fact_datetime,
      shipping_end_fact_datetime,
      shipping_end_plan_datetime
FROM state_status s 
            LEFT JOIN time_start ts 
            using(shippingid)
            LEFT JOIN time_end te 
            using(shippingid);

-- select * from shipping_status;

  ______________________         
 /*6. Итоговая витрина*/
/*___________________*/
DROP TABLE IF EXISTS shipping_datamart CASCADE;
CREATE TABLE shipping_datamart (
shippingid int8,
vendorid int8,
transfer_type Varchar(5),
full_day_at_shippeng int8,
is_delay int4,
is_shipping_finish int4,
delay_day_at_shipping int8,
payment_amount numeric(14,2),
vat numeric(14,2),
profit numeric(14,2));

-- Наполнение витрины
INSERT INTO shipping_datamart
SELECT 
     si.shippingid,
     vendorid,
     st.transfer_type,
     date_part('day', age(shipping_end_fact_datetime, shipping_start_fact_datetime)) AS full_day_at_shipping,
     CASE WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime THEN 1 ELSE 0 END AS is_delay,
     CASE WHEN status = 'finished' THEN 1 ELSE 0 END as is_shipping_finish ,
     CASE WHEN ss.shipping_end_fact_datetime > ss.shipping_end_plan_datetime THEN date_part('day', ss.shipping_end_fact_datetime) - date_part('day',ss.shipping_end_plan_datetime) ELSE 0 END AS delay_day_at_shipping,
     us.payment_amount,
     us.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) AS vat,
     us.payment_amount * sa.agreement_commission AS profit 
     
FROM shipping_info si
                 LEFT JOIN shipping_transfer st 
                 ON (si.transfer_id = st.id)
                 LEFT JOIN shipping_status ss 
                 ON (si.shippingid = ss.shippingid)
                 LEFT JOIN (SELECT shippingid,
                                   sum(payment_amount) AS payment_amount 
                            FROM shipping
                            GROUP BY clientid ,shippingid) AS us
                            ON (si.shippingid = us.shippingid)
                 LEFT JOIN shipping_country_rates scr 
                 ON (scr.id = si.shipping_country_id)
                 LEFT JOIN shipping_agreement sa 
                 USING(agreementid);
```