# Data Vault: Мой опыт создания модели вручную.

Как я делал Data Vault руками ... или custom migrate a Data Vault c нотками Data Vault 2.0. Достаточно интересный способ провести время, но для начала углубимся в краткий экскурс: 

- Data Vault Modeling - (преобразование обычной модели, в бизнес-ориентированную модель для хранения данных на основе бизнес ключей)

- Органика Data Vault:
    - Hub - отдельная таблица, описывающая уникальный бизнесовый ключ. В моем случае состоящая из: 
        1. Хеш - бизнес ключ в формате md5
        2. Источник, откуда берется выгрузка, можно вывести код источника, но я взял саму таблицу =) 
        3. Время загрузки данных в таблицу Hub,
        4. Бизнес ключ.

    - Satellite - описательная информация хаба
        1. Хеш хаба,
        2. Время загрузки данных в таблицу Satellite,
        3. Дата действия записи в Satellite (SCD2),
        4. Источник, откуда берется выгрузка,
        5. Сам атрибут саттелита.

    - Link - отношение между единицами бизнеса (ключами хабов)
        1. Хеш линка (для комплексного просмотра изменений при SCD2 - состоит из скрещения хешей хаба),
        2. Время загрузки данных в таблицу Link,
        3. Дата действия записи в Link (SCD2)(ПРИ НАЛИЧИИ),
        4. Источник, откуда берется выгрузка,
        5. Хеш первого хаба
        6. Хеш второго хаба
        7. ... третьего при наличии и тд.

В качестве примера была использована демонстрационная база данных, которую можно выгрузить [в открытом доступе](https://edu.postgrespro.ru/demo-small.zip) или глянуть здесь https://postgrespro.ru/education/demodb .

Перед формированием DataVault, необходимо возвести наглядную модель, в моем случае я использовал [drawio от гугle](https://drive.google.com/file/d/1VcN5-KVAizzSl9FWSyAXQiV4Z_Ublo8e/view?usp=sharing). Это не займет много времени, но поможет при формировании хранилища.
<br>


<br/>

## Итак, поехали!

Открываем скрипт в консоли psql под нужным пользователем , у меня по дефолту.

```psql -f demo_small_YYYYMMDD.sql -U postgres```

>Тем самым получим базу данных demo с данными об авиаперевозках по России. Объем данных небольшой, как раз подойдет для примера.

Для начала нам необходимо настроить репликацию (надо это вам или нет , решайте сами, но я сделал). Во избежании проблем связанных с доступами , рапределениями нагрузок и целостности данных.


### Шаг 1. Репликация

Для репликации данных в другую схему создадим новое расширение postgres_fdw, которое позволяет постгре держать связь с внешними данными из других серверов.


``` 
/* -- Создаем расширение*/
DROP EXTENSION IF EXISTS postgres_fdw CASCADE;
CREATE EXTENSION postgres_fdw;


/*-- Создание сервера, который использует расширение в качестве обертки сторонних данных*/
CREATE SERVER IF NOT EXISTS foreign_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', port '5432', dbname 'demo');

CREATE USER MAPPING IF NOT EXISTS FOR postgres
SERVER foreign_server
OPTIONS (user 'postgres', password 'postgres');


/*-- Создадим схему foreign_tables, в которую будем лить реплику*/
DROP SCHEMA IF EXISTS foreign_tables;
CREATE SCHEMA IF NOT EXISTS foreign_tables;

IMPORT FOREIGN SCHEMA bookings  -- название схемы исходника
FROM SERVER foreign_server
INTO foreign_tables;
```

Перед тем как создать сервер, сделал новую бд, которую назвал DWHDataVault. 

Вышеперечисленный код, был исполнен в новой бд. Итог 1 шага , мы получили реплицированные данные из исходника , но есть костыль ... куда без них )0))). 

> <details>
> <summary><span style="font-size: 13pt;"> Костыль</span> </summary>
>
>
> Реплика переносит данные в виде внешних таблиц, к которым нельзя подвязать ключи. 
>
> Поэтому переливаем данные из слоя реплицирования foreign_tables в табличный, т.к. у внешних таблиц нет полного функционала табличной части...
>
> ```/*сперва создадим слой Stage для хранения функциональных таблиц*/ DROP SCHEMA IF EXISTS Stage CASCADE; CREATE SCHEMA IF NOT EXISTS Stage;```
>
>
> ```/*aircrafts*/ DROP TABLE IF EXISTS Stage.aircrafts CASCADE; CREATE TABLE Stage.aircrafts AS TABLE foreign_tables.aircrafts;``` 
>
>```/*aircrafts_data*/ DROP TABLE IF EXISTS Stage.aircrafts_data CASCADE; CREATE TABLE Stage.aircrafts_data AS TABLE foreign_tables.aircrafts_data;``` 
>
>```/*boarding_passes*/ DROP TABLE IF EXISTS Stage.boarding_passes CASCADE; CREATE TABLE Stage.boarding_passes AS TABLE foreign_tables.boarding_passes;``` 
>
>```/*airports*/ DROP TABLE IF EXISTS Stage.airports CASCADE; CREATE TABLE Stage.Airports AS TABLE foreign_tables.airports;``` 
>
> Что касается таблицы airports_data, в исходнике есть поле coordinates, в котором по дефолту тип данных point, для дальнейшего преобразования таблицы в органику DataVault необходимо переопределить тип 
> 
>```/*airports_data*/ DROP TABLE IF EXISTS Stage.airports_data CASCADE; CREATE TABLE Stage.airports_data AS TABLE foreign_tables.airports_data; ALTER TABLE Stage.airports_data ALTER COLUMN coordinates TYPE VARCHAR;``` 
>
>```/*bookings*/ DROP TABLE IF EXISTS Stage.bookings CASCADE; CREATE TABLE Stage.bookings AS TABLE foreign_tables.bookings;``` 
>
>```/*flights*/ DROP TABLE IF EXISTS Stage.flights CASCADE; CREATE TABLE Stage.flights AS TABLE foreign_tables.flights;``` 
>
>```/*seats*/ DROP TABLE IF EXISTS Stage.seats CASCADE;CREATE TABLE Stage.seats AS TABLE foreign_tables.seats;``` 
>
>```/*ticket_flights*/ DROP TABLE IF EXISTS Stage.ticket_flights CASCADE; CREATE TABLE Stage.ticket_flights AS TABLE foreign_tables.ticket_flights;``` 
>
>```/*tickets*/ DROP TABLE IF EXISTS Stage.tickets CASCADE; CREATE TABLE Stage.tickets AS TABLE foreign_tables.tickets;``` 
> </details>



### Шаг 2. Подготовка DDL для органики DataVault 

Для начала необходимо определить схему, для миграция данных

```
DROP SCHEMA IF EXISTS data_vault CASCADE;
CREATE SCHEMA IF NOT EXISTS data_vault;
```
---
#### DDL для таблиц Hub 

первичным ключом в таблицах будет захешированный бизнес ключ.

Для начала удалим таблицы, если такие имеются.
```
DROP TABLE IF EXISTS data_vault.Hub_flights CASCADE;
DROP TABLE IF EXISTS data_vault.Hub_airports_data CASCADE;
DROP TABLE IF EXISTS data_vault.Hub_aircrafts_data CASCADE;
DROP TABLE IF EXISTS data_vault.Hub_seats CASCADE;
DROP TABLE IF EXISTS data_vault.Hub_ticket_flights CASCADE;
DROP TABLE IF EXISTS data_vault.Hub_boarding_passes CASCADE;
DROP TABLE IF EXISTS data_vault.Hub_tickets CASCADE;
DROP TABLE IF EXISTS data_vault.Hub_bookings CASCADE;
```

Напишем функцию для формирования таблиц Hub. Функция принимает на вход два параметра: 
1. Название таблицы саттелита 
2. Бизнес ключ

```
CREATE OR REPLACE FUNCTION data_vault.ddl_hub_table(      
               table_name TEXT , -- название таблицы саттелита
               business_key TEXT) -- какие колонки надо вытащить из таблицы источника указывается вместе с форматом
               RETURNS VOID AS $$
DECLARE 
     qwery TEXT;
BEGIN -- проверка на наличие данных в параметрах функции
    IF 
	  (table_name IS NULL) OR (business_key IS NULL)
		THEN RAISE EXCEPTION 'Не заполнены параметры';
	END IF;
qwery:= 'CREATE TABLE IF NOT EXISTS data_vault.' || table_name || '(' || 
        'Hash_key Varchar(33) PRIMARY KEY,
         record_sourse Varchar(20) NOT NULL,
         Load_date timestamp NOT NULL ,' ||
         business_key || ' NOT NULL);';
    EXECUTE qwery;
END;
$$ LANGUAGE plpgsql;
```

Запрос без функции выглядит так.

```
CREATE TABLE IF NOT EXISTS data_vault.Hub_flights(
Hash_key Varchar(33) PRIMARY KEY,
record_sourse Varchar(20) NOT NULL,
Load_date timestamp NOT NULL,
flight_id bigint NOT NULL);
```

DDL для Hub
```
-- flights
SELECT data_vault.ddl_hub_table('Hub_flights', 'flight_id bigint')

-- airports_data
SELECT data_vault.ddl_hub_table('Hub_airports_data', 'airport_code bpchar(3)')

-- aircrafts_data
SELECT data_vault.ddl_hub_table('Hub_aircrafts_data', 'aircraft_code bpchar(3)')

-- seats
SELECT data_vault.ddl_hub_table('Hub_seats', 'aircraft_code_seat_no Varchar(20)')

-- ticket_flights
SELECT data_vault.ddl_hub_table('Hub_ticket_flights', 'ticket_no_flight_id Varchar(25)')

-- boarding_passes
SELECT data_vault.ddl_hub_table('Hub_boarding_passes', 'ticket_no_flight_id Varchar(25)')

-- tickets
SELECT data_vault.ddl_hub_table('Hub_tickets', 'ticket_no Varchar(20)')

-- bookings
SELECT data_vault.ddl_hub_table('Hub_bookings', 'book_ref Varchar(15)')
```
---
#### DDL для таблиц Link 
DDL для Link прописан вручную, так как структура у таблиц может быть индивидуальной.

В некоторых таблицах, обработка данных ведется без хеширования, поэтому привязать Link к Satellite на этапе DDL не представляется возможным, но REFERENCES осуществляется на этапе обработки.

Для начала удалим таблицы, если такие имеются.
```
DROP TABLE IF EXISTS data_vault.Link_flights_airoport_data_departure CASCADE;
DROP TABLE IF EXISTS data_vault.Link_flights_airoport_data_arrival CASCADE;
DROP TABLE IF EXISTS data_vault.Link_flights_ticket_flights CASCADE;
DROP TABLE IF EXISTS data_vault.Link_ticket_flights_boarding_passes CASCADE;
DROP TABLE IF EXISTS data_vault.Link_ticket_flights_tickets CASCADE;
DROP TABLE IF EXISTS data_vault.Link_tickets_bookings CASCADE;
DROP TABLE IF EXISTS data_vault.Link_flights_aircrafts_data CASCADE;
DROP TABLE IF EXISTS data_vault.Link_seats_aircrafts_data CASCADE;
```


```
-- flights_airoport_data_departure
CREATE TABLE IF NOT EXISTS data_vault.Link_flights_airoport_data_departure(
Link_flights_airoport_data_departure_Hashkey Varchar(33),
load_date timestamp,
load_end_date timestamp,
record_sourse Varchar(20),
Hub_flights_Hash_key Varchar(33) REFERENCES data_vault.Hub_flights(hash_key),
Hub_airoport_data_Hash_key Varchar(33) REFERENCES data_vault.Hub_airports_data(hash_key)
);

-- flights_airoport_arrival_airport
CREATE TABLE IF NOT EXISTS data_vault.Link_flights_airoport_data_arrival(
Link_flights_airoport_data_arrival_Hashkey Varchar(33),
load_date timestamp,
load_end_date timestamp,
record_sourse Varchar(20),
Hub_flights_Hash_key Varchar(33) REFERENCES data_vault.Hub_flights(hash_key),
Hub_airoport_data_Hash_key Varchar(33) REFERENCES data_vault.Hub_airports_data(hash_key)
);

-- flights_ticket_flights
CREATE TABLE IF NOT EXISTS data_vault.Link_flights_ticket_flights(
Link_flights_ticket_flights_Hashkey Varchar(33),
load_date timestamp,
load_end_date timestamp,
record_sourse Varchar(30),
Hub_flights_Hash_key Varchar(33) REFERENCES data_vault.Hub_flights(hash_key),
ticket_no Varchar(33) , -- удалить потом, для инсерта первого
Hub_ticket_flights_Hash_key Varchar(33) REFERENCES data_vault.Hub_ticket_flights(hash_key));

-- ticket_flights_boarding_passes
CREATE TABLE IF NOT EXISTS data_vault.Link_ticket_flights_boarding_passes(
Link_ticket_flights_boarding_passes Varchar(40) ,
load_date timestamp,
load_end_date timestamp,
record_sourse Varchar(33),
ticket_no Varchar(33),
Hub_boarding_passes_Hash_key Varchar(33) /*REFERENCES в данном случае добавлены в коде с обработкой*/,
Hub_ticket_flights_Hash_key Varchar(33) /*REFERENCES в данном случае добавлены в коде с обработкой*/
);

-- ticket_flights_tickets
CREATE TABLE IF NOT EXISTS data_vault.Link_ticket_flights_tickets(
Link_ticket_flights_tickets Varchar(40),
load_date timestamp,
record_sourse Varchar(33), -- источники
Hub_ticket_flights_Hash_key Varchar(33) REFERENCES data_vault.Hub_ticket_flights(hash_key),
Hub_tickets_Hash_key Varchar(33) REFERENCES data_vault.Hub_tickets(hash_key)
);

-- tickets_bookings
CREATE TABLE IF NOT EXISTS data_vault.Link_tickets_bookings(
Link_tickets_bookings Varchar(33) PRIMARY KEY,
load_date timestamp,
load_end_date timestamp,
record_sourse Varchar(30),
Hub_ticket_Hash_key Varchar(33) REFERENCES data_vault.Hub_tickets(hash_key),
Hub_bookings_Hash_key Varchar(33) REFERENCES data_vault.Hub_bookings(hash_key)
);

-- flights_aircrafts_data
CREATE TABLE IF NOT EXISTS data_vault.Link_flights_aircrafts_data(
Link_flights_aircrafts_data_Hashkey Varchar(33),
load_date timestamp,
record_sourse Varchar(22),
Hub_flights_Hash_key Varchar(33) /*REFERENCES в данном случае добавлены в коде с обработкой*/,
Hub_aircrafts_data_Hash_key Varchar(33) /*REFERENCES в данном случае добавлены в коде с обработкой*/
);

-- seats_aircrafts_data
CREATE TABLE IF NOT EXISTS data_vault.Link_seats_aircrafts_data(
Link_seats_aircrafts_data_Hashkey Varchar(33),
load_date timestamp,
record_sourse Varchar(20),
Hub_aircrafts_data_Hash_key Varchar(33) /*REFERENCES в данном случае добавлены в коде с обработкой*/,
Hub_seats_Hash_key Varchar(33) /*REFERENCES в данном случае добавлен в коде с обработкой*/
);
```
---
#### DDL для таблиц Sattelite 

```
-- Чистим чистим

DROP TABLE IF exists data_vault.Sattelite_aircrafts_data_range;
DROP TABLE IF exists data_vault.Sattelite_aircrafts_data_model;
DROP TABLE IF exists data_vault.Sattelite_seats_fare_conditions;
DROP TABLE IF exists data_vault.Sattelite_airport_data_timezone;
DROP TABLE IF exists data_vault.Sattelite_airport_data_coordinates;
DROP TABLE IF exists data_vault.Sattelite_airport_data_airport_name;
DROP TABLE IF exists data_vault.Sattelite_airport_data_city;
DROP TABLE IF exists data_vault.Sattelite_ticket_flights_amount;
DROP TABLE IF exists data_vault.Sattelite_ticket_flights_fare_conditions;
DROP TABLE IF exists data_vault.Sattelite_boarding_passes_boarding_no;
DROP TABLE IF exists data_vault.Sattelite_boarding_passes_seat_no;
DROP TABLE IF exists data_vault.Sattelite_tickets_book_ref;
DROP TABLE IF exists data_vault.Sattelite_tickets_contact_data;
DROP TABLE IF exists data_vault.Sattelite_tickets_passenger_id;
DROP TABLE IF exists data_vault.Sattelite_tickets_passenger_name;
DROP TABLE IF exists data_vault.Sattelite_bookings_book_date;
DROP TABLE IF exists data_vault.Sattelite_bookings_total_amount;
DROP TABLE IF exists data_vault.Sattelite_flights_flight_no;
DROP TABLE IF exists data_vault.Sattelite_flights_scheduled_departure;
DROP TABLE IF exists data_vault.Sattelite_flights_scheduled_arrival;
DROP TABLE IF exists data_vault.Sattelite_flights_departure_airport;
DROP TABLE IF exists data_vault.Sattelite_flights_arrival_airport;
DROP TABLE IF exists data_vault.Sattelite_flights_status;
DROP TABLE IF exists data_vault.Sattelite_flights_aircraft_code;
DROP TABLE IF exists data_vault.Sattelite_flights_actual_departure;
DROP TABLE IF exists data_vault.Sattelite_flights_actual_arrival;
```

При формировании таблиц, делаем функцию, которая принимает на вход три параметра, первый - название таблицы саттелита, название хеша хаба, атрибут:
```
CREATE OR REPLACE FUNCTION data_vault.ddl_Sattelite_table(      
               table_name TEXT, -- название таблицы саттелита
               hub_hash TEXT, -- название хеша хаба, к которому относится саттелит
               atrib TEXT) -- какие колонки надо вытащить из таблицы источника указывается вместе с форматом
               RETURNS VOID AS $$
DECLARE 
     qwery TEXT;
BEGIN -- проверка на наличие данных в параметрах функции
    IF 
	  (table_name IS NULL) OR (hub_hash IS NULL) OR (atrib IS NULL)
		THEN RAISE EXCEPTION 'Не заполнены параметры';
	END IF;
qwery:= 'CREATE TABLE IF NOT EXISTS data_vault.' || table_name || '(' || 
         hub_hash || '_Hash_key Varchar(33)' || ' REFERENCES data_vault.' || hub_hash || '(Hash_key),' ||
         'load_date timestamp, load_end_date timestamp, record_sourse Varchar(20), ' || atrib || ', ' || 
         'PRIMARY KEY (' || hub_hash || '_Hash_key ' || ', load_date));';
    EXECUTE qwery;
    --RETURN qwery;    
END;
$$ LANGUAGE plpgsql;
```

Формирование таблиц Sattelite:
```
/*Sattelite_aircrafts_data*/
-- range
SELECT data_vault.ddl_Sattelite_table('Sattelite_aircrafts_data_range', 'Hub_aircrafts_data', 'range int4');
--model
SELECT data_vault.ddl_Sattelite_table('Sattelite_aircrafts_data_model', 'Hub_aircrafts_data', 'model TEXT');


/*Sattelite_seats*/
-- fare_conditions
SELECT data_vault.ddl_Sattelite_table('Sattelite_seats_fare_conditions', 'Hub_seats', 'fare_conditions Varchar(15)');

/*Sattelite_airoport_data*/
-- timezone
SELECT data_vault.ddl_Sattelite_table('Sattelite_airport_data_timezone', 'Hub_airport_data', 'timezone Varchar(34)');
-- coordinates
SELECT data_vault.ddl_Sattelite_table('Sattelite_airport_data_coordinates', 'Hub_airport_data', 'coordinates Varchar(50)');
-- airport_name
SELECT data_vault.ddl_Sattelite_table('Sattelite_airport_data_airport_name', 'Hub_airport_data', 'airport_name text');
-- city
SELECT data_vault.ddl_Sattelite_table('Sattelite_airport_data_city', 'Hub_airport_data', 'city text');

/*Sattelite_ticket_flights*/
-- amount
SELECT data_vault.ddl_Sattelite_table('Sattelite_ticket_flights_amount', 'Hub_ticket_flights', 'amount numeric(10, 2)');
-- fare_conditions
SELECT data_vault.ddl_Sattelite_table('Sattelite_ticket_flights_fare_conditions', 'Hub_ticket_flights', 'fare_conditions Varchar(10));

/*Sattelite_boarding_passes*/
-- boarding_no
SELECT data_vault.ddl_Sattelite_table('Sattelite_boarding_passes_boarding_no', 'Hub_boarding_passes', 'boarding_no int4');
-- seat_no
SELECT data_vault.ddl_Sattelite_table('Sattelite_boarding_passes_seat_no', 'Hub_boarding_passes', 'seat_no varchar(4)');

/*Sattelite_tickets*/
-- book_ref
SELECT data_vault.ddl_Sattelite_table('Sattelite_tickets_book_ref', 'Hub_ticket', 'book_ref bpchar(6)');
-- contact_data
SELECT data_vault.ddl_Sattelite_table('Sattelite_tickets_contact_data', 'Hub_ticket', 'contact_data jsonb');
-- passenger_id
SELECT data_vault.ddl_Sattelite_table('Sattelite_tickets_contact_data', 'Hub_ticket', 'passenger_id varchar(20)');
-- passenger_name
SELECT data_vault.ddl_Sattelite_table('Sattelite_tickets_passenger_name', 'Hub_ticket', 'passenger_name text');

/*Sattelite_bookings*/
-- book_date
SELECT data_vault.ddl_Sattelite_table('Sattelite_bookings_book_date', 'Hub_bookings', 'book_date timestamptz');
-- total_amount
SELECT data_vault.ddl_Sattelite_table('Sattelite_bookings_total_amount', 'Hub_bookings', 'total_amount numeric(10, 2)');

/*Sattelite_flights*/
-- flight_no
SELECT data_vault.ddl_Sattelite_table('Sattelite_flights_flight_no', 'Hub_flights', 'flight_no bpchar(6)');
-- scheduled_departure
SELECT data_vault.ddl_Sattelite_table('Sattelite_flights_scheduled_departure', 'Hub_flights', 'scheduled_departure timestamptz');
-- scheduled_arrival
SELECT data_vault.ddl_Sattelite_table('Sattelite_flights_scheduled_arrival', 'Hub_flights', 'scheduled_arrival timestamptz');
-- departure_airport
SELECT data_vault.ddl_Sattelite_table('Sattelite_flights_departure_airport', 'Hub_flights', 'departure_airport bpchar(3)');
-- arrival_airport
SELECT data_vault.ddl_Sattelite_table('Sattelite_flights_arrival_airport', 'Hub_flights', 'arrival_airport bpchar(3)');
-- status
SELECT data_vault.ddl_Sattelite_table('Sattelite_flights_status', 'Hub_flights', 'status varchar(20)');
-- aircraft_code
SELECT data_vault.ddl_Sattelite_table('Sattelite_flights_aircraft_code', 'Hub_flights', 'aircraft_code bpchar(3)');
-- actual_departure
SELECT data_vault.ddl_Sattelite_table('Sattelite_flights_actual_departure', 'Hub_flights', 'actual_departure timestamptz');
-- actual_arrival
SELECT data_vault.ddl_Sattelite_table('Sattelite_flights_actual_arrival', 'Hub_flights', 'actual_arrival timestamptz');
```

К итогам второго шага отнесем формирования ddl для саттелитов, хабов и линков и связи между ними путем формирования ограничений через REFERENCES. Остались заключительные шаги, с насыщением таблиц и постобработкой.


### Шаг 3. Перенос данных из stage хранилища в DataVault.
После залива данных в схему stage из временных таблиц foreign_tables и формирования ddl всех составляющих DataVault. Данные необходимо разложить по хабам, линкам и саттелитам. 

Начнем с Hub, напишем функцию, для миграции из схемы stage в схему data_vault.
```
CREATE OR REPLACE FUNCTION data_vault.insert_hub_table(      
               table_name TEXT , -- название таблицы хаба
               business_key TEXT, -- какие колонки надо вытащить из таблицы источника указывается вместе с форматом
               sourse TEXT, -- источник откуда тянутся данные
               times TEXT, -- время наполнения таблицы данными 
               sourse_table TEXT) -- полное обозначение источника для запроса 
               RETURNS VOID AS $$
DECLARE 
     qwery TEXT;
BEGIN -- проверка на наличие данных в параметрах функции
    IF 
	  (table_name IS NULL) OR (business_key IS NULL)
		THEN RAISE EXCEPTION 'Не заполнены параметры';
	END IF;
qwery:= 'INSERT INTO data_vault.' || table_name || '(Hash_key, record_sourse, Load_date, ' || (regexp_split_to_array(business_key, '::'))[1] || ')' ||
        ' SELECT DISTINCT MD5(' || business_key || '), ' ||
         sourse ||' , ' ||
         times ||' , ' ||
         (regexp_split_to_array(business_key, '::'))[1] ||
         ' FROM ' || sourse_table || ';';
    EXECUTE qwery;
END;
$$ LANGUAGE plpgsql;
```

Так выглядит запрос описанный в функции
```
INSERT INTO data_vault.Hub_flights (Hash_key, record_sourse, Load_date, flight_id)
SELECT DISTINCT
       MD5(flight_id::varchar), 
       'flights',
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       flight_id
FROM stage.flights;
```

Определим функцию для всех хабов.
```
/*Hub_flights*/
SELECT data_vault.insert_hub_table('Hub_flights',
                                   'flight_id::varchar',
                                   '''flights''',
                                   $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,
                                   'stage.flights')

/*Hub_airports_data*/
SELECT data_vault.insert_hub_table('Hub_airports_data',
                                   'airport_code::varchar',
                                   '''airports_data''',
                                   $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,
                                   'stage.airports_data')

/*Hub_aircrafts_data*/
SELECT data_vault.insert_hub_table('Hub_aircrafts_data',
                                   'aircraft_code::varchar',
                                   '''aircrafts_data''',
                                   $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,
                                   'stage.aircrafts_data')

/*Hub_seats*/
SELECT data_vault.insert_hub_table('Hub_seats',
                                   'concat(aircraft_code, seat_no)',
                                   '''seats''',
                                   $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,
                                   'stage.seats')

/*Hub_ticket_flights*/
SELECT data_vault.insert_hub_table('Hub_ticket_flights',
                                   '(concat(ticket_no, '&', flight_id))::varchar',
                                   '''ticket_flights''',
                                   $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,
                                   'stage.ticket_flights')

/*Hub_boarding_passes*/
SELECT data_vault.insert_hub_table('Hub_boarding_passes',
                                   '(concat(ticket_no, '&', flight_id))::varchar',
                                   '''boarding_passes''',
                                   $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,
                                   'stage.boarding_passes')

/*Hub_tickets*/
SELECT data_vault.insert_hub_table('Hub_tickets',
                                   '(ticket_no)::varchar',
                                   '''tickets''',
                                   $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,
                                   'stage.tickets')

/*Hub_bookings*/
SELECT data_vault.insert_hub_table('Hub_bookings',
                                   '(book_ref)::varchar',
                                   '''bookings''',
                                   $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,
                                   'stage.bookings')
```
Для наполнения таблиц Link я выкачу простынь, потому что нецелесообразно писать функцию, тк в параметрах будет много переменных.

```
/*Link_flights_airoport_data_departure - departure_airport - аэропорт отправления*/
INSERT INTO data_vault.Link_flights_airoport_data_departure
                 (Link_flights_airoport_data_departure_Hashkey, load_date, load_end_date, record_sourse, Hub_flights_Hash_key, Hub_airoport_data_Hash_key)
SELECT DISTINCT 
      concat(flight_id, COALESCE(sf.departure_airport, sa.airport_code)),
      to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
      '1111-11-11 11:11:11' :: timestamp,
      'flights_airport_data',
      MD5(flight_id :: varchar),
      MD5(sa.airport_code)
FROM stage.flights sf
            LEFT JOIN stage.airports_data sa 
            ON sf.departure_airport = sa.airport_code;

/*Link_flights_airoport_data - arrival_airport - аэропорт прибытия*/
INSERT INTO data_vault.Link_flights_airoport_data_arrival
                 (Link_flights_airoport_data_arrival_Hashkey, load_date, load_end_date, record_sourse, Hub_flights_Hash_key, Hub_airoport_data_Hash_key)
SELECT DISTINCT 
      concat(flight_id, COALESCE(sf.departure_airport, sa.airport_code)),
      to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
      '1111-11-11 11:11:11' :: timestamp,
      'flights_airport_data',
      MD5(flight_id :: varchar),
      MD5(sa.airport_code)
FROM stage.flights sf
            LEFT JOIN stage.airports_data sa 
            ON sf.arrival_airport = sa.airport_code;

/*Link_flights_ticket_flights*/
INSERT INTO data_vault.Link_flights_ticket_flights (Link_flights_ticket_flights_Hashkey, load_date, load_end_date, record_sourse, hub_flights_hash_key, ticket_no, hub_ticket_flights_hash_key)
SELECT DISTINCT
       (concat(tf.ticket_no, '&', f.flight_id)::varchar),
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       '1111-11-11 11:11:11' :: timestamp,
       'flights_ticket_flights', 
       md5(f.flight_id :: varchar), 
       md5(tf.ticket_no),
       MD5((concat(tf.ticket_no, '&', f.flight_id))::varchar) 
FROM stage.ticket_flights tf
                          JOIN stage.flights f ON f.flight_id = tf.flight_id;

/*Link_ticket_flights_boarding_passes*/
INSERT INTO data_vault.Link_ticket_flights_boarding_passes (Link_ticket_flights_boarding_passes, load_date, load_end_date, record_sourse, ticket_no, Hub_boarding_passes_Hash_key, Hub_ticket_flights_Hash_key)
SELECT 
      DISTINCT concat(concat(tf.ticket_no, '&' ,tf.flight_id), concat(bp.ticket_no, '&' ,bp.flight_id)),
      to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
      '1111-11-11 11:11:11' :: timestamp,
      'ticket_flights_boarding_passes',
      md5(bp.ticket_no),
      concat(bp.ticket_no, '&',bp.flight_id),
      concat(tf.ticket_no, '&',tf.flight_id)
FROM stage.boarding_passes bp
                          LEFT JOIN stage.ticket_flights tf 
                          USING(ticket_no, flight_id);

/*Link_ticket_flights_tickets*/
INSERT INTO data_vault.Link_ticket_flights_tickets (Link_ticket_flights_tickets, load_date, record_sourse, Hub_ticket_flights_Hash_key, Hub_tickets_Hash_key)
SELECT DISTINCT
       concat(concat(tf.ticket_no, '&', tf.flight_id), t.ticket_no),
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       'ticket_flights_tickets',
       md5(concat(tf.ticket_no, '&', tf.flight_id)), 
       md5(t.ticket_no)
FROM stage.ticket_flights tf  
                  full JOIN stage.tickets t
                  ON t.ticket_no = tf.ticket_no;

/*Link_ticket_bookings*/ 
INSERT INTO data_vault.Link_tickets_bookings (Link_tickets_bookings, load_date, load_end_date, record_sourse, Hub_ticket_Hash_key, Hub_bookings_Hash_key)
SELECT concat(ticket_no, book_ref),
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       '1111-11-11 11:11:11' :: timestamp,
       'ticket_bookings',
       md5(ticket_no),
       md5(book_ref)
FROM stage.tickets t
             FULL JOIN stage.bookings b USING(book_ref);

/*Link_flights_aircrafts_data*/ 
INSERT INTO data_vault.Link_flights_aircrafts_data (Link_flights_aircrafts_data_Hashkey, load_date, record_sourse, Hub_flights_Hash_key, Hub_aircrafts_data_Hash_key)
SELECT DISTINCT 
     concat(f.aircraft_code, ad.aircraft_code),
     to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
     'flights_aircrafts_data',
     f.aircraft_code,
     ad.aircraft_code
FROM stage.flights f  
            FULL JOIN stage.aircrafts_data ad 
               ON f.aircraft_code = ad.aircraft_code;

/*Link_seats_aircrafts_data*/
INSERT INTO data_vault.Link_seats_aircrafts_data(Link_seats_aircrafts_data_Hashkey, load_date, record_sourse, Hub_aircrafts_data_Hash_key, Hub_seats_Hash_key)
SELECT DISTINCT 
     concat(ad.aircraft_code, concat(s.aircraft_code, s.seat_no)),
     to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
     'seats_aircrafts_data',
     ad.aircraft_code,
     concat(s.aircraft_code, s.seat_no)
FROM stage.aircrafts_data ad 
             JOIN stage.seats s USING(aircraft_code);
```

Для заполнения данных в таблицах-спутниках написана функция, поскольку для каждого атрибута основной таблицы была создана отдельная таблица-спутник, которых в итоге получилось более двадцати. [Зачем на каждый атрибут (желательно) делать свой саттелит.](https://youtu.be/IZw1cB1uDts?t=390).

```
/*Функция*/
CREATE OR REPLACE FUNCTION data_vault.insert_data_from_table(      
               table_name TEXT , -- название таблицы саттелита
               source_table_columns TEXT, -- какие колонки надо вытащить из таблицы источника
               source_table_name TEXT, -- имя таблицы откуда тянутся данные
               hash TEXT, -- из чего будет состоять хеш
               times TEXT, -- время записи данных в таблицу
               sourse_col TEXT) -- название источника
               RETURNS VOID AS $$
DECLARE 
     qwery TEXT;
BEGIN -- проверка на наличие информации в параметрах
	IF (table_name IS NULL) OR (source_table_columns IS NULL) OR (source_table_name IS NULL) OR (hash IS NULL) OR (times IS NULL) OR (sourse_col IS NULL)
	   THEN RAISE EXCEPTION 'Не заполнены параметры';
	END IF;
    qwery:= 'INSERT INTO ' || table_name || 
            ' (' || (regexp_split_to_array(source_table_columns, ','))[1] || ', load_date' || ' ,' || ' load_end_date' || ' ,' || ' record_sourse' || ', ' 
           || (regexp_split_to_array(source_table_columns, ','))[2] || ') ' || ' SELECT DISTINCT md5(' || hash || ') , ' 
          ||  times || ', ' || '''1111-11-11 11:11:11'':: timestamp'|| ' , ' || sourse_col || ', ' || (regexp_split_to_array(source_table_columns, ','))[2] || ' FROM ' || source_table_name;
   EXECUTE qwery;
END;
$$ LANGUAGE plpgsql;
```

Определяем параметры функции к саттелитам (формат иерархии: Название таблицы как в исходнике , атрибуты таблицы, которые являются саттелитом):
```
-- Sattelite_aircrafts_data
--#####################################################################################################
/*Аircrafts_data_range*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_aircrafts_data_range' , -- название таблицы саттелита
                                          'Hub_aircrafts_data_Hash_key,range',         -- какие колонки надо вытащить из таблицы источника
                                          'stage.aircrafts_data',                      -- имя таблицы откуда тянутся данные
                                          'aircraft_code::varchar',                    -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''aircrafts_data''');
-- SELECT * FROM data_vault.Sattelite_aircrafts_data_range;

/*Аircrafts_data_model*/                                         
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_aircrafts_data_model' , -- название таблицы саттелита
                                          'Hub_aircrafts_data_Hash_key,model',         -- какие колонки надо вытащить из таблицы источника
                                          'stage.aircrafts_data',                      -- имя таблицы откуда тянутся данные
                                          'aircraft_code::varchar',                    -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''aircrafts_data''');
-- SELECT * FROM data_vault.Sattelite_aircrafts_data_model;                                         

  
-- Sattelite_seats
--#####################################################################################################
/*fare_conditions*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_seats_fare_conditions' ,-- название таблицы саттелита
                                          'Hub_seats_Hash_key,fare_conditions',        -- какие колонки надо вытащить из таблицы источника
                                          'stage.seats',                               -- имя таблицы откуда тянутся данные
                                          'concat(aircraft_code, seat_no)',            -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''seats''');
-- SELECT * FROM data_vault.Sattelite_seats_fare_conditions;

                                         
-- Sattelite_airoport_data
--#####################################################################################################
 /*timezone*/       
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_airport_data_timezone' ,-- название таблицы саттелита
                                          'Hub_airport_data_Hash_key,timezone',        -- какие колонки надо вытащить из таблицы источника
                                          'stage.airports_data',                      -- имя таблицы откуда тянутся данные
                                          'airport_code::varchar',                     -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''airoport_data''');
                                         
-- SELECT * FROM data_vault.Sattelite_airport_data_timezone;
 
 /*coordinates*/                                           
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_airport_data_coordinates' , -- название таблицы саттелита
                                          'Hub_airport_data_Hash_key,coordinates',         -- какие колонки надо вытащить из таблицы источника
                                          'stage.airports_data',                           -- имя таблицы откуда тянутся данные
                                          'airport_code::varchar',                         -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''airoport_data''');
-- SELECT * FROM data_vault.Sattelite_airport_data_coordinates;                                  

 /*airport_name*/  
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_airport_data_airport_name' , -- название таблицы саттелита
                                          'Hub_airport_data_Hash_key,airport_name',         -- какие колонки надо вытащить из таблицы источника
                                          'stage.airports_data',                           -- имя таблицы откуда тянутся данные
                                          'airport_code::varchar',                         -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''airoport_data''');
-- SELECT * FROM data_vault.Sattelite_airport_data_airport_name;                                   
                                         
 /*city*/  
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_airport_data_city' , -- название таблицы саттелита
                                          'Hub_airport_data_Hash_key,city',         -- какие колонки надо вытащить из таблицы источника
                                          'stage.airports_data',                           -- имя таблицы откуда тянутся данные
                                          'airport_code::varchar',                         -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''airoport_data''');
-- SELECT * FROM data_vault.Sattelite_airport_data_city;      
                                         

-- Sattelite_ticket_flights
--#####################################################################################################
 /*amount*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_ticket_flights_amount' ,             -- название таблицы саттелита
                                          'Hub_ticket_flights_Hash_key,amount',                     -- какие колонки надо вытащить из таблицы источника
                                          'stage.ticket_flights',                                   -- имя таблицы откуда тянутся данные
                                          $$(concat(ticket_no, '&', flight_id))::varchar$$,         -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, -- время залития
                                          '''airoport_data''');
-- SELECT * FROM data_vault.Sattelite_ticket_flights_amount;  
                                         
 /*fare_conditions*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_ticket_flights_fare_conditions' ,    -- название таблицы саттелита
                                          'Hub_ticket_flights_Hash_key,fare_conditions',            -- какие колонки надо вытащить из таблицы источника
                                          'stage.ticket_flights',                                   -- имя таблицы откуда тянутся данные
                                          $$(concat(ticket_no, '&', flight_id))::varchar$$,         -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''airoport_data''');
-- SELECT * FROM data_vault.Sattelite_ticket_flights_fare_conditions; 
                                         

-- Sattelite_boarding_passes
--#####################################################################################################
 /*boarding_no*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_boarding_passes_boarding_no' ,    -- название таблицы саттелита
                                          'Hub_boarding_passes_Hash_key,boarding_no',            -- какие колонки надо вытащить из таблицы источника
                                          'stage.boarding_passes',                                   -- имя таблицы откуда тянутся данные
                                          $$(concat(ticket_no, '&', flight_id))::varchar$$,         -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''boarding_passes''');
-- SELECT * FROM data_vault.Sattelite_boarding_passes_boarding_no; 
                                         
                                         
 /*seat_no*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_boarding_passes_seat_no' ,    -- название таблицы саттелита
                                          'Hub_boarding_passes_Hash_key,seat_no',            -- какие колонки надо вытащить из таблицы источника
                                          'stage.boarding_passes',                           -- имя таблицы откуда тянутся данные
                                          $$(concat(ticket_no, '&', flight_id))::varchar$$,  -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''boarding_passes''');
-- SELECT * FROM data_vault.Sattelite_boarding_passes_seat_no;      
            
                                         
-- Sattelite_tickets
--#####################################################################################################
 /*book_ref*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_tickets_book_ref' ,  -- название таблицы саттелита
                                          'Hub_ticket_Hash_key,book_ref',           -- какие колонки надо вытащить из таблицы источника
                                          'stage.tickets',                          -- имя таблицы откуда тянутся данные
                                          $$(ticket_no)::varchar$$,                 -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''tickets''');
-- SELECT * FROM data_vault.Sattelite_tickets_book_ref;  
                                         
 /*contact_data*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_tickets_contact_data' ,  -- название таблицы саттелита
                                          'Hub_ticket_Hash_key,contact_data',           -- какие колонки надо вытащить из таблицы источника
                                          'stage.tickets',                          -- имя таблицы откуда тянутся данные
                                          $$(ticket_no)::varchar$$,                 -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''tickets''');
-- SELECT * FROM data_vault.Sattelite_tickets_contact_data;  
                                         
                                         
 /*passenger_id*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_tickets_passenger_id' ,  -- название таблицы саттелита
                                          'Hub_ticket_Hash_key,passenger_id',           -- какие колонки надо вытащить из таблицы источника
                                          'stage.tickets',                          -- имя таблицы откуда тянутся данные
                                          $$(ticket_no)::varchar$$,                 -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''tickets''');
-- SELECT * FROM data_vault.Sattelite_tickets_passenger_id;                                     
                                         
                                         
 /*passenger_name*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_tickets_passenger_name' ,  -- название таблицы саттелита
                                          'Hub_ticket_Hash_key,passenger_name',           -- какие колонки надо вытащить из таблицы источника
                                          'stage.tickets',                          -- имя таблицы откуда тянутся данные
                                          $$(ticket_no)::varchar$$,                 -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''tickets''');
-- SELECT * FROM data_vault.Sattelite_tickets_passenger_name;  
                                         
                                         
-- Sattelite_bookings
--#####################################################################################################
 /*book_date*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_bookings_book_date' ,  -- название таблицы саттелита
                                          'Hub_bookings_Hash_key,book_date',           -- какие колонки надо вытащить из таблицы источника
                                          'stage.bookings',                          -- имя таблицы откуда тянутся данные
                                          $$(book_ref)::varchar$$,                 -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''bookings''');
-- SELECT * FROM data_vault.Sattelite_bookings_book_date;
                                         
 /*total_amount*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_bookings_total_amount' ,  -- название таблицы саттелита
                                          'Hub_bookings_Hash_key,total_amount',           -- какие колонки надо вытащить из таблицы источника
                                          'stage.bookings',                          -- имя таблицы откуда тянутся данные
                                          $$(book_ref)::varchar$$,                 -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''bookings''');
-- SELECT * FROM data_vault.Sattelite_bookings_total_amount;
                                         
                                         
-- Sattelite_flights
--#####################################################################################################
 /*flight_no*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_flights_flight_no' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,flight_no',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                          -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                 -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_flight_no;

 /*scheduled_departure*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_flights_scheduled_departure' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,scheduled_departure',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                     -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                              -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_scheduled_departure;
                                         
 /*scheduled_arrival*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_flights_scheduled_arrival' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,scheduled_arrival',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_scheduled_arrival;
                                         
 /*departure_airport*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_flights_departure_airport' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,departure_airport',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_departure_airport;
                                         
 /*arrival_airport*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_flights_arrival_airport' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,arrival_airport',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_arrival_airport;
                                         
 /*status*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_flights_status' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,status',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_status;

 /*aircraft_code*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_flights_aircraft_code' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,aircraft_code',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_aircraft_code;
                                         
 /*actual_departure*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_flights_actual_departure' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,actual_departure',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_actual_departure;

 /*actual_arrival*/
SELECT data_vault.insert_data_from_table('data_vault.Sattelite_flights_actual_arrival' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,actual_arrival',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_actual_arrival;
```
В схеме DataVault собраны и наполнены все три составляющих - Hub, Link, Sattelite. 

### Шаг 4. Жизненый цикл DataVault.

Необходимость обработки данных в таблицах DataVault обосновывается изменениями, обновлениями или дополнениями информации в таблицах исходниках. Например, конструкция таблиц Hub должна оставаться неизменной, т.е. данные не могут подвергаться изменениям, только добавляться. В Link и Sattelite наоборот, таблицы имеют в своем составе атрибут load_end_date - любая запись имеет свой срок жизни, если в источнике изменился атрибут, то в линке или саттелите обновится поле load_end_date и появится новая строка, согласно идеологии Дена Линстеда - идейного вдохновителя и основателя данного подхода.

Обработка для хабов примечательна тем, что данные сопоставляются с источником, если таких нет в хабе, то добавляем.
```
-- Обработка Hub

/*Hub_flights*/
INSERT INTO data_vault.Hub_flights (Hash_key, record_sourse, Load_date, flight_id)
SELECT DISTINCT
       MD5(flight_id::varchar), 
       'flights',
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       flight_id
FROM stage.flights f
                  LEFT JOIN data_vault.Hub_flights hf USING(flight_id)
WHERE load_date IS null;

/*Hub_airports_data*/
INSERT INTO data_vault.Hub_airports_data (Hash_key, record_sourse, Load_date, airport_code)
SELECT DISTINCT
       MD5(airport_code::varchar), 
       'airports_data',
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       airport_code
FROM stage.airports_data ad
                           LEFT JOIN data_vault.Hub_airports_data hf USING(airport_code)
WHERE load_date IS null;

/*Hub_aircrafts_data*/
INSERT INTO data_vault.Hub_aircrafts_data (Hash_key, record_sourse, Load_date, aircraft_code)
SELECT DISTINCT
       MD5(aircraft_code::varchar), 
       'aircrafts_data',
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       aircraft_code
FROM stage.aircrafts_data ad
                           LEFT JOIN data_vault.Hub_aircrafts_data hf USING(aircraft_code)
WHERE load_date IS null;

/*Hub_seats*/
INSERT INTO data_vault.Hub_seats (Hash_key, record_sourse, Load_date, aircraft_code_seat_no)
SELECT DISTINCT
       MD5(concat(aircraft_code, seat_no)), 
       'seats',
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       concat(aircraft_code, seat_no) aircraft_code_seat_no
FROM stage.seats ad
                  LEFT JOIN data_vault.Hub_seats hf ON concat(ad.aircraft_code, ad.seat_no) = hf.aircraft_code_seat_no
WHERE load_date IS null;

/*Hub_ticket_flights*/
INSERT INTO data_vault.Hub_ticket_flights (Hash_key, record_sourse, Load_date, ticket_no_flight_id)
SELECT DISTINCT
       MD5(concat(ticket_no, '&', flight_id)::varchar), 
       'ticket_flights',
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       concat(ticket_no, '&', flight_id)
FROM stage.ticket_flights еа
                  LEFT JOIN data_vault.Hub_ticket_flights htf ON (concat(еа.ticket_no, '&', еа.flight_id)) = ticket_no_flight_id
WHERE load_date IS null;

/*Hub_boarding_passes*/
INSERT INTO data_vault.Hub_boarding_passes (Hash_key, record_sourse, Load_date, ticket_no_flight_id)
SELECT DISTINCT
       MD5(concat(ticket_no, '&', flight_id)::varchar), 
       'boarding_passes',
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       concat(ticket_no, '&', flight_id)
FROM stage.ticket_flights еа
                  LEFT JOIN data_vault.Hub_ticket_flights htf ON (concat(еа.ticket_no, '&', еа.flight_id)) = ticket_no_flight_id
WHERE load_date IS null;

/*Hub_tickets*/
INSERT INTO data_vault.Hub_tickets (Hash_key, record_sourse, Load_date, ticket_no)
SELECT DISTINCT
       MD5((ticket_no)::varchar), 
       'tickets',
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       ticket_no
FROM stage.tickets еа
                  LEFT JOIN data_vault.Hub_tickets htf USING(ticket_no)
WHERE load_date IS null;

/*Hub_bookings*/
INSERT INTO data_vault.Hub_bookings (Hash_key, record_sourse, Load_date, book_ref)
SELECT DISTINCT
       MD5((book_ref)::varchar), 
       'bookings',
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       book_ref
FROM stage.tickets еа
                  LEFT JOIN data_vault.Hub_bookings htf USING(book_ref)
WHERE load_date IS null;
```


Можно конечно не мостить простынь, а сделать аккуратную функцию, но я не стал =) 

Самым трудозатратным на этом пути было прописать обработку таблиц link, так как каждая таблица имеет свою структуру упростить обработку у меня не вышло, выкатываю очередную простынь.

```
/*###########################
обработка - departure_airport
#############################*/

/*проверка на наличие изменение связей - если поменяют название аэропорта*/
DROP TABLE IF EXISTS data_vault.for_lfadp_update_time;

CREATE TABLE IF NOT EXISTS data_vault.for_lfadp_update_time AS 
SELECT DISTINCT  lfadd.hub_flights_hash_key, 
                 lfadd.hub_airoport_data_hash_key, 
                 MD5(hf.flight_id :: varchar) AS new_flight_id, 
                 MD5(hf.departure_airport :: varchar) AS new_airport_name,
                 hf.flight_id new_flight,
                 hf.departure_airport new_airport,
                 to_char(now() - INTERVAL '1 second', 'YYYY-MM-DD HH24:MI:SS') :: timestamp AS new_fix_time
FROM stage.flights hf
            LEFT JOIN data_vault.Link_flights_airoport_data_departure lfadd ON md5(hf.flight_id::varchar) = lfadd.hub_flights_hash_key
WHERE (CASE WHEN upper(replace(lfadd.hub_airoport_data_hash_key, ' ', '')) = upper(replace(md5(hf.departure_airport::varchar), ' ', '')) THEN 1 ELSE 0 END) = 0;

-- обновляем время в линке на новое, если сменилось название аэропорта в исходнике
UPDATE data_vault.Link_flights_airoport_data_departure a
      SET load_end_date = flut.new_fix_time
      FROM data_vault.for_lfadp_update_time flut
      WHERE a.hub_flights_hash_key = 
            flut.new_flight_id;
           
-- добавляем строку в таблицу Link_flights_airoport_data_departure новую 
INSERT INTO data_vault.Link_flights_airoport_data_departure
                    (Link_flights_airoport_data_departure_Hashkey, load_date, load_end_date, record_sourse, Hub_flights_Hash_key, Hub_airoport_data_Hash_key)
SELECT 
      concat(new_flight, new_airport),
      new_fix_time + INTERVAL '1 second',
      '1111-11-11 11:11:11' :: timestamp,
      'flights_airport_data',
      new_flight_id,
      new_airport_name
FROM data_vault.for_lfadp_update_time;

DROP TABLE IF EXISTS data_vault.for_lfadp_update_time;
/*обработка departure_airport завершена*/


/*###########################
обработка - arrival_airport
#############################*/

/*проверка на наличие изменение связей - если поменяют название аэропорта*/
DROP TABLE IF EXISTS data_vault.for_lfada_update_time;

CREATE TABLE IF NOT EXISTS data_vault.for_lfada_update_time AS 
SELECT DISTINCT  lfada.hub_flights_hash_key, 
                 lfada.hub_airoport_data_hash_key, 
                 MD5(hf.flight_id :: varchar) AS new_flight_id, 
                 MD5(hf.arrival_airport :: varchar) AS new_airport_name,
                 hf.flight_id new_flight,
                 hf.arrival_airport new_airport,
                 to_char(now() - INTERVAL '1 second', 'YYYY-MM-DD HH24:MI:SS') :: timestamp AS new_fix_time
FROM stage.flights hf
            LEFT JOIN data_vault.Link_flights_airoport_data_arrival lfada ON md5(hf.flight_id::varchar) = lfada.hub_flights_hash_key
WHERE (CASE WHEN upper(replace(lfada.hub_airoport_data_hash_key, ' ', '')) = upper(replace(md5(hf.arrival_airport::varchar), ' ', '')) THEN 1 ELSE 0 END) = 0;

-- SELECT * FROM data_vault.for_lfada_update_time

-- обновляем время в линке на новое, если сменилось название аэропорта в исходнике
UPDATE data_vault.Link_flights_airoport_data_arrival a
      SET load_end_date = flut.new_fix_time
      FROM data_vault.for_lfada_update_time flut
      WHERE a.hub_flights_hash_key = 
            flut.new_flight_id;

           
INSERT INTO data_vault.Link_flights_airoport_data_arrival
                    (Link_flights_airoport_data_arrival_Hashkey, load_date, load_end_date, record_sourse, Hub_flights_Hash_key, Hub_airoport_data_Hash_key)
SELECT 
      concat(new_flight_id, new_airport_name),
      new_fix_time + INTERVAL '1 second',
      '1111-11-11 11:11:11' :: timestamp,
      'flights_airport_data',
      new_flight_id,
      new_airport_name
FROM data_vault.for_lfada_update_time;

DROP TABLE IF EXISTS data_vault.for_lfada_update_time;
/*обработка arrival_airport завершена*/


/*################################
обработка - flights_ticket_flights
##################################*/

/*проверка если отменили рейс flight_id и перенесли билет на новый рейс - такое же может быть , значит load end date будет актуален*/
DROP TABLE IF EXISTS data_vault.for_lfada_update_time;

CREATE TABLE IF NOT EXISTS data_vault.for_lfada_update_time as
WITH 
new AS (SELECT distinct
                     replace(concat(tf.ticket_no, tf.flight_id), ' ', '') new_data
                   , tf.ticket_no
                   , tf.flight_id n_flight_id
          FROM stage.ticket_flights tf 
                  LEFT JOIN data_vault.Link_flights_ticket_flights lftf
                  ON replace(concat(md5(tf.ticket_no), md5(tf.flight_id :: varchar)), ' ', '') = replace(concat(lftf.ticket_no , lftf.Hub_flights_Hash_key), ' ', '')
          WHERE LENGTH(replace(concat(lftf.ticket_no, lftf.Hub_flights_Hash_key), ' ', '')) = 0),
      
old AS (SELECT distinct
                    lftf.ticket_no
                   , Hub_flights_Hash_key o_flight_id
           FROM data_vault.Link_flights_ticket_flights lftf
                  LEFT JOIN stage.ticket_flights tf 
                  ON replace(concat(md5(tf.ticket_no), md5(tf.flight_id :: varchar)), ' ', '') = replace(concat(lftf.ticket_no , lftf.Hub_flights_Hash_key), ' ', '')
           WHERE LENGTH(replace(concat(tf.ticket_no, tf.flight_id), ' ', '')) = 0)
           
SELECT md5(NEW.ticket_no) ticket_no, 
       concat(OLD.ticket_no, o_flight_id) AS OLD_ticket_no_flight_id,
       o_flight_id AS OLD_flight_id, 
       concat(NEW.ticket_no, '&', n_flight_id) AS NEW_ticket_no_flight_id, 
       md5(n_flight_id ::varchar) AS NEW_flight_id,
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp AS new_fix_time
FROM old JOIN NEW ON OLD.ticket_no = NEW.ticket_no;
-- SELECT * FROM data_vault.for_lfada_update_time;

-- обновляем время в линке на новое, если сменилось название аэропорта в исходнике
UPDATE data_vault.Link_flights_ticket_flights a
      SET load_end_date = flut.new_fix_time
      FROM data_vault.for_lfada_update_time flut
      WHERE concat(a.ticket_no, a.hub_flights_hash_key) = 
            flut.OLD_ticket_no_flight_id;

-- добавляем строку в таблицу Link_flights_ticket_flights новую, которую добавили в ticket_flights
INSERT INTO data_vault.Link_flights_ticket_flights (Link_flights_ticket_flights_Hashkey, load_date, load_end_date, record_sourse, Hub_flights_Hash_key, ticket_no, Hub_ticket_flights_Hash_key)
SELECT flut.NEW_ticket_no_flight_id,
       to_char(now() - INTERVAL '1 second', 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       '1111-11-11 11:11:11' :: timestamp,
       'flights_ticket_flights',
       md5(NEW_flight_id),
       md5(ticket_no),
       md5(flut.NEW_ticket_no_flight_id)
FROM data_vault.for_lfada_update_time flut;
/*обработка flights_ticket_flights завершена*/


/*########################################
обработка - ticket_flights_boarding_passes
##########################################*/

-- так как данные в таблице связаны с хабами , а обработка линка идет с не зашифрованными данными т.е. без md5, то удалим привязку 
ALTER TABLE data_vault.Link_ticket_flights_boarding_passes DROP CONSTRAINT IF EXISTS Hub_bp_fk;
ALTER TABLE data_vault.Link_ticket_flights_boarding_passes DROP CONSTRAINT IF EXISTS Hub_tf_fk;

DROP TABLE IF EXISTS data_vault.for_lfada_update_time;

-- проверка на новые данные , если их нет то добавляем 
CREATE TABLE IF NOT EXISTS data_vault.for_lfada_update_time as
WITH FIRST as(SELECT concat(bp.ticket_no, '&', bp.flight_id) AS concat_bp,
                     concat(tf.ticket_no, '&', tf.flight_id) AS concat_tf,
                     bp.ticket_no
              FROM stage.boarding_passes bp
                   LEFT JOIN stage.ticket_flights tf USING(ticket_no, flight_id))
SELECT concat_bp,
       concat_tf,
       hub_boarding_passes_hash_key,
       Hub_ticket_flights_Hash_key,
       f.ticket_no
FROM FIRST f
          LEFT JOIN data_vault.Link_ticket_flights_boarding_passes flut 
          ON f.concat_bp = Hub_ticket_flights_Hash_key AND f.concat_tf = Hub_boarding_passes_Hash_key
WHERE load_date IS null;

-- вставляем в таблицу обновленные элементы с источника, вставятся все поля если 
INSERT INTO data_vault.Link_ticket_flights_boarding_passes (Link_ticket_flights_boarding_passes, 
                                                            load_date, load_end_date, record_sourse, 
                                                             ticket_no, Hub_boarding_passes_Hash_key, 
                                                             Hub_ticket_flights_Hash_key)
SELECT concat(flut.concat_tf , flut.concat_bp),
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       '1111-11-11 11:11:11' :: timestamp,
       'ticket_flights_boarding_passes',
       md5(ticket_no),
       flut.concat_bp,
       flut.concat_tf
FROM data_vault.for_lfada_update_time flut;

-- если один из хешей не подгрузился, догружаем его, вытащив из уже известного, так как хеши таблиц boarding_passes и ticket_flights одинаковы
-- обновляем данные столбца ticket_flights_boarding_passes с хешем , если он будет не заполнен
UPDATE data_vault.Link_ticket_flights_boarding_passes a
      SET (hub_boarding_passes_hash_key,
           Hub_ticket_flights_Hash_key,
           link_ticket_flights_boarding_passes) = 
                                              (concat(ticket_nom, '&', flight_id),
                                               concat(ticket_nom, '&', flight_id),
                                               concat(concat(ticket_nom, '&', flight_id), concat(ticket_nom, '&', flight_id)))
      FROM 
      
           (SELECT CASE WHEN 
               length(CASE WHEN length(Hub_ticket_flights_Hash_key) < 2 OR length(hub_boarding_passes_hash_key) < 2
                           THEN substring(Hub_ticket_flights_Hash_key FROM 1 FOR position('&' IN Hub_ticket_flights_Hash_key) - 1)
                           ELSE '0' 
                           END) < 1 
            THEN substring(hub_boarding_passes_hash_key FROM 1 FOR position('&' IN hub_boarding_passes_hash_key) - 1) 
            ELSE 'ошибка: необходима проверка данных' END ticket_nom,
       
                   CASE WHEN 
               length(CASE WHEN length(Hub_ticket_flights_Hash_key) < 2 OR length(hub_boarding_passes_hash_key) < 2
                           THEN substring(Hub_ticket_flights_Hash_key FROM position('&' IN Hub_ticket_flights_Hash_key) + 1)
                           ELSE '0' 
                           END) < 1 
            THEN substring(hub_boarding_passes_hash_key FROM position('&' IN hub_boarding_passes_hash_key) + 1 ) 
            ELSE 'ошибка: необходима проверка данных' END flight_id,
              link_ticket_flights_boarding_passes,load_date,load_end_date,record_sourse,ticket_no,hub_boarding_passes_hash_key,Hub_ticket_flights_Hash_key   

            FROM data_vault.Link_ticket_flights_boarding_passes
            WHERE length(hub_boarding_passes_hash_key) < 2
                                      OR  length(Hub_ticket_flights_Hash_key) < 2) flut
                                      
      WHERE a.hub_boarding_passes_hash_key = 
            (concat(flut.ticket_nom, '&', flut.flight_id)) 
         OR a.Hub_ticket_flights_Hash_key = (concat(flut.ticket_nom, '&', flut.flight_id));     

                         
-- хеш состоит из 32 символов , обновляем данные у которых длинна символов меньше 30, т.е. при последующих обработках захешированные данные, которые уже лежат в линке не будут подвержены переводу в формат md5. Обработка идет без хеширования, для перевода данных в формат md5 мы берем только те данные, у которых длина строки менее 30, т.е. новые данные
UPDATE data_vault.Link_ticket_flights_boarding_passes a
      SET (hub_boarding_passes_hash_key, 
           hub_ticket_flights_hash_key) = (md5(b.hub_boarding_passes_hash_key), md5(b.hub_ticket_flights_hash_key))
      FROM data_vault.Link_ticket_flights_boarding_passes b                                       
      WHERE (a.link_ticket_flights_boarding_passes = b.link_ticket_flights_boarding_passes)
          AND (length(Hub_boarding_passes_Hash_key) < 30 OR (length(Hub_ticket_flights_Hash_key) < 30)); 


ALTER TABLE data_vault.Link_ticket_flights_boarding_passes ADD CONSTRAINT Hub_bp_fk FOREIGN KEY (Hub_boarding_passes_Hash_key) REFERENCES data_vault.Hub_boarding_passes(hash_key); 

ALTER TABLE data_vault.Link_ticket_flights_boarding_passes ADD CONSTRAINT Hub_tf_fk FOREIGN KEY (Hub_ticket_flights_Hash_key) REFERENCES data_vault.Hub_ticket_flights(hash_key);   
/*обработка ticket_flights_boarding_passes завершена*/


/*################################
обработка - ticket_flights_tickets
##################################*/

/*обработка - обновляем данные в таблице, если в одном из источников данные не сгенирились, то вставляем их из другого источника*/    
DROP TABLE IF EXISTS data_vault.for_lfada_update_time;

-- проверка на новые данные , если их нет то добавляем 
CREATE TABLE IF NOT EXISTS data_vault.for_lfada_update_time as
SELECT * 
FROM data_vault.Link_ticket_flights_tickets ltft
                                            FULL JOIN (SELECT concat(concat(tf.ticket_no, '&', tf.flight_id), t.ticket_no) link,
                                                              md5(concat(tf.ticket_no, '&', tf.flight_id)) new_hub_ticket_flights_hash_key, 
                                                              md5(t.ticket_no) T_ticket_no,
                                                              t.ticket_no tticket_no,
                                                              md5(tf.ticket_no) TF_ticket_no,
                                                              tf.ticket_no tfticket_no,
                                                              md5(tf.flight_id :: varchar) flight_id,
                                                              tf.flight_id tflight_id
                                                       FROM stage.ticket_flights tf  
                                                             full JOIN stage.tickets t
                                                             ON t.ticket_no = tf.ticket_no) we
ON we.link = ltft.Link_ticket_flights_tickets
WHERE link_ticket_flights_tickets IS NULL;

-- SELECT * FROM data_vault.for_lfada_update_time;

-- вставка данных в таблицу линк из таблиц источников, если их там нет
INSERT INTO data_vault.Link_ticket_flights_tickets (Link_ticket_flights_tickets, load_date, record_sourse, Hub_ticket_flights_Hash_key, Hub_tickets_Hash_key)
SELECT concat(COALESCE(tticket_no, tfticket_no), '&', CASE WHEN tflight_id :: varchar IS NULL THEN 'нет flight_id в ticket_flights' ELSE tflight_id :: varchar END, COALESCE(tticket_no, tfticket_no)),
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       'ticket_flights_tickets',
       CASE WHEN length(new_hub_ticket_flights_hash_key) > 2 
            THEN new_hub_ticket_flights_hash_key 
            ELSE concat(COALESCE(t_ticket_no, tf_ticket_no), '&', CASE WHEN flight_id :: varchar IS NULL THEN 'нет flight_id в ticket_flights' ELSE flight_id :: varchar END)
            END,
       COALESCE(t_ticket_no, tf_ticket_no)           
FROM data_vault.for_lfada_update_time;

/*обработка ticket_flights_tickets завершена*/


/*#########################
обработка - ticket_bookings
##########################*/

-- если клиент сдал билет, следовательно запись в таблице удалится, а значит необходимо сделать пометку времени
UPDATE data_vault.Link_tickets_bookings a
SET load_end_date = to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp
FROM  (WITH 
          new_data AS (
              SELECT concat(ticket_no, book_ref) Link_tickets_bookings ,
                     ticket_no,
                     book_ref
              FROM stage.tickets t
                         FULL JOIN stage.bookings b USING(book_ref))
       SELECT ltb.Link_tickets_bookings
       FROM data_vault.Link_tickets_bookings ltb
                         left JOIN new_data nd USING (Link_tickets_bookings)
       WHERE nd.Link_tickets_bookings IS NULL) b
WHERE a.link_tickets_bookings = b.Link_tickets_bookings;
            
-- добавляем новые данные из источника в таблицу линк
INSERT INTO data_vault.Link_tickets_bookings (Link_tickets_bookings, load_date, load_end_date, record_sourse, Hub_ticket_Hash_key, Hub_bookings_Hash_key)
WITH new_data AS (
                  SELECT concat(ticket_no, book_ref) Link_tickets_bookings ,
                         ticket_no,
                         book_ref
                  FROM stage.tickets t
                                FULL JOIN stage.bookings b USING(book_ref))                               
SELECT nd.Link_tickets_bookings,
       to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
       '1111-11-11 11:11:11' :: timestamp,
       'ticket_bookings',
       md5(nd.ticket_no),
       md5(nd.book_ref)
FROM data_vault.Link_tickets_bookings ltb
                         RIGHT JOIN new_data nd USING (Link_tickets_bookings)
WHERE load_date IS NULL;

/*обработка ticket_bookings завершена*/


/*################################
обработка - flights_aircrafts_data
#################################*/

-- при добавлении новых данных удаляем привязку к хабам, так как обработка идет без хеширования, после обработки привязка будет восстановлена 
ALTER TABLE data_vault.Link_flights_aircrafts_data DROP CONSTRAINT IF EXISTS Hub_f_fk;
ALTER TABLE data_vault.Link_flights_aircrafts_data DROP CONSTRAINT IF EXISTS Hub_ad_fk;

-- Насыщение таблицы новыми данными из источников, предусмотрен момент если данные в одном источнике есть, а в другом не подгрузились
INSERT INTO data_vault.Link_flights_aircrafts_data (Link_flights_aircrafts_data_Hashkey, load_date, record_sourse, Hub_flights_Hash_key, Hub_aircrafts_data_Hash_key)           
WITH 
new_data AS (
             SELECT DISTINCT 
                    concat(f.aircraft_code, ad.aircraft_code) link_flights_aircrafts_data_hashkey,
                    to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
                    'flights_aircrafts_data',
                    f.aircraft_code AS f_aircraft_code,
                    ad.aircraft_code AS ad_aircraft_code
             FROM stage.flights f  
                 FULL JOIN stage.aircrafts_data ad 
                 ON f.aircraft_code = ad.aircraft_code)               
SELECT CASE WHEN length(lfad.link_flights_aircrafts_data_hashkey) < 5 OR -- тк коды должны быть идентичны, если нет данных в одной таблице , то подтягиваем их из другой
                 length(nd.link_flights_aircrafts_data_hashkey) < 5 THEN concat(COALESCE(f_aircraft_code, ad_aircraft_code), COALESCE(f_aircraft_code, ad_aircraft_code)) 
                 ELSE COALESCE(nd.link_flights_aircrafts_data_hashkey, lfad.link_flights_aircrafts_data_hashkey) 
                 END,
                 to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
                 'flights_aircrafts_data',
                 COALESCE(f_aircraft_code, ad_aircraft_code),
                 COALESCE(f_aircraft_code, ad_aircraft_code)
FROM data_vault.Link_flights_aircrafts_data lfad
                       RIGHT JOIN new_data nd ON lfad.link_flights_aircrafts_data_hashkey = nd.link_flights_aircrafts_data_hashkey
WHERE lfad.link_flights_aircrafts_data_hashkey IS NULL;
    

-- хеш состоит из 32 символов , обновляем данные у которых длинна символов меньше 30, т.е. при последующих обработках захешированные данные, которые уже лежат в линке не будут подвержены переводу в формат md5. Обработка идет без хеширования, для перевода данных в формат md5 мы берем только те данные, у которых длина строки менее 30, т.е. новые данные
UPDATE data_vault.Link_flights_aircrafts_data a
      SET (Hub_flights_Hash_key, 
           Hub_aircrafts_data_Hash_key) = (md5(b.Hub_flights_Hash_key), md5(b.Hub_aircrafts_data_Hash_key))
      FROM data_vault.Link_flights_aircrafts_data b                                       
      WHERE (a.Link_flights_aircrafts_data_Hashkey = b.Link_flights_aircrafts_data_Hashkey)
          AND (length(Hub_flights_Hash_key) < 30 OR (length(Hub_aircrafts_data_Hash_key) < 30)); 

-- восстанавливаем привязку таблицы к новым данным
ALTER TABLE data_vault.Link_flights_aircrafts_data ADD CONSTRAINT Hub_f_fk FOREIGN KEY (Hub_flights_Hash_key) REFERENCES data_vault.Hub_flights(hash_key); 
ALTER TABLE data_vault.Link_flights_aircrafts_data ADD CONSTRAINT Hub_ad_fk FOREIGN KEY (Hub_aircrafts_data_Hash_key) REFERENCES data_vault.Hub_aircrafts_data(hash_key);   

/*обработка flights_aircrafts_data завершена*/


/*##############################
обработка - seats_aircrafts_data
###############################*/

-- при добавлении новых данных удаляем привязку к хабам, так как обработка идет без хеширования, после обработки привязка будет восстановлена 
ALTER TABLE data_vault.Link_seats_aircrafts_data DROP CONSTRAINT IF EXISTS Hub_ad_fk;
ALTER TABLE data_vault.Link_seats_aircrafts_data DROP CONSTRAINT IF EXISTS Hub_s_fk;            
            
-- Насыщение таблицы новыми данными из источников, предусмотрен момент если данные в одном источнике есть, а в другом не подгрузились
INSERT INTO data_vault.Link_seats_aircrafts_data(Link_seats_aircrafts_data_Hashkey, load_date, record_sourse, Hub_aircrafts_data_Hash_key, Hub_seats_Hash_key)
WITH 
new_data AS (
             SELECT DISTINCT 
                    concat(ad.aircraft_code, concat(s.aircraft_code, s.seat_no)) Link_seats_aircrafts_data_Hashkey,
                    ad.aircraft_code AS f_aircraft_code,
                    concat(s.aircraft_code, s.seat_no) AS ad_aircraft_code
             FROM stage.aircrafts_data ad 
                     FULL JOIN stage.seats s USING(aircraft_code))

SELECT 
     CASE 
	     WHEN length(nd.link_seats_aircrafts_data_hashkey) < 6 
	     THEN concat(COALESCE(f_aircraft_code, ad_aircraft_code), COALESCE(f_aircraft_code, ad_aircraft_code)) 
	          ELSE COALESCE(nd.Link_seats_aircrafts_data_Hashkey, lsad.Link_seats_aircrafts_data_Hashkey) END ,
     to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp,
     'seats_aircrafts_data',
     COALESCE(f_aircraft_code, ad_aircraft_code),
     COALESCE(f_aircraft_code, ad_aircraft_code)
     
FROM data_vault.Link_seats_aircrafts_data lsad
                      RIGHT JOIN new_data nd ON lsad.Link_seats_aircrafts_data_Hashkey = nd.Link_seats_aircrafts_data_Hashkey
WHERE load_date IS null;

-- хеш состоит из 32 символов , обновляем данные у которых длинна символов меньше 30, т.е. при последующих обработках захешированные данные, которые уже лежат в линке не будут подвержены переводу в формат md5. Обработка идет без хеширования, для перевода данных в формат md5 мы берем только те данные, у которых длина строки менее 30, т.е. новые данные
UPDATE data_vault.Link_seats_aircrafts_data a
      SET (Hub_aircrafts_data_Hash_key, 
           Hub_seats_Hash_key) = (md5(b.Hub_aircrafts_data_Hash_key), md5(b.Hub_seats_Hash_key))
      FROM data_vault.Link_seats_aircrafts_data b                                       
      WHERE (a.Link_seats_aircrafts_data_Hashkey = b.Link_seats_aircrafts_data_Hashkey)
          AND (length(a.Hub_seats_Hash_key) < 30 OR (length(a.Hub_aircrafts_data_Hash_key) < 30)); 

-- восстанавливаем привязку таблицы к новым данным
ALTER TABLE data_vault.Link_seats_aircrafts_data ADD CONSTRAINT Hub_ad_fk FOREIGN KEY (Hub_aircrafts_data_Hash_key) REFERENCES data_vault.Hub_aircrafts_data(hash_key); 
ALTER TABLE data_vault.Link_seats_aircrafts_data ADD CONSTRAINT Hub_s_fk FOREIGN KEY (Hub_seats_Hash_key) REFERENCES data_vault.Hub_seats(hash_key);   
```

Хабы и линки обработаны, остались саттелиты.
Для обработки саттелитов была написана функция.

```
CREATE OR REPLACE FUNCTION data_vault.update_data_from_table(      
               table_name TEXT , -- название таблицы саттелита
               source_table_columns TEXT, -- какие колонки надо вытащить из таблицы источника
               source_table_name TEXT, -- имя таблицы откуда тянутся данные
               hash TEXT, -- из чего будет состоять хеш
               times TEXT, -- время записи данных в таблицу
               sourse_col TEXT) -- название источника
               RETURNS VOID AS $$
DECLARE 
     qwery TEXT;
BEGIN -- проверка на наличие информации в параметрах
	IF 
	  (table_name IS NULL) OR (source_table_columns IS NULL) OR (source_table_name IS NULL) OR (hash IS NULL) OR (times IS NULL) OR (sourse_col IS NULL)
		THEN RAISE EXCEPTION 'Не заполнены параметры';
	END IF;
       -- добавление только новых записей из источника
qwery:= 'INSERT INTO ' || table_name || ' (' || (regexp_split_to_array(source_table_columns, ','))[1] || ', load_date' || ' ,load_end_date' || ' ,record_sourse , '  || (regexp_split_to_array(source_table_columns, ','))[2] || ') ' ||
         'WITH new_data as (SELECT DISTINCT md5(' || hash || ') ' || (regexp_split_to_array(source_table_columns, ','))[1] || ' ,' || (regexp_split_to_array(source_table_columns, ','))[2] || ' FROM ' || source_table_name || ') '  ||
         'SELECT ' || 'nd.' || (regexp_split_to_array(source_table_columns, ','))[1] || ', ' || times || ', ' || '''1111-11-11 11:11:11'':: timestamp' || ' , ' || sourse_col || ' , ' || 'nd.' || (regexp_split_to_array(source_table_columns, ','))[2] ||
         ' FROM ' || table_name || ' tn ' || 'RIGHT JOIN new_data nd ON (' || 'tn.' || (regexp_split_to_array(source_table_columns, ','))[1] || ') = (' || 'nd.' || (regexp_split_to_array(source_table_columns, ','))[1] || ')' || 
         ' WHERE tn.load_date IS NULL;' || 
       -- обновление значений и показателя Load_end_date уже имеющихся аттрибутов, при изменении аттрибута в источнике
         ' DROP TABLE IF EXISTS data_vault.for_lfada_update_time; ' || 
         ' CREATE TABLE IF NOT EXISTS data_vault.for_lfada_update_time as
                  WITH 
                      new_data AS (SELECT DISTINCT 
                                          md5(' || hash || ') ' || (regexp_split_to_array(source_table_columns, ','))[1] || ' ,' ||
                                          (regexp_split_to_array(source_table_columns, ','))[2] ||
                                   ' FROM ' || source_table_name || ' ) ' || 
                   ', need_swap AS (SELECT nd.' || (regexp_split_to_array(source_table_columns, ','))[1] || ' ,' ||
                                          ' nd.' || (regexp_split_to_array(source_table_columns, ','))[2] || 
                                    ' FROM ' || table_name || ' tn ' || 
                                              'RIGHT JOIN new_data nd ON (concat(tn.' || (regexp_split_to_array(source_table_columns, ','))[1] || ' , tn.'  || (regexp_split_to_array(source_table_columns, ','))[2] || ')) = (concat(nd.' || (regexp_split_to_array(source_table_columns, ','))[1] || ' , nd.' || (regexp_split_to_array(source_table_columns, ','))[2] || ')) ' || 
                                    ' WHERE tn.load_date IS NULL)' || 
                   ' SELECT ' || (regexp_split_to_array(source_table_columns, ','))[1] || ' , ' || 
                             ' load_date, load_end_date, record_sourse, need_swap.' || (regexp_split_to_array(source_table_columns, ','))[2] || 
                   ' FROM ' || table_name || ' nd' || 
                                  ' JOIN need_swap USING(' || (regexp_split_to_array(source_table_columns, ','))[1] || '); ' || 
       -- обновление поля смены даты load_end_date 
       ' UPDATE ' || table_name || ' firsts ' || 
       ' SET load_end_date = ' || times || 
       ' FROM data_vault.for_lfada_update_time flut ' || 
       ' WHERE flut.' || (regexp_split_to_array(source_table_columns, ','))[1] || ' = firsts.' || (regexp_split_to_array(source_table_columns, ','))[1] ||' ;'
       
       -- добавление новой записи 
       ' INSERT INTO ' || table_name || ' (' || (regexp_split_to_array(source_table_columns, ','))[1] || ', load_date' || ' ,load_end_date' || ' ,record_sourse , '  || (regexp_split_to_array(source_table_columns, ','))[2] || ') ' || 
       ' SELECT ' || (regexp_split_to_array(source_table_columns, ','))[1] || ' , ' ||
                  times || ',' || ' load_end_date, record_sourse, ' || (regexp_split_to_array(source_table_columns, ','))[2] || ' FROM data_vault.for_lfada_update_time;';
    EXECUTE qwery;
    -- RETURN qwery;
END;
$$ LANGUAGE plpgsql;
```
Обозначим обработку саттелитов.
```                                      
-- Sattelite_aircrafts_data
--#####################################################################################################
/*Аircrafts_data_range*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_aircrafts_data_range' , -- название таблицы саттелита
                                          'Hub_aircrafts_data_Hash_key,range',         -- какие колонки надо вытащить из таблицы источника
                                          'stage.aircrafts_data',                      -- имя таблицы откуда тянутся данные
                                          'aircraft_code::varchar',                    -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''aircrafts_data''');
-- SELECT * FROM data_vault.Sattelite_aircrafts_data_range;

/*Аircrafts_data_model*/                                         
SELECT data_vault.update_data_from_table('data_vault.Sattelite_aircrafts_data_model' , -- название таблицы саттелита
                                          'Hub_aircrafts_data_Hash_key,model',         -- какие колонки надо вытащить из таблицы источника
                                          'stage.aircrafts_data',                      -- имя таблицы откуда тянутся данные
                                          'aircraft_code::varchar',                    -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''aircrafts_data''');
-- SELECT * FROM data_vault.Sattelite_aircrafts_data_model;                                         

  
-- Sattelite_seats
--#####################################################################################################
/*fare_conditions*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_seats_fare_conditions' ,-- название таблицы саттелита
                                          'Hub_seats_Hash_key,fare_conditions',        -- какие колонки надо вытащить из таблицы источника
                                          'stage.seats',                               -- имя таблицы откуда тянутся данные
                                          'concat(aircraft_code, seat_no)',            -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''seats''');
-- SELECT * FROM data_vault.Sattelite_seats_fare_conditions;

                                         
-- Sattelite_airoport_data
--#####################################################################################################
 /*timezone*/       
SELECT data_vault.update_data_from_table('data_vault.Sattelite_airport_data_timezone' ,-- название таблицы саттелита
                                          'Hub_airport_data_Hash_key,timezone',        -- какие колонки надо вытащить из таблицы источника
                                          'stage.airports_data',                      -- имя таблицы откуда тянутся данные
                                          'airport_code::varchar',                     -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''airoport_data''');
                                         
-- SELECT * FROM data_vault.Sattelite_airport_data_timezone;
 
 /*coordinates*/                                           
SELECT data_vault.update_data_from_table('data_vault.Sattelite_airport_data_coordinates' , -- название таблицы саттелита
                                          'Hub_airport_data_Hash_key,coordinates',         -- какие колонки надо вытащить из таблицы источника
                                          'stage.airports_data',                           -- имя таблицы откуда тянутся данные
                                          'airport_code::varchar',                         -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''airoport_data''');
-- SELECT * FROM data_vault.Sattelite_airport_data_coordinates;                                  

 /*airport_name*/  
SELECT data_vault.update_data_from_table('data_vault.Sattelite_airport_data_airport_name' , -- название таблицы саттелита
                                          'Hub_airport_data_Hash_key,airport_name',         -- какие колонки надо вытащить из таблицы источника
                                          'stage.airports_data',                           -- имя таблицы откуда тянутся данные
                                          'airport_code::varchar',                         -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''airoport_data''');
-- SELECT * FROM data_vault.Sattelite_airport_data_airport_name;                                   
                                         
 /*city*/  
SELECT data_vault.update_data_from_table('data_vault.Sattelite_airport_data_city' , -- название таблицы саттелита
                                          'Hub_airport_data_Hash_key,city',         -- какие колонки надо вытащить из таблицы источника
                                          'stage.airports_data',                           -- имя таблицы откуда тянутся данные
                                          'airport_code::varchar',                         -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, --время залития
                                          '''airoport_data''');
-- SELECT * FROM data_vault.Sattelite_airport_data_city;      
                                         

-- Sattelite_ticket_flights
--#####################################################################################################
 /*amount*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_ticket_flights_amount' ,             -- название таблицы саттелита
                                          'Hub_ticket_flights_Hash_key,amount',                     -- какие колонки надо вытащить из таблицы источника
                                          'stage.ticket_flights',                                   -- имя таблицы откуда тянутся данные
                                          $$(concat(ticket_no, '&', flight_id))::varchar$$,         -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$, -- время залития
                                          '''airoport_data''');
-- SELECT * FROM data_vault.Sattelite_ticket_flights_amount;  
                                         
 /*fare_conditions*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_ticket_flights_fare_conditions' ,    -- название таблицы саттелита
                                          'Hub_ticket_flights_Hash_key,fare_conditions',            -- какие колонки надо вытащить из таблицы источника
                                          'stage.ticket_flights',                                   -- имя таблицы откуда тянутся данные
                                          $$(concat(ticket_no, '&', flight_id))::varchar$$,         -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''airoport_data''');
-- SELECT * FROM data_vault.Sattelite_ticket_flights_fare_conditions; 
                                         

-- Sattelite_boarding_passes
--#####################################################################################################
 /*boarding_no*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_boarding_passes_boarding_no' ,    -- название таблицы саттелита
                                          'Hub_boarding_passes_Hash_key,boarding_no',            -- какие колонки надо вытащить из таблицы источника
                                          'stage.boarding_passes',                                   -- имя таблицы откуда тянутся данные
                                          $$(concat(ticket_no, '&', flight_id))::varchar$$,         -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''boarding_passes''');
-- SELECT * FROM data_vault.Sattelite_boarding_passes_boarding_no; 
                                         
                                         
 /*seat_no*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_boarding_passes_seat_no' ,    -- название таблицы саттелита
                                          'Hub_boarding_passes_Hash_key,seat_no',            -- какие колонки надо вытащить из таблицы источника
                                          'stage.boarding_passes',                           -- имя таблицы откуда тянутся данные
                                          $$(concat(ticket_no, '&', flight_id))::varchar$$,  -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''boarding_passes''');
-- SELECT * FROM data_vault.Sattelite_boarding_passes_seat_no;      
            
                                         
-- Sattelite_tickets
--#####################################################################################################
 /*book_ref*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_tickets_book_ref' ,  -- название таблицы саттелита
                                          'Hub_ticket_Hash_key,book_ref',           -- какие колонки надо вытащить из таблицы источника
                                          'stage.tickets',                          -- имя таблицы откуда тянутся данные
                                          $$(ticket_no)::varchar$$,                 -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''tickets''');
-- SELECT * FROM data_vault.Sattelite_tickets_book_ref;  
                                         
 /*contact_data*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_tickets_contact_data' ,  -- название таблицы саттелита
                                          'Hub_ticket_Hash_key,contact_data',           -- какие колонки надо вытащить из таблицы источника
                                          'stage.tickets',                          -- имя таблицы откуда тянутся данные
                                          $$(ticket_no)::varchar$$,                 -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''tickets''');
-- SELECT * FROM data_vault.Sattelite_tickets_contact_data;  
                                         
                                         
 /*passenger_id*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_tickets_passenger_id' ,  -- название таблицы саттелита
                                          'Hub_ticket_Hash_key,passenger_id',           -- какие колонки надо вытащить из таблицы источника
                                          'stage.tickets',                          -- имя таблицы откуда тянутся данные
                                          $$(ticket_no)::varchar$$,                 -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''tickets''');
-- SELECT * FROM data_vault.Sattelite_tickets_passenger_id;                                     
                                         
                                         
 /*passenger_name*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_tickets_passenger_name' ,  -- название таблицы саттелита
                                          'Hub_ticket_Hash_key,passenger_name',           -- какие колонки надо вытащить из таблицы источника
                                          'stage.tickets',                          -- имя таблицы откуда тянутся данные
                                          $$(ticket_no)::varchar$$,                 -- хеш
                                         $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''tickets''');
-- SELECT * FROM data_vault.Sattelite_tickets_passenger_name;  
                                         
                                         
-- Sattelite_bookings
--#####################################################################################################
 /*book_date*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_bookings_book_date' ,  -- название таблицы саттелита
                                          'Hub_bookings_Hash_key,book_date',           -- какие колонки надо вытащить из таблицы источника
                                          'stage.bookings',                          -- имя таблицы откуда тянутся данные
                                          $$(book_ref)::varchar$$,                 -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''bookings''');
-- SELECT * FROM data_vault.Sattelite_bookings_book_date;
                                         
 /*total_amount*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_bookings_total_amount' ,  -- название таблицы саттелита
                                          'Hub_bookings_Hash_key,total_amount',           -- какие колонки надо вытащить из таблицы источника
                                          'stage.bookings',                          -- имя таблицы откуда тянутся данные
                                          $$(book_ref)::varchar$$,                 -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''bookings''');
-- SELECT * FROM data_vault.Sattelite_bookings_total_amount;
                                         
                                         
-- Sattelite_flights
--#####################################################################################################
 /*flight_no*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_flights_flight_no' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,flight_no',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                          -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                 -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_flight_no;

 /*scheduled_departure*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_flights_scheduled_departure' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,scheduled_departure',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                     -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                              -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_scheduled_departure;
                                         
 /*scheduled_arrival*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_flights_scheduled_arrival' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,scheduled_arrival',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_scheduled_arrival;
                                         
 /*departure_airport*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_flights_departure_airport' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,departure_airport',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_departure_airport;
                                         
 /*arrival_airport*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_flights_arrival_airport' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,arrival_airport',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_arrival_airport;
                                         
 /*status*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_flights_status' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,status',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_status;

 /*aircraft_code*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_flights_aircraft_code' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,aircraft_code',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_aircraft_code;
                                         
 /*actual_departure*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_flights_actual_departure' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,actual_departure',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_actual_departure;

 /*actual_arrival*/
SELECT data_vault.update_data_from_table('data_vault.Sattelite_flights_actual_arrival' ,  -- название таблицы саттелита
                                          'Hub_flights_Hash_key,actual_arrival',          -- какие колонки надо вытащить из таблицы источника
                                          'stage.flights',                                   -- имя таблицы откуда тянутся данные
                                          $$flight_id::varchar$$,                            -- хеш
                                          $$to_char(now(), 'YYYY-MM-DD HH24:MI:SS') :: timestamp$$,  -- время залития
                                          '''flights''');
-- SELECT * FROM data_vault.Sattelite_flights_actual_arrival;
```

На этом этапе можно четко сказать, что у нас построена [схема модели данных DataVault](https://drive.google.com/file/d/1VcN5-KVAizzSl9FWSyAXQiV4Z_Ublo8e/view) составленая в тулзе гугла. Наполнены хабы линки и саттелиты и прописан код к их обработке. Можно строить витрины!