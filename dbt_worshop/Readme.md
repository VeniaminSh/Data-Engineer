# Практика с DBT

1. Необходимо развернуть бд (в примере используется облачное решение от ВК клауд) и получить внешний ip адрес , ну и данные для подключения к бд .

2. Подкючиться к бд через dbeaver.
    Создадим три схемы:
     * create schema if not exists jaffle_shop;

     * create schema if not exists stripe;

     * create schema if not exists dev; (схема для разработки)

     Создали три таблицы в схемах:
     * create table jaffle_shop.customers(
        id integer,
        first_name varchar(50),
        last_name varchar(50));

     * create table jaffle_shop.orders(
        id integer,
        user_id integer,
        order_date date,
        status varchar(50)
        );

     * create table stripe.payment(
        id integer,
        orderid integer,
        paymentmethod varchar(50),
        status varchar(50),
        amount integer,
        created date
        );

И добавим в них данные из папки scr.


3. Создаем и активируем оружение

```
python -m venv venv

source venv/Scripts/activate
```

4. Грузим
* библиотеки для DBT и адаптер для Postgres, а затем делаем проверку
* пакет pre-commit
* файл со всем предустановленными библиотеками и их версиями

```
python -m pip install dbt-core dbt-postgres

dbt --version

python -m pip install pre-commit

pip freeze -l > requirements.txt
```

5. Делаем инициализацию dbt проекта (прописываем запрашиваемые данные) и запускаем dbt

```
dbt init

dbtworkshop  - название

dbt run
```
* в гит игнор добавить venv

6. Удалим из папки model папку example.
в папке model создадим две папки mart и staging <- создадим еще две папки stripe и jaffle_shop <- в нее добавим скрипт из scr. После изменить файл dbt_project.yml
```
models:
  dbtworkshop:
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view
```
сюда добавить еще одну строку

```
models:
  dbtworkshop:
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view
    staging:
      +materialized: view
      +tags: staging
```

7. Выполним модель
```
dbt run --select customer_sales
```

8. Удалим из папки model/jaffle_shop файл customer_sales.sql и добавим новый файл (model/jaffle_shop) _jaffle_shop_sources.yml
Наполним его данными - это описание где хранится, то к чему будет обращаться скрипт в БД
```
version: 2

sources:
  - name: jaffle_shop
    description: Jaffle Shop
    loader: Manual
    database: PostgreSQL-9482
    schema: jaffle_shop
    tables:
      - name: customers
      - name: orders

```

так же создадим сам скрипт stg_jaffle_shop__customer в этой же папке и наполним его данными
```
select
     id as customer_id,
     first_name,
     last_name
from {{ source('jaffle_shop', 'customers') }}
```

и еще один сркипт stg_jaffle_shop__orders
```
select
     id as customer_id,
     first_name,
     last_name
from {{ source('jaffle_shop', 'orders') }}
```

9. Убрать из dbt_project.yml информацию
```
example:
      +materialized: view
```

10. Добавим ямл файл теперь для схемы stripe
(model/stripe)
```
version: 2

sources:
  - name: stripe
    database: PostgreSQL-9482
    schema: stripe
    tables:
      - name: payment
```

и добавить sql файл для схемы stripe
```
select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    -- amount is stored in cents, convert it to dollars
    amount / 100 as amount,
    created as created_at
from {{ source('stripe', 'payment') }}
```

11. Мы создали три модели, но к ним необходимо добавить документацию (c названием модели, описанием модели и тестом)

* stg_jaffle_shop__customer.yml
```
version: 2

models:
  - name: stg_jaffle_shop__customers
    description: This model cleans up customer data
    columns:
      - name: customer_id
        description: Primary key
        data_tests:
          - unique
          - not_null
```

* stg_jaffle_shop__orders.yml
```
version: 2

models:
  - name: stg_jaffle_shop__orders
    description: This model cleans up order data
    columns:
      - name: order_id
        description: Primary key
        data_tests:
        - unique
        - not_null
      - name: status
        tests:
          - accepted_values:
              values: ['placed', 'shipped', 'completed', 'return_pending', 'returned']
      - name: customer_id
        data_tests:
          - not_null
          - relationships:
              to: ref('stg_jaffle_shop__customers')
              field: customer_id
```

* stg_stripe__payment.yml
```
version: 2

models:
  - name: stg_stripe__payments
    description: Stripe payments
    columns:
      - name: payment_id
        description: Primary key
        data_tests:
          - unique
          - not_null
```

12. В папке dbt_worshop\tests создадим тест для таблицы payments
```
select
    order_id,
    sum(amount) as total_amount
from {{ ref('stg_stripe__payments') }}
group by 1
having sum(amount) > 0
```

13. Вызовем все записи
```
dbt run --select tag:staging Мы можем запускать модели

dbt test --select tag:staging Мы можем запускать тесты
```

14. Добавим тесты свежести (если разница между текущим моментом и максимальной датой order_date то вылетит варнинг, если 24 то это ошибка и пайплайн упадет)

код добавляется в сорс схемы models\staging\jaffle_shop\_jaffle_shop_sources.yml
```
version: 2

sources:
  - name: jaffle_shop
    description: Jaffle Shop
    loader: Manual
    database: PostgreSQL-1718
    schema: jaffle_shop
    tables:
      - name: customers
      - name: orders
        loaded_at_field: order_date
        freshness:
           warn_after: {count: 12, period: hour}
           error_after: {count: 24, period: hour}
```

запуск теста свежести
```
dbt source freshness
```

15. Установим pre-commit
```
 pip install pre-commit
```

создадим в файл ".pre-commit-config.yaml" в папке dbt_worshop\.pre-commit-config.yaml

и вставим туда
```
# Pre-commit that runs locally
fail_fast: false

repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.6.0
    hooks:
      - id: trailing-whitespace
      - id: check-yaml

  - repo: https://github.com/pre-commit/mirrors-prettier
    rev: v3.1.0
    hooks:
      - id: prettier
        files: '\.(yaml|yml)$'

  - repo: https://github.com/psf/black
    rev: 24.8.0
    hooks:
      - id: black
        language_version: python3.11

  - repo: https://github.com/pycqa/flake8
    rev: 7.1.1
    hooks:
      - id: flake8

  - repo: https://github.com/tconbeer/sqlfmt
    rev: v0.21.0
    hooks:
      - id: sqlfmt
        files: ^(models|analyses|tests)/.*.sql$
```

далее предустановить надстройку
```
pre-commit install
```