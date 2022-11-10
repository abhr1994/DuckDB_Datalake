from duckpond import SQL, DuckDB, DuckPondIOManager
import pandas as pd
from prefect import flow, task

duckdb_obj = DuckDB(options="""set s3_access_key_id='test';
set s3_secret_access_key='test';
set s3_endpoint='localhost:4566';
set s3_use_ssl='false';
set s3_url_style='path';
""")
dpio_obj = DuckPondIOManager(bucket_name="datalake", duckdb=duckdb_obj, prefix="test_env")


@task(retries=3)
def stg_customers() -> SQL:
    df = pd.read_csv(
        "https://raw.githubusercontent.com/abhr1994/DuckDB_Datalake/main/data/raw_customers.csv"
    )
    df.rename(columns={"id": "customer_id"}, inplace=True)
    return_sql = SQL("select * from $df", df=df)
    print(duckdb_obj.query(return_sql))
    dpio_obj.handle_output(table_name="stg_customers", select_statement=return_sql)
    return return_sql


@task(retries=3)
def stg_orders() -> SQL:
    df = pd.read_csv(
        "https://raw.githubusercontent.com/abhr1994/DuckDB_Datalake/main/data/raw_orders.csv"
    )
    df.rename(columns={"id": "order_id", "user_id": "customer_id"}, inplace=True)
    return_sql = SQL("select * from $df", df=df)
    print(duckdb_obj.query(return_sql))
    dpio_obj.handle_output(table_name="stg_orders", select_statement=return_sql)
    return return_sql


@task(retries=3)
def stg_payments() -> SQL:
    df = pd.read_csv(
        "https://raw.githubusercontent.com/abhr1994/DuckDB_Datalake/main/data/raw_payments.csv"
    )
    df.rename(columns={"id": "payment_id"}, inplace=True)
    df["amount"] = df["amount"].map(lambda amount: amount / 100)
    return_sql = SQL("select * from $df", df=df)
    print(duckdb_obj.query(return_sql))
    dpio_obj.handle_output(table_name="stg_payments", select_statement=return_sql)
    return return_sql


@task(retries=3)
def customers(stg_customers: SQL, stg_orders: SQL, stg_payments: SQL) -> SQL:
    return_sql = SQL(
        """
with customers as (
    select * from $stg_customers
),
orders as (
    select * from $stg_orders
),
payments as (
    select * from $stg_payments
),
customer_orders as (
    select
        customer_id,
        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders
    from orders
    group by customer_id
),
customer_payments as (
    select
        orders.customer_id,
        sum(amount) as total_amount
    from payments
    left join orders on
         payments.order_id = orders.order_id
    group by orders.customer_id
),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        customer_payments.total_amount as customer_lifetime_value
    from customers
    left join customer_orders
        on customers.customer_id = customer_orders.customer_id
    left join customer_payments
        on  customers.customer_id = customer_payments.customer_id
)
select * from final
    """,
        stg_customers=stg_customers,
        stg_orders=stg_orders,
        stg_payments=stg_payments,
    )
    print(duckdb_obj.query(return_sql))
    dpio_obj.handle_output(table_name="customers", select_statement=return_sql)
    return return_sql


@task(retries=3)
def orders(stg_orders: SQL, stg_payments: SQL) -> SQL:
    payment_methods = ["credit_card", "coupon", "bank_transfer", "gift_card"]
    return_sql = SQL(
        f"""
with orders as (
    select * from $stg_orders
),
payments as (
    select * from $stg_payments
),
order_payments as (
    select
        order_id,
        {"".join(f"sum(case when payment_method = '{payment_method}' then amount else 0 end) as {payment_method}_amount," for payment_method in payment_methods)}
        sum(amount) as total_amount
    from payments
    group by order_id
),
final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,
        {"".join(f"order_payments.{payment_method}_amount," for payment_method in payment_methods)}
        order_payments.total_amount as amount
    from orders
    left join order_payments
        on orders.order_id = order_payments.order_id
)
select * from final
    """,
        stg_orders=stg_orders,
        stg_payments=stg_payments,
    )
    print(duckdb_obj.query(return_sql))
    dpio_obj.handle_output(table_name="orders", select_statement=return_sql)
    return return_sql


@flow(name="ETL DuckDB")
def ETL_FLOW():
    p = stg_customers.submit()
    q = stg_orders.submit()
    r = stg_payments.submit()
    customers.submit(p, q, r)
    orders.submit(q, r)


ETL_FLOW()
