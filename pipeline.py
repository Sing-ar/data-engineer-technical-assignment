import sqlite3
import pandas as pd
import structlog
from prefect import flow, task

log = structlog.get_logger()


@task(retries = 3, retry_delay_seconds = 10)
def extract_view():
    log.info("Extract Data")

    with sqlite3.connect("shopdata.db") as conn:

        customers = pd.read_sql(f"SELECT * FROM vw_raw_customers", conn)
        orders = pd.read_sql(f"SELECT * FROM vw_raw_orders", conn)
        exchange_rate = pd.read_sql(f"SELECT * FROM vw_exchange_rates", conn)

        return (customers,
                orders,
                exchange_rate
        )


@task
def transform(customers, orders, exchange_rate):

    log.info("Transform Data")

    customers = (customers.sort_values("signup_date", ascending = False)
                          .drop_duplicates(subset = ["customer_id"], keep = "first")
                          .reset_index(drop = True)
                )

    customers["email"] = customers["email"].fillna("unknown@domain.com")

    customers["phone"] = customers["phone"].str.replace(r"\D", "", regex = True)

    orders = orders.dropna(subset = ["order_date"])

    orders["customer_id"] = orders["customer_id"].replace(99, None)

    orders = orders.dropna(subset = ["customer_id"])

    orders = (orders[orders["total_amount"] > 0].reset_index(drop = True))
    
    orders = orders.merge(exchange_rate, 
                          how = 'left', 
                          left_on = ["currency", "order_date"],
                          right_on = ["currency", "date"]
                    )

    orders["rate_to_usd"] = orders["rate_to_usd"].fillna(1)

    orders["total_amount_usd"] = (orders["total_amount"] * orders["rate_to_usd"]).round(2)

    return customers, orders


@task(retries = 3, retry_delay_seconds = 10)
def load_to_sqlite(customers, orders):

    log.info("Load Data")

    with sqlite3.connect("analytics.db") as conn:

        customers.to_sql("dim_customers", conn, if_exists = "replace", index = False)

        orders.to_sql("fct_orders", conn, if_exists = "replace", index = False)

    log.info("analytics.db created successfully")


@flow(name = "pipeline")
def run_pipeline():
    
    try:
        customers, orders, exchange_rate = extract_view()
        customers, orders = transform(customers, orders, exchange_rate)
        load_to_sqlite(customers, orders)

        log.info("Pipeline Load Success")

    except Exception as e:
        log.error("Pipeline Failed", error = str(e))
        raise

if __name__ == '__main__':
    run_pipeline()
