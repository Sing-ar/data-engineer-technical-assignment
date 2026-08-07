import sqlite3
import pandas as pd
import structlog, time, os
from prefect import flow, task

log = structlog.get_logger()


# views = pd.read_sql(""" SELECT name FROM sqlite_master WHERE type = 'view'; """, conn)
# print(views)


@task(retries = 3, retry_delay_seconds = 10)
def extract_view() -> tuple:
    log.info("Extract")

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

    customers["signup_date"] = pd.to_datetime(customers["signup_date"])

    customers = (customers.sort_values("signup_date", ascending = False)
                          .drop_duplicates(subset = ["customer_id"], keep = "first")
                          .reset_index(drop = True)
                )

    customers["email"] = customers["email"].fillna("unknown@domain.com")

    customers["phone"] = customers["phone"].str.replace(r"\D", "", regex = True)

    orders = (orders[orders["total_amount"] > 0].reset_index(drop=True))

    orders["order_date"] = pd.to_datetime(orders["order_date"]).dt.date
    exchange_rate["date"] = pd.to_datetime(exchange_rate["date"]).dt.date
    
    orders = orders.merge(exchange_rate, 
                          how = 'left', 
                          left_on = ["currency", "order_date"],
                          right_on = ["currency", "date"]
                    )

    orders["rate_to_usd"] = orders["rate_to_usd"].fillna(1)

    orders["total_amount_usd"] = (orders["total_amount"] * orders["rate_to_usd"]).round(2)

    return customers, orders


@task(retries = 3, retry_delay_seconds = 10)
def load_to_csv(customers, orders):
    
    log.info("Load Data")

    customers.to_csv("clean_customers.csv", index = False)
    orders.to_csv("clean_orders.csv", index = False)

    return "clean_customers.csv", "clean_orders.csv"


@flow(name = "pipeline")
def run_pipeline():
    
    try:
        customers, orders, exchange_rate = extract_view()
        customers, orders = transform(customers, orders, exchange_rate)
        load_to_csv(customers, orders)

        log.info('Pipeline Load Success')

    except Exception as e:
        log.error('Pipeline Failed', error = str(e))
        raise

if __name__ == '__main__':
    run_pipeline()
