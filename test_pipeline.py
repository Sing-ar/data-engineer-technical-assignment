import pandas as pd
from pipeline import transform


def test_clean_customers_duplicates():

    customers = pd.DataFrame({
        "customer_id": [1, 1, 2],
        "signup_date": [
            "2024-01-01",
            "2024-02-01",
            "2024-01-15"
        ],
        "email": [
            "a@test.com",
            "new@test.com",
            "b@test.com"
        ],
        "phone": [
            "081-111-1111",
            "081-111-1111",
            "082-222-2222"
        ]
    })

    orders = pd.DataFrame({
        "customer_id": [1],
        "order_date": ["2024-01-01"],
        "currency": ["USD"],
        "total_amount": [100]
    })

    exchange = pd.DataFrame({
        "currency": ["USD"],
        "date": ["2024-01-01"],
        "rate_to_usd": [1]
    })

    customers_result, _ = transform(customers, orders, exchange)

    assert customers_result["customer_id"].duplicated().sum() == 0


def test_clean_phone():

    customers = pd.DataFrame({
        "customer_id": [1],
        "signup_date": ["2024-01-01"],
        "email": ["a@test.com",],
        "phone": ["ABC +081-234-5678"]
    })

    orders = pd.DataFrame({
        "customer_id": [1],
        "order_date": ["2024-01-01"],
        "currency": ["USD"],
        "total_amount": [100]
    })

    exchange = pd.DataFrame({
        "currency": ["USD"],
        "date": ["2024-01-01"],
        "rate_to_usd": [1]
    })

    customers, _ = transform(customers, orders, exchange)

    assert customers.iloc[0]["phone"] == "0812345678"
    


def test_currency_conversion():

    customers = pd.DataFrame({
        "customer_id": [1],
        "signup_date": ["2024-01-01"],
        "email": ["a@test.com"],
        "phone": ["0811111111"]
    })

    orders = pd.DataFrame({
        "customer_id": [1],
        "order_date": ["2024-01-01"],
        "currency": ["EUR"],
        "total_amount": [100]
    })

    exchange = pd.DataFrame({
        "currency": ["EUR"],
        "date": ["2024-01-01"],
        "rate_to_usd": [1.2]
    })

    _, orders_result = transform(customers, orders, exchange)

    assert orders_result.loc[0, "total_amount_usd"] == 120