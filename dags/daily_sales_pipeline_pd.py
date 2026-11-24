import pandas as pd
import pendulum

from pathlib import Path
from airflow.sdk import dag, task


INPUT_FILE = Path("/opt/airflow/data/orders.csv")
OUTPUT_FILE = Path("/opt/airflow/output/daily_sales.csv")


@dag(
    dag_id="daily_sales_pipeline_pd",
    start_date=pendulum.datetime(
        2026, 11, 1, tz="Asia/Jakarta"
    ),
    schedule=None,
    catchup=False,
    tags=["workshop"]
)
def daily_sales_pipeline():

    @task
    def extract():
        """TASK 1: Extract data."""

        df = pd.read_csv(INPUT_FILE)

        print(f"Read {len(df)} orders.")

        return df.to_dict("records")

    @task
    def transform(orders):
        """TASK 2: Transform data."""

        df = pd.DataFrame(orders)

        df = df[df["status"] == "paid"].copy()
        df["amount"] = df["amount"].astype(int)

        daily = (
            df.groupby("order_date", as_index=False)
            .agg(
                total_orders=("amount", "size"),
                revenue=("amount", "sum")
            )
            .rename(columns={"order_date": "date"})
        )

        return daily.to_dict("records")

    @task
    def validate(sales):
        """TASK 3: Validate data."""

        df = pd.DataFrame(sales)

        if df.empty:
            raise ValueError("No paid orders found!")

        invalid = df[
            (df["total_orders"] <= 0) |
            (df["revenue"] < 0)
        ]

        if not invalid.empty:
            raise ValueError(f"Invalid sales data:\n{invalid}")

        print("Sales data validation passed!")

        return df.to_dict("records")

    @task
    def publish(sales):
        """TASK 4: Publish data."""

        df = pd.DataFrame(sales)

        OUTPUT_FILE.parent.mkdir(
            parents=True, exist_ok=True
        )

        df.to_csv(OUTPUT_FILE, index=False)

        print(f"Saved report to {OUTPUT_FILE}.")

    orders = extract()
    sales = transform(orders)
    checked = validate(sales)
    publish(checked)


daily_sales_pipeline()