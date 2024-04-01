import requests
import json
from typing import Any
from datetime import date, timedelta
import pandas as pd
import io
from google.cloud import bigquery
from copy import copy
from tenacity import retry


URL = "https://us-central1-passion-fbe7a.cloudfunctions.net/dzn54vzyt5ga"
HEADERS = {
    "Authorization": "gAAAAABmAY6FF0R7g69CkjcS3EWtOLMGqZE5DbmQPpdJuzT9qaaQZR1gRv2sYYSHwLCZpRptqjugjOEuYNLOtqiqHwiE9B9vSdK3VYHCyFUNzTCJgn2FGh5Dh8c-zj_Q5_l6M9zU_QfhW3BiO-GyBGS_Jl1GsE1XAw=="
}

BQ_CLIENT = bigquery.Client.from_service_account_json("my-project-1-393913-60c0ae7f9a40.json")


def get_installs(_date: date) -> list[dict[str, Any]]:
    response = requests.get(f"{URL}/installs", headers=HEADERS, params={"date": str(_date)})
    json_response = json.loads(response.text)
    records = json.loads(json_response.get("records"))
    return records


def get_costs(_date: date) -> list[dict[str, Any]]:
    dimensions = "location,channel,medium,campaign,keyword,ad_content,ad_group,landing_page"
    all_costs = []
    response = requests.get(f"{URL}/costs", headers=HEADERS, params={"date": str(_date), "dimensions": dimensions})
    rows = response.text.split("\n")
    header_row = rows[0].split("\t")
    for row in rows[1:]:
        values = row.split("\t")
        all_costs.append(dict(zip(header_row, values)))

    # explicitly add date
    for cost in all_costs:
        cost["date"] = str(_date)
    return all_costs


def get_events(_date: date) -> list[dict[str, Any]]:
    all_events = []

    response = requests.get(f"{URL}/events", headers=HEADERS, params={"date": str(_date)})
    json_response = json.loads(response.text)
    first_page_events = json.loads(json_response.get("data"))
    all_events.extend(first_page_events)
    next_page = json_response["next_page"]

    while next_page:
        _events, next_page = get_events_from_page(_date, next_page)
        all_events.extend(_events)
        if not next_page:
            break
    clean_events = transform_none_values(all_events)
    return clean_events


@retry
def get_events_from_page(_date: date, next_page: str) -> tuple[list[dict[str, Any]], str | None]:
    response = requests.get(f"{URL}/events", headers=HEADERS, params={"date": str(_date), "next_page": next_page})
    json_response = json.loads(response.text)
    records = json.loads(json_response.get("data"))
    return records, json_response.get("next_page")


def get_orders(_date: date) -> pd.DataFrame:
    response = requests.get(f"{URL}/orders", headers=HEADERS, params={"date": str(_date)})
    parquet_data = response.content
    with io.BytesIO() as buffer:
        buffer.write(parquet_data)
        return pd.read_parquet(buffer)


def load_json_to_bq(data: list[dict[str, Any]], table_name: str) -> None:
    table_id = f"my-project-1-393913.test_task.{table_name}"

    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True)

    load_job = BQ_CLIENT.load_table_from_json(data, table_id, job_config=job_config)
    load_job.result()

    print(f"{len(data)} Rows loaded into BigQuery table: {table_id}")


def transform_orders(orders: pd.DataFrame) -> list[dict[str, Any]]:
    """Rename columns & convert pandas timestamp to a str"""
    orders = orders.rename(
        columns={
            "discount.code": "discount_code",
            "discount.amount": "discount_amount",
            "iap_item.name": "item_name",
            "iap_item.price": "item_price",
        }
    )
    _orders = [order.to_dict() for _, order in orders.iterrows()]
    for order in _orders:
        order["event_time"] = str(order["event_time"])
    return _orders


def transform_none_values(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Replace "None" and Undefined with Python's None"""
    _records = copy(records)
    for record in records:
        for key, value in record.items():
            if value in ("None", "Null", "Undefined"):
                record[key] = None

    return records


def align_sex(installs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Align "sex" to be either male or female for data consistency"""
    _installs = copy(installs)
    for install in _installs:
        if install["sex"] in ("Masculine", "m"):
            install["sex"] = "male"

        if install["sex"] in ("Feminine", "f"):
            install["sex"] = "female"

    return _installs


def load_data() -> None:
    date_to_fetch_data = date.today() - timedelta(days=2)
    installs = get_installs(date_to_fetch_data)
    installs = transform_none_values(installs)
    installs = align_sex(installs)
    load_json_to_bq(installs, "installs")

    costs = get_costs(date_to_fetch_data)
    load_json_to_bq(transform_none_values(costs), "costs")

    events = get_events(date_to_fetch_data)
    load_json_to_bq(events, "events")

    orders = get_orders(date_to_fetch_data)
    _prepared_orders = transform_orders(orders)
    load_json_to_bq(_prepared_orders, "orders")
