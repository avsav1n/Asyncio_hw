import datetime
import itertools
import time

import requests
from sync_models import Session, SyncPerson, refresh_db_state

MAX_CONCURRENT_REQUESTS = 10
TOTAL_REQUESTS = 100
BASE_URL = "https://swapi.py4e.com/api/"
ASSOCIATIVE_ATTRS = {
    "homeworld": ["name"],
    "films": ["title", "release_date"],
    "species": ["name"],
    "vehicles": ["name", "model"],
    "starships": ["name", "model"],
}


def make_request(url: str):
    response = requests.get(url=url)
    data = response.json()
    return data


def insert_objects(data: list[dict]):
    with Session() as session:
        persons_models = [SyncPerson(**person_info) for person_info in data if len(person_info) > 1]
        if persons_models:
            session.add_all(persons_models)
            session.commit()


def data_handler(data: list[dict]) -> list[dict]:
    for person_info in data:
        for attr in ASSOCIATIVE_ATTRS:
            if isinstance(person_info.get(attr), str) and person_info[attr].startswith(BASE_URL):
                person_info[attr] = [person_info[attr]]

            if isinstance(person_info.get(attr), list):
                nested_data: list[dict] = [make_request(url=url) for url in person_info[attr]]
                target_info: str = "; ".join(
                    ", ".join(info[target_attr] for target_attr in ASSOCIATIVE_ATTRS[attr])
                    for info in nested_data
                )
                person_info[attr] = target_info
    return data


@refresh_db_state()
def main():
    requests_sequence = itertools.batched(range(1, TOTAL_REQUESTS + 1), MAX_CONCURRENT_REQUESTS)
    for ids in requests_sequence:
        time.sleep(0.2)
        data: list[dict] = [make_request(url=f"{BASE_URL}/people/{id}/") for id in ids]
        data_to_upload: list[dict] = data_handler(data=data)
        insert_objects(data=data_to_upload)


if __name__ == "__main__":
    start = time.time()
    main()
    delta = time.time() - start
    print(f"\nИтоговое время синхронной выгрузки - {datetime.timedelta(seconds=delta)}")
