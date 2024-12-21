import asyncio
import datetime
import itertools
import time
from typing import Coroutine

import aiohttp

from models import Person, Session, refresh_db_state

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


async def make_request(url: str, session: aiohttp.ClientSession):
    response = await session.get(url=url)
    data = await response.json()
    return data


async def insert_objects(data: list[dict]):
    async with Session() as session:
        persons_models = [Person(**person_info) for person_info in data if len(person_info) > 1]
        if persons_models:
            session.add_all(persons_models)
            await session.commit()


async def data_handler(data: list[dict], session: aiohttp.ClientSession) -> list[dict]:
    for person_info in data:
        for attr in ASSOCIATIVE_ATTRS:
            if isinstance(person_info.get(attr), str) and person_info[attr].startswith(BASE_URL):
                person_info[attr] = [person_info[attr]]

            if isinstance(person_info.get(attr), list):
                coros: list[Coroutine] = [
                    make_request(url=url, session=session) for url in person_info[attr]
                ]
                nested_data: list[dict] = await asyncio.gather(*coros)
                target_info: str = "; ".join(
                    ", ".join(info[target_attr] for target_attr in ASSOCIATIVE_ATTRS[attr])
                    for info in nested_data
                )
                person_info[attr] = target_info
    return data


@refresh_db_state()
async def main():
    async with aiohttp.ClientSession() as session:
        requests_sequence = itertools.batched(range(1, TOTAL_REQUESTS + 1), MAX_CONCURRENT_REQUESTS)
        for ids in requests_sequence:
            await asyncio.sleep(0.2)
            coros: list[Coroutine] = [
                make_request(url=f"{BASE_URL}/people/{id}/", session=session) for id in ids
            ]
            data: list[dict] = await asyncio.gather(*coros)
            data_to_upload: list[dict] = await data_handler(data=data, session=session)
            task: asyncio.Task = asyncio.create_task(insert_objects(data=data_to_upload))
        all_tasks: set[asyncio.Task] = asyncio.all_tasks()
        all_tasks.discard(asyncio.current_task())
        await asyncio.gather(*all_tasks)


if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    delta = time.time() - start
    print(f"\nИтоговое время асинхронной выгрузки - {datetime.timedelta(seconds=delta)} секунд")
