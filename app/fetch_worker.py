import aiohttp
from aiohttp import web
import asyncio
import async_timeout
import json
import asyncpg
import time
from datetime import datetime
from migrations import get_pairs


async def fetch(session, url):
    async with async_timeout.timeout(5):
        async with session.get(url) as response:
            return await response.text()


async def fetch_rates(pool):
    currency_pairs = await get_pairs(pool)
    print("CURRENCY PAIRS: ", currency_pairs)
    param_code_pairs = "-".join(currency_pairs.values())
    async with aiohttp.ClientSession() as session, pool.acquire() as conn:
        while(True):
            time_begin_request = datetime.now()
            json_result = await fetch(session, f'https://wex.nz/api/3/ticker/{param_code_pairs}')
            result = json.loads(json_result)
            values = {pair_id: result[pair_name]['last']
                      for pair_id, pair_name in currency_pairs.items()}
            transformed_rates = [(value, time_begin_request, pair_id)
                                 for pair_id, value in values.items()]
            await conn.executemany("INSERT INTO CurrencyPairRates(value, time, pair_id) VALUES ($1, $2, $3)", transformed_rates)
            print("Portion of new data has fetched.")
            await asyncio.sleep(5)


if __name__ == '__main__':
    time.sleep(4)
    print("Fetching is starting...")
    loop = asyncio.get_event_loop()
    pool = loop.run_until_complete(asyncpg.create_pool(
        host='db', database='async_test', user='postgres'))
    loop.run_until_complete(fetch_rates(pool))
    print("Fetching has started!")
