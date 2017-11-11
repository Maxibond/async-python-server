import aiohttp
from aiohttp import web
import asyncio
import async_timeout
import json
import asyncpg
import time
from migrations import get_pairs
import simplejson as json
from collections import defaultdict


async def handle(request):
    pool = request.app['pool']
    pair_name = request.match_info.get('name', 'all')
    print(pair_name)
    limit = int(request.rel_url.query.get('limit', 1))
    print(limit)
    if pair_name == 'all':
        result = await get_all_pairs(request.app['pairs_count'], pool, limit=limit)
    else:
        result = await get_pair(pair_name, pool, limit=limit)
    return web.Response(text=json.dumps(result))


async def get_pair(name, pool, limit=1):
    async with pool.acquire() as conn:
        dbresult = await conn.fetch("""
            SELECT cpr.value, cpr.time FROM CurrencyPairs cp
            JOIN CurrencyPairRates cpr ON cp.id = cpr.pair_id
            WHERE cp.name = $1 
            ORDER BY cpr.id DESC
            LIMIT $2
        """, name, limit)
        result = [{'value': record['value'], 'time': str(record['time'])} for record in dbresult]
        print("GET PAIR ", result)
        return result


async def get_all_pairs(pairs_count, pool, limit=1):
    async with pool.acquire() as conn:
        dbresult = await conn.fetch("""
            SELECT cp.name, cpr.value, cpr.time FROM CurrencyPairRates cpr
            JOIN CurrencyPairs cp ON cp.id = cpr.pair_id
            ORDER BY cpr.id DESC 
            LIMIT $1
        """, pairs_count * limit)  # we want to get LIMIT times by PAIRS_COUNT
        result = defaultdict(list)
        for record in dbresult:
            result[record['name']].append({'value': record['value'], 'time': str(record['time'])})
        print("GET ALL ", result)
        return result


async def main():
    app = web.Application()
    app['pool'] = await asyncpg.create_pool(host='db',
                                            database='async_test',
                                            user='postgres')
    pairs = await get_pairs(app['pool'])
    app['pairs_count'] = len(pairs.keys())

    app.router.add_route('GET', '/', handle)
    app.router.add_route('GET', '/{name}', handle)

    return app


if __name__ == '__main__':
    time.sleep(4)
    print("Starting...")
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(main())
    web.run_app(app)
    print("Started!")
