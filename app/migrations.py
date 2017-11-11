import requests
import psycopg2
import asyncpg
import asyncio
import json
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time


async def get_pairs(pool):
    async with pool.acquire() as conn:
        result = await conn.fetch("SELECT id, name FROM CurrencyPairs")
        return dict(result)


def get_currency_pairs():
    response_info = requests.get('https://wex.nz/api/3/info')
    info = response_info.json()
    pairs = info['pairs'].keys()
    print(f"Currency pairs received: {pairs}")
    return pairs


# DEPRECATED
def create_tables(currency_pairs):
    with psycopg2.connect(host='db', database='postgres', user='postgres') as conn, conn.cursor() as cur:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        # create database
        cur.execute("CREATE DATABASE async_test;")

    with psycopg2.connect(host='db', database='async_test', user='postgres') as conn, conn.cursor() as cur:
        # create table for currency pairs
        cur.execute(
            "CREATE TABLE IF NOT EXISTS CurrencyPairs (id serial PRIMARY KEY, name varchar UNIQUE);")
        # create table for stashing values of pairs
        cur.execute("""CREATE TABLE IF NOT EXISTS CurrencyPairRates (
                                    id serial PRIMARY KEY,
                                    value decimal,
                                    time timestamp,
                                    pair_id int REFERENCES CurrencyPairs(id)
                                    );""")
        # create index for pair_name
        cur.execute("CREATE INDEX pair_idx ON CurrencyPairs (pair_id)")

        print("Database has created.")


async def async_create_tables(currency_pairs):
    async with asyncpg.create_pool(host='db', database='postgres', user='postgres') as pool:
        async with pool.acquire() as conn:
            # create database
            exists = await conn.fetchval("SELECT COUNT(*) = 0 FROM pg_catalog.pg_database WHERE datname = 'python_db' LIMIT 1")
            if not exists:
                await conn.execute("CREATE DATABASE async_test;")

    async with asyncpg.create_pool(host='db', database='async_test', user='postgres') as pool:
        async with pool.acquire() as conn:
            # create table for currency pairs
            await conn.execute("CREATE TABLE IF NOT EXISTS CurrencyPairs (id serial PRIMARY KEY, name varchar UNIQUE);")
            # create table for stashing values of pairs
            await conn.execute("""CREATE TABLE IF NOT EXISTS CurrencyPairRates (
                                        id serial PRIMARY KEY,
                                        value decimal,
                                        time timestamp,
                                        pair_id int REFERENCES CurrencyPairs(id)
                                        );""")
            # create index for pair_name
            await conn.execute("CREATE INDEX IF NOT EXISTS pair_idx ON CurrencyPairRates (pair_id)")
            # insert currency pairs into table
            transformed_pairs = [(i,) for i in currency_pairs]
            try:
                await conn.executemany("INSERT INTO CurrencyPairs(name) VALUES ($1)", transformed_pairs)
            except asyncpg.exceptions.UniqueViolationError:
                pass

        print("Database has created.")


if __name__ == '__main__':
    currency_pairs = get_currency_pairs()
    time.sleep(2)
    asyncio.get_event_loop().run_until_complete(async_create_tables(currency_pairs))
