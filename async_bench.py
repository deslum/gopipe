#!/usr/bin/env python3
import asyncio
import aredis
import asyncio_redis
import time
import cpipe
import cpipelib
import asyncio
from aredis import StrictRedis
from credis import Connection
from credis.geventpool import ResourcePool
import pandas as pd

HOST = '127.0.0.1'
PORT = 6379
NUM_INSERTS = 50000

credis_connection = Connection()

hset_dict = dict()
hget_dict = dict()


def asyncio_redis_bench():
    try:
        connection = yield from asyncio_redis.Pool.create(host=HOST, port=PORT, poolsize=10)
        connection.flushdb()
        start = time.time()
        for x in range(NUM_INSERTS):
            yield from connection.hset("words", "word|{}".format(x), "1")
        print('Asyncio HSET Done. Duration=', time.time() - start)
        hset_dict['asyncio_redis'] = time.time() - start

        start = time.time()
        for x in range(NUM_INSERTS):
            yield from connection.hget("words", "word|{}".format(x))
        print('Asyncio HGET Done. Duration=', time.time() - start)
        hget_dict['asyncio_redis'] = time.time() - start
    finally:
        connection.close()

def cpipe_bench():
    cpipe.Connect(HOST, PORT)
    credis_connection.execute("flushdb")
    start = time.time()
    for x in range(NUM_INSERTS):
        cpipe.phset("words", "word|{}".format(x), "1")
    print('CPipe HSET Done. Duration=', time.time() - start)
    hset_dict['cpipe'] = time.time() - start


    start = time.time()
    for x in range(NUM_INSERTS):
        cpipe.phget("words", "word|{}".format(x))
    print('CPipe HGET Done. Duration=', time.time() - start)
    hget_dict['cpipe'] = time.time() - start


def cpipelib_bench():
    cpipelib.Connect(HOST, PORT)
    credis_connection.execute("flushdb")
    start = time.time()
    for x in range(NUM_INSERTS):
        cpipelib.phset("words", "word|{}".format(x), "1")
    print('CPipelib HSET Done. Duration=', time.time() - start)
    hset_dict['cpipelib'] = time.time() - start


    start = time.time()
    for x in range(NUM_INSERTS):
        cpipelib.phget("words", "word|{}".format(x))
    print('CPipelib HGET Done. Duration=', time.time() - start)
    hget_dict['cpipelib'] = time.time() - start


def credis_bench():
    credis_pool = ResourcePool(10, Connection, host=HOST, port=PORT)
    credis_connection.execute("flushdb")
    start = time.time()
    for x in range(NUM_INSERTS):
        with credis_pool.ctx() as conn:
            conn.execute("hset", "words", "word|{}".format(x), "1")
    print('CRedis HSET Done. Duration=', time.time() - start)
    hset_dict['credis'] = time.time() - start

    start = time.time()
    for x in range(NUM_INSERTS):
        with credis_pool.ctx() as conn:
            conn.execute("hget", "words", "word|{}".format(x))
    print('CRedis HGET Done. Duration=', time.time() - start)
    hget_dict['credis'] = time.time() - start


async def aredis_bench():
    client = StrictRedis(host=HOST, port=PORT, db=0)
    await client.flushdb()
    start = time.time()
    for x in range(NUM_INSERTS):
        await client.hset("words", "word|{}".format(x), "1")
    print('Aredis HSET Done. Duration=', time.time() - start)
    hset_dict['aredis'] = time.time() - start


    start = time.time()
    for x in range(NUM_INSERTS):
        await client.hget("words", "word|{}".format(x))
    print('Aredis HGET Done. Duration=', time.time() - start)
    hget_dict['aredis'] = time.time() - start

writer = pd.ExcelWriter('Redis cli async benchmark.xlsx', engine='xlsxwriter')

def save_to_excel(data, index, sheet_name):
    
    df = pd.DataFrame(data, index=index)
    df.to_excel(writer, sheet_name=sheet_name)
    workbook  = writer.book
    worksheet = writer.sheets[sheet_name]
    chart = workbook.add_chart({'type': 'column'})
    colors = ['#E41A1C', '#377EB8', '#4DAF4A', '#984EA3', '#FF7F00']

    for col_num in range(1, len(data[0]) + 1):
        chart.add_series({
            'name':       [sheet_name, 0, col_num],
            'categories': [sheet_name, 1, 0, 7, 0],
            'values':     [sheet_name, 1, col_num, 7, col_num],
            'fill':       {'color':  colors[col_num - 1]},
            'overlap':    -10,
        })

    chart.set_y_axis({'name': 'Seconds', 'major_gridlines': {'visible': False}})
    worksheet.insert_chart('H2', chart)

if __name__ == '__main__':
    
    index = list(map(lambda pipe_size: str(pipe_size), [1]))
    asyncio.get_event_loop().run_until_complete(asyncio_redis_bench())
    asyncio.get_event_loop().run_until_complete(aredis_bench())
    cpipe_bench()
    cpipelib_bench()
    credis_bench()
    save_to_excel([hset_dict], index, "hset")
    save_to_excel([hget_dict], index, "hget")
    writer.save()