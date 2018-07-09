#!/usr/bin/python3
import cpipe
import cpipelib
from credis import Connection
from credis.geventpool import ResourcePool
import time
import redis
from time import time
import asyncio
import asyncio_redis
from aredis import StrictRedis
import pandas as pd


retry_attempts = 1
hashmap_sizes = [50000]

HOST = '127.0.0.1'
PORT = 6379

# Credis connection
credis_pool = ResourcePool(10, Connection, host=HOST, port=PORT)

#Aredis client connection
aredis_client = StrictRedis(host=HOST, port=PORT, db=0)

#AsyncIO redis connection
asyncio_redis_connection = asyncio_redis.Pool.create(host=HOST, port=PORT, poolsize=10)


# Golang extension connection
cpipe.Connect(HOST, PORT)

# Golang extension connection with golang lib
cpipelib.Connect(HOST, PORT)

# Redispy connection
pool = redis.ConnectionPool(host=HOST, port=PORT)
redispy = redis.Redis(connection_pool=pool)

def async_timeit(method):
    async def timed(*args, **kw):
        sum_time = 0
        l = list()
        for _ in range(0, retry_attempts):
            ts = time()
            await method(*args, **kw)
            sum_time += time() - ts
        pipe_size = str(args[0])
        credis_pool.execute("flushdb")
        print(method.__name__, sum_time / retry_attempts)
        return method.__name__, sum_time / retry_attempts
    return timed


def timeit(method):
    @asyncio.coroutine
    def timed(*args, **kw):
        sum_time = 0
        l = list()
        for _ in range(0, retry_attempts):
            ts = time()
            method(*args, **kw)
            sum_time += time() - ts
        pipe_size = str(args[0])
        credis_pool.execute("flushdb")
        print("Coroutine {}", method.__name__, sum_time / retry_attempts)
        return method.__name__, sum_time / retry_attempts
    return timed



@timeit
@asyncio.coroutine
def asyncio_redis_pooling_bench(cmd, hm_size):
    for x in range(0, hm_size):
        if cmd == 'hset':
            yield from asyncio_redis_connection.hset('words222', 'word|{}'.format(x), "1")
        else:
            yield from asyncio_redis_connection.hget('words222', 'word|{}'.format(x))
    asyncio_redis_connection.close()


@async_timeit
async def aredis_pooling_bench(cmd, hm_size):
     for x in range(0, hm_size):
        if cmd == 'hset':
            await aredis_client.hset('words111', 'word|{}'.format(x), "1")
        else:
            await aredis_client.hget('words111', 'word|{}'.format(x))

@timeit
def credis_pooling_bench(cmd, hm_size):
    for x in range(0, hm_size):
        with credis_pool.ctx() as conn:
            if cmd == 'hset':
                conn.execute(cmd, "words", "word|{}".format(x), "1")
            else:
                conn.execute(cmd, "words", "word|{}".format(x))

@timeit
def pipelayer_pooling_bench(cmd, hm_size):
    for x in range(0, hm_size):
        if cmd == 'hset':
            cpipe.phset("words", "word|{}".format(x), "1")
        else:
            cpipe.phget("words", "word|{}".format(x))

@timeit
def pipelayerlib_pooling_bench(cmd, hm_size):
    for x in range(0, hm_size):
        if cmd == 'hset':
            cpipelib.phset("words", "word|{}".format(x), "1")
        else:
            cpipelib.phget("words", "word|{}".format(x))

writer = pd.ExcelWriter('Redis cli pooling benchmark.xlsx', engine='xlsxwriter')

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
            'categories': [sheet_name, 1, 0, 1, 0],
            'values':     [sheet_name, 1, col_num, 1, col_num],
            'fill':       {'color':  colors[col_num - 1]},
            'overlap':    -10,
        })

    chart.set_y_axis({'name': 'Seconds', 'major_gridlines': {'visible': False}})
    worksheet.insert_chart('H2', chart)


if __name__ == '__main__':
    clients = list([
                    asyncio_redis_pooling_bench,
                    aredis_pooling_bench, 
                     
                   ])
    for cmd in ['hget', 'hset']:
        data = list()
        for hashmap_size in hashmap_sizes:
            dic = {}
            for x, cli in enumerate(clients):
                asyncio.get_event_loop().run_until_complete(cli(cmd, hashmap_size))
                dic["1"] = 1 
            data.append(dic)
        index = list(map(lambda hashmap_size: str(hashmap_size), hashmap_sizes))
        save_to_excel(data, index, cmd)

    writer.save()
