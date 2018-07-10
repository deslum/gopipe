#!/usr/bin/python3
import cpipe
import cpipelib
import credis
import time
import redis
from time import time
import pandas as pd

retry_attempts = 1
pipe_sizes = [100, 500, 1000]
hashmap_sizes = [1000, 5000, 10000]

HOST = '127.0.0.1'
PORT = 6379

# Credis connection
r = credis.Connection()

# Golang extension connection
cpipe.Connect(HOST, PORT)

# Golang extension connection with golang lib
cpipelib.Connect(HOST, PORT)

# Redispy connection
redispy = redis.Redis()


def timeit(method):
    def timed(*args, **kw):
        sum_time = 0
        l = list()
        for _ in range(0, retry_attempts):
            ts = time()
            method(*args, **kw)
            sum_time += time() - ts
        pipe_size = str(args[0])
        r.execute("flushdb")
        return method.__name__, sum_time / retry_attempts
    return timed


@timeit
def credis_bench(cmd, pipe_size, hm_size):
    pipe_list = list()
    for x in range(0, hm_size):
        if cmd == 'hset':
            pipe_list.append((cmd, "words", "word|{}".format(x), "1",))
        else:
            pipe_list.append((cmd, "words", "word|{}".format(x),))
        if x % pipe_size == 0:
            r.execute_pipeline(*pipe_list)
            pipe_list.clear()
    if len(pipe_list) > 0:
        r.execute_pipeline(*pipe_list)


@timeit
def pipelayer_bench(cmd, pipe_size, hm_size):
    for x in range(0, hm_size):
        if cmd == 'hset':
            cpipe.add_command(cmd, "words", "word|{}".format(x), "1")
        else:
            cpipe.add_command(cmd, "words", "word|{}".format(x))
        if x % pipe_size == 0:
            cpipe.execute()
    cpipe.execute()

@timeit
def pipelayerlib_bench(cmd, pipe_size, hm_size):
    for x in range(0, hm_size):
        if cmd == 'hset':
            cpipelib.add_command(cmd, "words", "word|{}".format(x), "1")
        else:
            cpipelib.add_command(cmd, "words", "word|{}".format(x))
        if x % pipe_size == 0:
            cpipelib.execute()
    cpipelib.execute()


@timeit
def redispy_bench(cmd, pipe_size, hm_size):
    pipe = redispy.pipeline(transaction=False)
    for x in range(0, hm_size):
        if cmd == 'hset':
            pipe.hset("words", "word|{}".format(x), "1")
        else:
            pipe.hget("words", "word|{}".format(x))
        if x % pipe_size == 0:
            pipe.execute()
    pipe.execute()


writer = pd.ExcelWriter('Redis cli pipeline benchmark.xlsx', engine='xlsxwriter')

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
            'categories': [sheet_name, 1, 0, 3, 0],
            'values':     [sheet_name, 1, col_num, 3, col_num],
            'fill':       {'color':  colors[col_num - 1]},
            'overlap':    -10,
        })

    chart.set_x_axis({'name': 'Pipeline size'})
    chart.set_y_axis({'name': 'Seconds', 'major_gridlines': {'visible': False}})
    worksheet.insert_chart('H2', chart)


if __name__ == '__main__':
    clients = list([credis_bench, pipelayer_bench, pipelayerlib_bench, redispy_bench])
    for cmd in ['hget', 'hset']:
        data = list()
        for pipe_size in pipe_sizes:
            dic = {}
            for cli in clients:
                name, value = cli(cmd, pipe_size, 500000)
                dic[name] = value 
            data.append(dic)
        index = list(map(lambda pipe_size: str(pipe_size), pipe_sizes))
        save_to_excel(data, index, '{} Pipeline size'.format(cmd))

        data = list()
        for hashmap_size in hashmap_sizes:
            dic = {}
            for cli in clients:
                name, value = cli(cmd, 100, hashmap_size)
                dic[name] = value 
            data.append(dic)
        index = list(map(lambda pipe_size: str(pipe_size), pipe_sizes))
        save_to_excel(data, index, '{} Hashmap size'.format(cmd))

    writer.save()