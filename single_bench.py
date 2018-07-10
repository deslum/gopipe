#!/usr/bin/python3
import cpipe
import cpipelib
import credis
import time
import redis
from time import time
import pandas as pd

retry_attempts = 1
hashmap_sizes = [50000]

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
def credis_single_bench(cmd, hm_size):
    for x in range(0, hm_size):
        if cmd == 'hset':
            r.execute(cmd, "words", "word|{}".format(x), "1")
        else:
            r.execute(cmd, "words", "word|{}".format(x))

@timeit
def pipelayer_single_bench(cmd, hm_size):
    for x in range(0, hm_size):
        if cmd == 'hset':
            cpipe.phset("swords0", "word|{}".format(x), "1")
        else:
            cpipe.phget("hwords0", "word|{}".format(x))

@timeit
def pipelayerlib_single_bench(cmd, hm_size):
    for x in range(0, hm_size):
        if cmd == 'hset':
            cpipelib.hset("swords1", "word|{}".format(x), "1")
        else:
            cpipelib.hget("gwords1", "word|{}".format(x))

@timeit
def redispy_single_bench(cmd, hm_size):
    for x in range(0, hm_size):
        if cmd == 'hset':
            redispy.hset("words", "word|{}".format(x), "1")
        else:
            redispy.hget("words", "word|{}".format(x))

writer     = pd.ExcelWriter('Redis cli single benchmark.xlsx', engine='xlsxwriter')

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

    chart.set_x_axis({'name': 'Pipeline size'})
    chart.set_y_axis({'name': 'Seconds', 'major_gridlines': {'visible': False}})
    worksheet.insert_chart('H2', chart)


if __name__ == '__main__':
    
    clients = list([credis_single_bench, 
                    pipelayer_single_bench, 
                    pipelayerlib_single_bench, 
                    redispy_single_bench
                   ])
    for cmd in ['hget', 'hset']:
        data = list()
        for hashmap_size in hashmap_sizes:
            dic = {}
            for cli in clients:
                name, value = cli(cmd, hashmap_size)
                dic[name] = value 
            data.append(dic)
        index = list(map(lambda hashmap_size: str(hashmap_size), [1]))
        save_to_excel(data, index, cmd)

    writer.save()
