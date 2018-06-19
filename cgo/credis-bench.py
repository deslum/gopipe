#!/usr/bin/python3
import pandas as pd
import cpipe
import credis
import time
import redis
from time import time

retry_attempts = 5
hashmap_sizes = [1000, 10000, 100000, 1000000]
pipe_sizes = [100, 1000, 10000, 100000]

# Credis connection
r = credis.Connection()

# Golang extension connection
cpipe.Connect("127.0.0.1", 6379)

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
        print('PipeSize {0} HashmapSize {1} Time {2}'.format(args[0], args[1], sum_time / retry_attempts))
        r.execute("flushdb")
        return 
    return timed


@timeit
def start_bench(pipe_size, hm_size):
    pipe_list = list()
    for x in range(0, hm_size):
        pipe_list.append(("hset", "words", "word|{}".format(x), "1",))
        if x % pipe_size == 0:
            r.execute_pipeline(*pipe_list)
            pipe_list.clear()
    if len(pipe_list) > 0:
        r.execute_pipeline(*pipe_list)


@timeit
def start_pipelayer_bench(pipe_size, hm_size):
    for x in range(0, hm_size):
        cpipe.add_command("hset", "words", "word|{}".format(x), "1")
        if x % pipe_size == 0:
            cpipe.execute()
    cpipe.execute()


@timeit
def start_redispy_bench(pipe_size, hm_size):
    pipe = redispy.pipeline(transaction=False)
    for x in range(0, hm_size):
        pipe.hset("words", "word|{}".format(x), "1")
        if x % pipe_size == 0:
            pipe.execute()
    pipe.execute()


if __name__ == '__main__':
    data = list()
    index = list()
    for pipe_size in pipe_sizes:
        for hm_size in hashmap_sizes:
            results = start_pipelayer_bench(pipe_size, hm_size)
        for x, result in enumerate(results):
            if not data[x]:
                data[x] = {}
            data[x]['pipelayer'] = result[x]
        index.append(pipe_size)
    for pipe_size in pipe_sizes:


    farm_1 = list(){'Apples': 10, 'Berries': 32, 'Squash': 21, 'Melons': 13, 'Corn': 18}
    farm_2 = {'Apples': 15, 'Berries': 43, 'Squash': 17, 'Melons': 10, 'Corn': 22}
    farm_3 = {'Apples': 6, 'Berries': 24, 'Squash': 22, 'Melons': 16, 'Corn': 30}