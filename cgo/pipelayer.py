#!/usr/bin/python3
import cpipe
import redis
from time import time

cpipe.Connect("127.0.0.1", 6379)


bulks = [1000, 10000, 100000, 10000000]
r = redis.Redis()

for bulk in bulks:
    now = time()
    for x in range(0, bulk):
        cpipe.add_command("hset","words","word|{}".format(x),"1")
        if x%10000 == 0:
            cpipe.execute()
    cpipe.execute()
    print(bulk, time() - now)
    r.flushdb()


    