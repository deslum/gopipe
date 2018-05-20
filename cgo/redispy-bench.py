#!/usr/bin/python3

import redis
import time
import sys
from time import time

r = redis.Redis()
pipe = r.pipeline()

bulks = [1000, 10000, 100000, 1000000]

for bulk in bulks:
    now = time()
    for x in range(0, bulk):
        pipe.hset("words","word|{}".format(x),"1")
        if x%10000 == 0:
            pipe.execute()
    pipe.execute()
    print(bulk, time() - now)
    r.flushdb()