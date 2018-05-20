#!/usr/bin/env python

import redis
import time
import sys


r = redis.Redis()
pipe = r.pipeline()

import cpipe
cpipe.Connect("127.0.0.1", 6379)

payload = "a" * int(sys.argv[1])

now = time.time()
for x in range(10000):
    x += 1
    if x%1000 == 0:
        cpipe.execute(4)
    if x%1000 == 0:
        print(x)
    cpipe.add_command("hset",str(x),payload, "1")

cpipe.execute(4)
print(time.time() - now)

r.flushdb()
