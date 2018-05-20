#!/usr/bin/python3

import credis
import time
import sys
from time import time

r = credis.Connection()

bulks = [1000, 10000, 100000, 1000000]
pipe_list = list()
for bulk in bulks:
    now = time()
    for x in range(0, bulk):
        pipe_list.append(("hset","words","word|{}".format(x),"1",))
        if x%1000 == 0:
            r.execute_pipeline(*pipe_list)
            pipe_list = list()
    r.execute_pipeline(*pipe_list)
    print(bulk, time() - now)
    r.execute("flushdb")