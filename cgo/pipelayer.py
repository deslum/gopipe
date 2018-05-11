#!/usr/bin/python3
import cpipe
from time import time

cpipe.Connect("127.0.0.1", 6379)
now = time()
for x in range(0,90000):
    cpipe.add_command("hset","words","word|{}".format(x),"1")
    if x%80 == 0:
        cpipe.execute(10)
cpipe.execute(10)
print(time() - now)