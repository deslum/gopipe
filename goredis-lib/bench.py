from credis import Connection
import hashlib

connection = Connection(host='127.0.0.1', port=6379)
hashes = list()
del_hashes = list()
for n in range(0, 1000):
	line = 'word|{}'.format(n)
	md5word = hashlib.md5(line.encode('utf-8')).hexdigest()
	hashes.append(('hset','words', md5word, "1",))
	del_hashes.append(('hdel','words', md5word, "1",))
	create = connection.execute_pipeline(*del_hashes)


def execute():
	cpipe.add_command("hset", "words", "word1", "1")
	cpipe.add_command("hset", "words", "word2", "1")
	cpipe.add_command("hset", "words", "word3", "1")
	cpipe.add_command("hset", "words", "word4", "1")
	cpipe.add_command("hset", "words", "word5", "1")
	cpipe.add_command("hset", "words", "word6", "1")
	connection.execute_pipeline(*hashes)

import cProfile
cProfile.run('execute()')


# from credis.geventpool import ResourcePool
# from credis import Connection
# from multiprocessing import Pool

# from time import time
# import hashlib

# connection = ResourcePool(256, Connection, host='127.0.0.1', port=6379)

# def hset(hashs):
# 	with connection.ctx() as conn:
# 		create = conn.execute_pipeline(*hashs)

# def hdel(hashs):
# 	with connection.ctx() as conn:
# 		create = conn.execute_pipeline(*hashs)


# if __name__ == '__main__':	
# 	hashes = list()
# 	del_hashes = list()
# 	chnk = list()
# 	del_chnk = list()
# 	for n in range(0, 100001):
# 		line = 'word|{}'.format(n)
# 		md5word = hashlib.md5(line.encode('utf-8')).hexdigest()
# 		if len(chnk) < 10000:
# 			chnk.append(('hset','words', md5word, "1",))
# 			del_chnk.append(('hdel','words', md5word))
# 		else:
# 			hashes.append(chnk)
# 			del_hashes.append(del_chnk)
# 			chnk = list()
# 			del_chnk = list()
# 	times = list()
# 	for x in range(1,11):
# 		t_now = time()
# 		pool = Pool()
# 		pool.map(hset, hashes)
# 		pool.close()
# 		pool.join()
# 		now = time()- t_now
# 		print("part{} {}".format(x, now))
# 		times.append(now)
# 		pool = Pool()
# 		pool.map(hdel, del_hashes)
# 		pool.close()
# 		pool.join()
		
# 	print("Result", sum(times)/ 10.0)