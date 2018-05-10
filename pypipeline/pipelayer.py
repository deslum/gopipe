from ex import Connection
conn = Connection(host='127.0.0.1', port=6379)
commands = [("hset","word","word1","1",),("hset","word","word2","2",),("hset","word","word3","3",)]
conn.execute_pipeline(*commands)