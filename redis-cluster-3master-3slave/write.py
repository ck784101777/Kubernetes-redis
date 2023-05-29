from rediscluster import ClusterBlockingConnectionPool,RedisCluster,ClusterConnectionPool
from string import ascii_letters
from time import sleep
from loguru import logger
import random

startup_nodes = [
    {'host': 'redis-service.default.svc.cluster.local', 'port': 6379},
]
pool = ClusterConnectionPool(startup_nodes=startup_nodes,skip_full_coverage_check=True,socket_connect_timeout=1)
while True:
  try:
    redis_client = RedisCluster(connection_pool=pool)
    if redis_client.ping():
      logger.info('get Redis connection instance successful!',redis_client.info())
    for i in range(100):
      sleep(0.05)
      key = ''.join(random.sample(ascii_letters, k=7))
      redis_client.set(key, random.randint(1, 100), ex=500)
      print('write:',key)
  except Exception as e:
    logger.error('Redis connection exception:{str(e)},traceback={traceback.format_exc()}')
    pool = ClusterConnectionPool(startup_nodes=startup_nodes,skip_full_coverage_check=True,socket_connect_timeout=1)
