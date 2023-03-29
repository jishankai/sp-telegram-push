import redis
import json

class RedisClient:
    def __init__(self):
        self.client = redis.Redis(host='redis', port=6379, db=0)

    def put(self, item, id):
        item_str = json.dumps(item)
        self.client.lpush('block_trade_queue', item_str)
        self.client.sadd('block_trade_set', id)

    def get(self):
        if self.client.llen('block_trade_queue') > 0:
            item_str = self.client.rpop('block_trade_queue')
            item = json.loads(item_str)
            return item

    def length(self):
        return self.client.llen('block_trade_queue')

    def is_member(self, id):
        return self.client.sismember('block_trade_set', id)

    # store array in redis with a timeout
    def put_array(self, array, key, timeout):
        self.client.delete(key)
        for item in array:
            self.client.lpush(key, item)
        self.client.expire(key, timeout)

    def get_array(self, key):
        return self.client.lrange(key, 0, -1)
