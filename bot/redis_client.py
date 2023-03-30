import redis
import json

class RedisClient:
    def __init__(self):
        self.client = redis.Redis(host='redis', port=6379, db=0)

    def put(self, item, id):
        item_str = json.dumps(item)
        self.client.lpush('trade_queue', item_str)
        self.client.sadd('trade_set', id)

    def get(self):
        if self.client.llen('trade_queue') > 0:
            item_str = self.client.rpop('trade_queue')
            item = json.loads(item_str)
            return item

    def is_member(self, id):
        return self.client.sismember('trade_set', id)

    # store a item with push and pop method in redis
    def put_item(self, item, key):
        item_str = json.dumps(item)
        self.client.lpush(key, item_str)

    def get_item(self, key):
        if self.client.llen(key) > 0:
            item_str = self.client.rpop(key)
            item = json.loads(item_str)
            return item

    # store array in redis with a timeout
    def put_array(self, array, key, timeout):
        for item in array:
            self.client.lpush(key, item)
        self.client.expire(key, timeout)

    def get_array(self, key):
        return self.client.lrange(key, 0, -1)
