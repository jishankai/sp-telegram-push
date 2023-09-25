import redis
import json
import time

class RedisClient:
    def __init__(self):
        self.client = redis.Redis(host='redis', port=6379, db=0)

    def put_trade(self, item, id):
        item_str = json.dumps(item)
        self.client.lpush('trade_queue', item_str)
        self.client.sadd('trade_set', id)

    def get_trade(self):
        if self.client.llen('trade_queue') > 0:
            item_str = self.client.rpop('trade_queue')
            item = json.loads(item_str)
            return item

    def is_trade_member(self, id):
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
    def put_array(self, array, key):
        self.client.delete(key)
        for item in array:
            self.client.lpush(key, item)

    def get_array(self, key):
        return self.client.lrange(key, 0, -1)

    # store data by using set method in redis
    def set_data(self, data, key):
        self.client.set(key, data)
    # get data by using get method in redis
    def get_data(self, key):
        return self.client.get(key)

    # store timestamp named timeout in redis
    def set_bybit_symbols_timeout(self, timeout):
        self.client.set('bybit_symbols_timeout', timeout)

    def get_bybit_symbols_timeout(self):
        return self.client.get('bybit_symbols_timeout')

    # store block_trade_id in block_trade_id_queue
    def put_block_trade_id(self, block_trade_id):
        self.client.lpush('block_trade_id_queue', block_trade_id)

    def get_block_trade_id(self):
        if self.client.llen('block_trade_id_queue') > 0:
            block_trade_id = self.client.rpop('block_trade_id_queue')
            return block_trade_id

    def is_block_trade_id_member(self, block_trade_id):
        id_list = self.client.lrange('block_trade_id_queue', 0, -1)
        return block_trade_id in id_list

    def put_block_trade(self, block_trade, id):
        block_trade_str = json.dumps(block_trade)
        self.client.lpush(id, block_trade_str)
        self.client.sadd('trade_set', block_trade['trade_id'])

    def get_block_trade(self, id):
        if self.client.llen(id) > 0:
            block_trade_str = self.client.rpop(id)
            block_trade = json.loads(block_trade_str)
            return block_trade

    def get_block_trade_len(self, id):
        return self.client.llen(id)

    # add paradigm trade timestamp to paradigm_trade_timestamp_set
    def add_paradigm_trade_timestamp(self, timestamp):
        self.client.sadd('paradigm_trade_timestamp_set', timestamp)

    # remove items from paradigm_trade_timestamp_set if they are expired more than 5 minutes
    def remove_paradigm_trade_timestamp(self):
        timestamp_list = self.client.smembers('paradigm_trade_timestamp_set')
        for timestamp in timestamp_list:
            if int(timestamp) < int(time.time()) - 300:
                self.client.srem('paradigm_trade_timestamp_set', timestamp)

    def is_paradigm_trade_timestamp_member(self, timestamp):
        return self.client.sismember('paradigm_trade_timestamp_set', timestamp)
