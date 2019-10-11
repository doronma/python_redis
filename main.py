import bz2
import json
import time
from datetime import timedelta
from pprint import pprint

import redis
import os
from cryptography.fernet import Fernet

from flat_dict import setflat_skeys
from hats_db import hats
from restaurant import restaurant_484272
from tran import buyitem

# create redis client on localhost 6379
#r = redis.Redis()

os.environ["REDIS_ENDPOINT"] = 'localhost'
r = redis.Redis(host=os.environ["REDIS_ENDPOINT"],port=6379)



# clean db
r.flushdb()
# set a key
r.set("myName", "Doron")
# get a key
print(f"My Name is {(r.get('myName')).decode('utf-8')}")
# Work with pipline to use only one open connection to redis
with r.pipeline() as pipe:
    for h_id, hat in hats.items():
        # set key:dict items
        pipe.hmset(h_id, hat)
    pipe.execute()
r.bgsave()

# search for keys
hat_keys = [item.decode("utf-8") for item in r.keys('hat*')]

# get dict values
for key in hat_keys:
    print(key, {k.decode("utf-8"): v.decode("utf-8")
                for (k, v) in r.hgetall(key).items()})

key = "hat:56854717"
before = r.hget(key, "quantity")
# decrease quantity value by one
r.hincrby(key, "quantity", -1)
print(f'{key} quantity was decreased from {before} to {r.hget(key,"quantity")}')

# buy item within a transaction
print("before", key, {k.decode("utf-8"): v.decode("utf-8")
                      for (k, v) in r.hgetall(key).items()})
buyitem(r, key)
print("after", key, {k.decode("utf-8"): v.decode("utf-8")
                     for (k, v) in r.hgetall(key).items()})

# expired keys
#r.setex("runner", timedelta(seconds=5), value="now you see me, now you don't")
r.set("runner", "now you see me, now you don't")
r.expire("runner", 5)
time.sleep(3)
print(r.ttl("runner"))
print(f'runner is - {r.get("runner").decode("utf-8")}', r.exists('runner'))
time.sleep(3)
print(f'runner is - {r.get("runner")}', r.exists('runner'))

# save serialized dictionary
restaurant_json = json.dumps(restaurant_484272)
r.set(484272, restaurant_json)
pprint(json.loads(r.get(484272)))
# remove item
r.delete(484272)
# save  dictionary as flat dictionary
setflat_skeys(r, restaurant_484272, 484272)
for key in sorted(r.keys("484272*")):  # Filter to this pattern
    print(f"{repr(key):35}{repr(r.get(key)):15}")

# use cryptography to save encrypted data
cipher = Fernet(Fernet.generate_key())
password = 'my_password'
encrypted_password = cipher.encrypt(bytes(password, 'utf-8'))
r.set('password',encrypted_password)
print (cipher.decrypt(r.get('password')))

# use compression
blob = "i have a lot to talk about" * 10000
blob_compressed = bz2.compress(blob.encode("utf-8"))
print(f'len_before = {len(blob)}, len_after = {len(blob_compressed)}')
