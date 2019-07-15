from app import setup_database, setup_workers, setup_system
from controllers import *
from time import time, sleep
from uuid import uuid4
from random import choice, sample, randint
from utils import redis

published_ids = []
users = ['joe', 'candice', 'jack', 'alice']


def clear_ns(ns):
    """
    Clears a namespace
    :param ns: str, namespace i.e your:prefix
    :return: int, cleared keys
    """
    count = 0
    ns_keys = ns + '*'
    for key in redis.scan_iter(ns_keys):
        redis.delete(key)
        count += 1
    return count


def init():
    setup_system()
    setup_workers()


def drop():
    setup_database(drop=True)
    clear_ns('')


def subscribe():

    for user in users:
        EventProcessor.subscribe("feed", "shayan", user)


def publish():

    for i in range(500):
        producer_id = choice(users)
        item_id = i
        published_ids.append((item_id, producer_id))
        timestamp = time() - randint(10, 1000)
        EventProcessor.add_event({
            'verb': 'podcast', 'producer_id': producer_id, 'timestamp': timestamp, 'item_id': item_id
        })


def retract():

    for item_id, producer_id in sample(published_ids, k=50):
        EventProcessor.retract_event({
            'verb': 'podcast', 'item_id': item_id, 'producer_id': producer_id
        })


def unsub_sub():

    for user in sample(users, k=2):
        EventProcessor.unsubscribe('feed', producer_id=user, consumer_id='shayan')
        EventProcessor.subscribe('feed', producer_id=user, consumer_id='jenna')


def consume():

    data = (EventProcessor.consume('feed', consumer_id='shayan', limit=10))
    print(len(data))
    for item in data:
        print(item)

    print('after', data[5]['item_id'])
    data = (EventProcessor.consume('feed', consumer_id='shayan', limit=10, after=data[5]['item_id']))
    print(len(data))
    for item in data:
        print(item)


if __name__ == '__main__':
    init()
    #     # subscribe()
    #     # publish()
    #     # retract()
    #     # unsub_sub()
    #     #
    #     # sleep(10)
    consume()
    print('done')
