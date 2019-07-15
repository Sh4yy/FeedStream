from app import setup_database, setup_workers, setup_system
from controllers import *
from time import time, sleep
from uuid import uuid4
from random import choice, sample

published_ids = []
users = ['joe', 'candice', 'jack', 'alice']


def init():
    setup_system()
    setup_workers()
    setup_database(drop=True)


def subscribe():

    for user in users:
        EventProcessor.subscribe("feed", "shayan", user)


def publish():

    for _ in range(500):
        producer_id = choice(users)
        item_id = uuid4().hex
        published_ids.append((item_id, producer_id))
        EventProcessor.add_event({
            'verb': 'podcast', 'producer_id': producer_id, 'timestamp': time(), 'item_id': item_id
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


if __name__ == '__main__':
    init()
    subscribe()
    publish()
    retract()
    unsub_sub()
    print('done')
