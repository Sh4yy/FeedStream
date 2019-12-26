from app import setup_database, setup_workers, setup_system, db, BaseModel
from controllers import *
from time import time, sleep
from random import choice, sample, randint
from utils import redis
import unittest
from uuid import uuid4

item_id = 0


def create_users(count):
    return list(map(lambda x: uuid4().hex, range(count)))


def create_event(verb, publisher_id, consumer_id=None):
    global item_id

    item_id += 1
    timestamp = time() - randint(10, 1000)
    return {
        'verb': verb, 'producer_id': publisher_id,
        'timestamp': timestamp, 'item_id': item_id,
        'consumer_id': consumer_id
    }


def clear_ns(ns=''):
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


class TestSubscribe(unittest.TestCase):

    users = create_users(5)

    def test_subscribe(self):

        for user in self.users:
            self.assertTrue(EventProcessor.subscribe("feed", user, "publisher_id"))

        for user in self.users:
            self.assertRaises(Exception, EventProcessor.subscribe, "invalid_event", user, "publisher_id")

    def test_unsubscribe(self):

        for user in self.users:
            self.assertTrue(EventProcessor.unsubscribe("feed", user, "publisher_id"))

        for user in self.users:
            self.assertRaises(Exception, EventProcessor.unsubscribe, "invalid_event", user, "publisher_id")


class TestPublish(unittest.TestCase):

    users = create_users(5)
    publisher = "publisher_id"
    event_ids = []
    events = []

    def test_publishing(self):

        event_count = 10

        for user in self.users:
            EventProcessor.subscribe("feed", user, self.publisher)

        for _ in range(event_count):
            event = create_event('tweet', self.publisher)
            self.events.append(event)
            self.event_ids.append(event['item_id'])
            EventProcessor.add_event(event)

        sleep(1)

        for user in self.users:
            events = list(EventProcessor.consume('feed', user))
            self.assertEqual(len(events), event_count)

            for event in events:
                self.assertTrue(int(event['item_id']) in self.event_ids)

    def test_unsubscribe(self):

        user = self.users.pop()
        EventProcessor.unsubscribe("feed", user, self.publisher)

        sleep(1)

        events = list(EventProcessor.consume("feed", user))
        self.assertEqual(len(events), 0)

    def test_retract(self):

        event = self.events.pop()
        self.event_ids.remove(event['item_id'])
        EventProcessor.retract_event(event)

        sleep(1)

        for user in self.users:
            events = list(EventProcessor.consume('feed', user))
            self.assertEqual(len(events), len(self.events))

            for event in events:
                self.assertTrue(int(event['item_id']) in self.event_ids)

    def test_new_subscriber(self):

        user = create_users(1)[0]
        self.assertTrue(EventProcessor.subscribe('feed', user, self.publisher))

        sleep(1)

        events = list(EventProcessor.consume('feed', user))
        self.assertEqual(len(events), len(self.events))

        for event in events:
            self.assertTrue(int(event['item_id']) in self.event_ids)


class TestActivity(unittest.TestCase):

    publisher_1 = "publisher_id_1"
    publisher_2 = "publisher_id_2"
    users = create_users(10)
    events = []

    def test_subscribe(self):

        for user in self.users:
            self.assertTrue(EventProcessor.subscribe('notification', user, self.publisher_1))
            self.assertTrue(EventProcessor.subscribe('notification', user, self.publisher_2))

    def test_publish(self):

        user = self.users.pop()
        self.events = []
        for publisher in [self.publisher_1, self.publisher_2]:
            for verb in ['follow', 'like', 'comment', 'mention']:
                event = create_event(verb, publisher, consumer_id=user)
                self.events.append(event)
                EventProcessor.add_event(event)

        sleep(1)

        notifications = list(EventProcessor.consume('notification', user, limit=10))

        self.assertEqual(len(notifications), len(self.events))
        for item in notifications:
            self.assertTrue(int(item['item_id']) in list(map(lambda x: x['item_id'], self.events)))

        self.assertTrue(EventProcessor.unsubscribe('notification', user, self.publisher_1))
        limited_events = list(filter(lambda x: x['producer_id'] != self.publisher_1, self.events))

        sleep(1)

        notifications = list(EventProcessor.consume('notification', user, limit=10))
        self.assertEqual(len(limited_events), len(notifications))

    def test_retract(self):

        user = self.users.pop()

        self.events = []
        for publisher in [self.publisher_1, self.publisher_2]:
            for verb in ['follow', 'like', 'comment', 'mention']:
                event = create_event(verb, publisher, consumer_id=user)
                self.events.append(event)
                EventProcessor.add_event(event)

        sleep(1)

        for event in sample(self.events, k=4):
            self.events.remove(event)
            self.assertTrue(EventProcessor.retract_event(event))

        sleep(1)

        notifications = list(EventProcessor.consume('notification', user, limit=10))

        self.assertEqual(len(notifications), len(self.events))
        for item in notifications:
            self.assertTrue(int(item['item_id']) in list(map(lambda x: x['item_id'], self.events)))


if __name__ == '__main__':

    clear_ns()
    setup_system()
    setup_workers(2)
    setup_database(drop=True)

    unittest.main()
