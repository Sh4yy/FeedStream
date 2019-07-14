from abc import ABC, abstractmethod
from peewee import chunked
from redis import Redis


redis = Redis()


class BaseEvent(ABC):

    def __init__(self, name, dataset, relations, verbs, include_actor, max_cache):
        """
        register an event controller
        :param name: name of the event
        :param dataset: storage dataset
        :param relations: producer/consumer relation
        :param verbs: event verbs
        :param include_actor: include producer's data in their own feed
        :param max_cache: max number of cached events
        """
        self._name = name.lower()
        self._dataset = dataset
        self._relations = relations
        self._verbs = [verb.lower() for verb in verbs]
        self._include_actor = include_actor
        self._max_cache = max_cache

    @abstractmethod
    def add_event(self, payload):
        raise NotImplementedError()

    @abstractmethod
    def retract_event(self, payload):
        raise NotImplementedError()

    @abstractmethod
    def subscribe(self, consumer_id, producer_id):
        raise NotImplementedError()

    @abstractmethod
    def unsubscribe(self, consumer_id, producer_id):
        raise NotImplementedError()

    def create_cache_name(self, id):
        """
        create cache name for an id
        :param id: target id
        :return: string cache name
        """
        return f"{id}:{self.name}"

    @property
    def verbs(self):
        return self._verbs

    @property
    def name(self):
        return self.name


class Flat(BaseEvent):

    def add_event(self, payload):
        """
        add a new event
        :param payload: json payload { actor_id, item_id, timestamp, verb }
        :return: True on success
        """
        # 1. create a new instance and add to database
        self._dataset.create(
            actor_id=payload.get('actor_id'),
            item_id=payload.get('item_id'),
            timestamp=payload.get('timestamp'),
            verb=payload.get('verb')
        ).save()

        # TODO 2. queue fan out process
        return True

    def retract_event(self, payload):
        """
        remove a new event
        :param payload: json payload { actor_id, item_id }
        :return: True on success
        """
        # 1. delete instance from database
        (self._dataset
         .delete()
         .where(
            (self._dataset.actor_id == payload.get('actor_id')) &
            (self._dataset.item_id == payload.get('item_id')))
         .execute())
        # todo 2. queue fan out
        return True

    def subscribe(self, consumer_id, producer_id):
        """
        subscribe a consumer to a producer
        :param consumer_id:
        :param producer_id:
        :return: True on success
        """
        # 1. create a new instance of follow
        self._relations.create(
            producer_id=producer_id,
            consumer_id=consumer_id
        ).save()
        # 2. todo broadcast update timeline
        return True

    def unsubscribe(self, consumer_id, producer_id):
        """
        unsubscribe a consumer from a producer
        :param consumer_id: consumer's id
        :param producer_id: producer's id
        :return: True on success
        """
        (self._relations
         .delete()
         .where(
            (self._relations.consumer_id == consumer_id) &
            (self._relations.producer_id == producer_id))
         .execute())
        # 2. todo broadcast update timeline
        return True

    def _delete_from_producer_for_consumer(self, producer_id, consumer_id):
        """
        for unsubscribe events.
        :param producer_id: producer's id
        :param consumer_id: consumer's id
        :return: True on success
        """
        # get producer's recent content (need to figure out how many)
        content_id = (self._dataset
                          .select(self._dataset.item_id)
                          .where((self._dataset.actor_id == producer_id)))

        # inject to consumer's feed list
        consumer_feed = self.create_cache_name(consumer_id)
        for chunk in chunked(content_id, 400):
            redis.zrem(consumer_feed, chunk)

        return True

    def _add_from_producer_to_consumer(self, producer_id, consumer_id):
        """
        for subscribe events.
        :return: True on success
        """
        # get producer's content
        content = (self._dataset
                   .select(self._dataset.item_id, self._dataset.timestamp)
                   .where((self._dataset.actor_id == producer_id))
                   .namedtuple())

        consumer_feed = self.create_cache_name(consumer_id)
        for chunk in chunked(content, 400):
            redis.zadd(consumer_feed, dict((c.item_id, c.timestamp) for c in chunk))

        return True

    def _publish_fan_out_from_producer(self, producer_id, item_id):
        """
        for when a consumer publishes new content
        :return: True on success
        """
        # get producer's followers
        followers = (self._relations
                         .select(self._relations.subscriber_id)
                         .where(self._relations.producer_id == producer_id))

        content = self._dataset.get(self._dataset.item_id == item_id)
        content_info = {content.item_id, content.timestamp}

        # inject content id to their list
        for follower in followers:
            redis.zadd(self.create_cache_name(follower), content_info)

        if self._include_actor:
            redis.zadd(self.create_cache_name(producer_id), content_info)

        return True

    def _delete_fan_out_from_producer(self, producer_id, item_id):
        """
        for when a producer retracts their content
        :return: True on success
        """
        # get producer's followers
        followers = (self._relations
                     .select(self._relations.subscriber_id)
                     .where(self._relations.producer_id == producer_id))

        # inject content id to their list
        for follower in followers:
            redis.zrem(self.create_cache_name(follower), [item_id])

        if self._include_actor:
            redis.zrem(self.create_cache_name(producer_id), [item_id])

        return True

    def _recreate_user_timeline(self, consumer_id):
        """
        for when (server restarts, or a new user logs in)
        :return: True on success
        """
        # get following producers content
        content = (self._dataset
                       .select(self._dataset.item_id, self._dataset.timestamp)
                       .join(self._relations, on=self._relations.producer_id == self._dataset.actor_id)
                       .where(self._relations.consumer_id == consumer_id)
                       .namedtuple())

        # inject into user's list
        consumer_feed = self.create_cache_name(consumer_id)
        for chunk in chunked(content, 400):
            redis.zadd(consumer_feed, dict((c.item_id, c.timestamp) for c in chunk))

        if not self._include_actor:
            return True

        content = (self._dataset
                       .select(self._dataset.item_id, self._dataset.timestamp)
                       .where(self._dataset.actor_id == consumer_id)
                       .namedtuple())

        for chunk in chunked(content, 400):
            redis.zadd(consumer_id, dict((c.item_id, c.timestamp) for c in chunk))

        return True


class Activity(BaseEvent):

    def add_event(self, payload):
        """
        add a new event
        :param payload: json payload { actor_id, item_id, verb, target_id, timestamp }
        :return: True on success
        """
        # 1. create a new instance and add to database
        self._dataset.create(
            actor_id=payload.get('actor_id'),
            item_id=payload.get('item_id'),
            verb=payload.get('verb'),
            target_id=payload.get('target_id'),
            timestamp=payload.get('timestamp')
        ).save()
        # todo 2. queue fan out process
        return True

    def retract_event(self, payload):
        """
        retract a new event
        :param payload: json payload { actor_id, item_id, verb, target_id }
        :return: True on success
        """
        # 1. delete the corresponding instance in database
        (self._dataset
         .delete()
         .where(
             (self._dataset.actor_id == payload.get('actor_id')) &
             (self._dataset.item_id == payload.get('item_id')) &
             (self._dataset.verb == payload.get('verb')) &
             (self._dataset.target_id == payload.get('target_id')))
         .execute())
        # todo queue fan out removal process
        return True

    def subscribe(self, consumer_id, producer_id):
        """
        subscribe a consumer to a producer
        :param consumer_id: consumer's id
        :param producer_id: producer's id
        :return: True on success
        """
        # 1. create a new instance of follow
        self._relations.create(
            producer_id=producer_id,
            consumer_id=consumer_id
        ).save()
        # 2. todo broadcast update timeline
        return True

    def unsubscribe(self, consumer_id, producer_id):
        """
        unsubscribe a consumer from a producer
        :param consumer_id: consumer's id
        :param producer_id: producer's id
        :return: True on success
        """
        (self._relations
         .delete()
         .where(
            (self._relations.consumer_id == consumer_id) &
            (self._relations.producer_id == producer_id))
         .execute())
        # 2. todo broadcast update timeline
        return True

    def _delete_from_producer_for_consumer(self):
        # for unsubscribe
        pass

    def _add_from_producer_to_consumer(self):
        # for subscribe
        pass

    def _publish_fan_out_from_producer(self):
        # for publishing content
        pass

    def _delete_fan_out_from_producer(self):
        # for retracting content
        pass

    def _recreate_user_timeline(self):
        # to recreate a timeline
        pass


class EventProcessor:

    events = []
    event_by_verb = {}
    event_by_name = {}

    @classmethod
    def register_event_handler(cls, event: BaseEvent):
        """
        register a new event
        :param event: new event
        :return: True on success
        """
        cls.events.append(event)
        cls.event_by_name[event.name] = event
        for verb in event.verbs:
            cls.event_by_verb.setdefault(verb, [])
            cls.event_by_verb[verb].append(event)

    @classmethod
    def add_event(cls, payload):
        """
        register new event
        :param payload: json payload
        :return: True on success
        """
        if 'verb' not in payload:
            raise Exception('invalid payload; missing verb')

        for event_handler in cls.event_by_verb[payload['verb']]:
            event_handler.add_event(payload)

        return True

    @classmethod
    def retract_event(cls, payload):
        """
        retract an event
        :param payload: json payload
        :return: True on success
        """

        if 'verb' not in payload:
            raise Exception('invalid payload; missing verb')

        for event_handler in cls.event_by_verb[payload['verb']]:
            event_handler.retract_event(payload)

        return True

    @classmethod
    def subscribe(cls, event_name, consumer_id, producer_id):
        """
        subscribe follower to producer
        :param event_name: event's name
        :param consumer_id: consumer's id
        :param producer_id: producer's id
        :return: True on success
        """

        if event_name not in cls.event_by_name:
            raise Exception('invalid event name')

        cls.event_by_name[event_name].subscribe(consumer_id, producer_id)
        return True

    @classmethod
    def unsubscribe(cls, event_name, consumer_id, producer_id):
        """
        unsubscribe follower from producer
        :param event_name: event's name
        :param consumer_id: consumer's id
        :param producer_id: producer's id
        :return: True on success
        """

        if event_name not in cls.event_by_name:
            raise Exception('invalid event name')

        cls.event_by_name[event_name].unsubscribe(consumer_id, producer_id)
        return True
