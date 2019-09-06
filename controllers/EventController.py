from abc import ABC, abstractmethod
from peewee import chunked
from utils import redis


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

    def clean_excess_from_cache(self, consumer_id):
        """
        clean excess data from the cache
        :param consumer_id: consumer's id
        :return: lua script for this command
        """
        cache_name = self.create_cache_name(consumer_id)
        lua_script = f"""
        local count = redis.call('zcount', {cache_name}, 0, 1000)
	    if count > {self._max_cache} then
	  	    local diff = count - {self._max_cache}
	  	    redis.call('zpopmin', 'scores', diff)
	  	    return diff
	    else 
	        return 0
	    end
        """
        return lua_script

    @staticmethod
    def _calculate_start_end(name, limit, after, before):
        """
        calculate start and end index of list
        :param name: list name
        :param limit: number of elements
        :param after: after id
        :param before: before id
        :return: start, end index
        """
        start, end = 0, 0
        if not after and not before:
            end = limit - 1
        elif after is not None:
            start = redis.zrevrank(name, after) + 1
            end = start + limit - 1
        elif before is not None:
            end = redis.zrevrank(name, before) - 1
            start = end - limit + 1 if end > limit else 0
        else:
            raise Exception('cant have both after and before')
        return start, end

    def consume(self, consumer_id, limit=20, after=None, before=None):
        """
        get data for consumer
        :param consumer_id: consumer's id
        :param limit: number of data to be returned
        :param after: return after (id)
        :param before: return before (id)
        :return: list of { 'id': item_id, 'verb': verb }
        """

        consumer_feed = self.create_cache_name(consumer_id)
        start, end = self._calculate_start_end(consumer_feed, limit, after, before)

        # if consumer feed does not exist, query for creation
        if not redis.exists(consumer_feed):
            self._recreate_user_timeline(consumer_id)

        bin_resp = redis.zrevrange(consumer_feed, start, end)
        response = list(map(lambda x: x.decode(), bin_resp))

        if not response:
            return []

        return (self._dataset
                .select(self._dataset.item_id, self._dataset.verb)
                .where(self._dataset.item_id << response)
                .order_by(self._dataset.timestamp.desc())
                .dicts())

    @abstractmethod
    def _recreate_user_timeline(self, consumer_id):
        raise NotImplementedError()

    @property
    def verbs(self):
        return self._verbs

    @property
    def name(self):
        return self._name


class Flat(BaseEvent):

    def add_event(self, payload):
        """
        add a new event
        :param payload: json payload { producer_id, item_id, timestamp, verb }
        :return: True on success
        """
        # 1. create a new instance and add to database
        self._dataset.create(
            producer_id=payload['producer_id'],
            item_id=payload['item_id'],
            timestamp=payload['timestamp'],
            verb=payload['verb']
        ).save()

        # 2. fan out process
        self._publish_fan_out_from_producer(
            producer_id=payload['producer_id'],
            item_id=payload['item_id'])

        return True

    def retract_event(self, payload):
        """
        remove a new event
        :param payload: json payload { producer_id, item_id }
        :return: True on success
        """

        # 1. delete fan out
        self._delete_fan_out_from_producer(
            producer_id=payload.get('producer_id'),
            item_id=payload.get('item_id'))

        # 2. delete instance from database
        (self._dataset
         .delete()
         .where(
            (self._dataset.producer_id == payload.get('producer_id')) &
            (self._dataset.item_id == payload.get('item_id')))
         .execute())

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

        # 2. broadcast update timeline
        self._add_from_producer_to_consumer(
            producer_id=producer_id,
            consumer_id=consumer_id)

        return True

    def unsubscribe(self, consumer_id, producer_id):
        """
        unsubscribe a consumer from a producer
        :param consumer_id: consumer's id
        :param producer_id: producer's id
        :return: True on success
        """

        # 1. delete from producer for consumer
        self._delete_from_producer_for_consumer(
            producer_id=producer_id,
            consumer_id=consumer_id)

        (self._relations
         .delete()
         .where(
            (self._relations.consumer_id == consumer_id) &
            (self._relations.producer_id == producer_id))
         .execute())

        return True

    def _delete_from_producer_for_consumer(self, producer_id, consumer_id):
        """
        for unsubscribe events.
        :param producer_id: producer's id
        :param consumer_id: consumer's id
        :return: True on success
        """
        # get producer's recent content (need to figure out how many)
        content_ids = (self._dataset
                       .select(self._dataset.item_id)
                       .where((self._dataset.producer_id == producer_id))
                       .namedtuples())

        # inject to consumer's feed list
        pipe = redis.pipeline()
        consumer_feed = self.create_cache_name(consumer_id)
        for content_id in content_ids:
            pipe.zrem(consumer_feed, content_id.item_id)

        pipe.execute()
        return True

    def _add_from_producer_to_consumer(self, producer_id, consumer_id):
        """
        for subscribe events.
        :return: True on success
        """
        # get producer's content
        content = (self._dataset
                   .select(self._dataset.item_id, self._dataset.timestamp)
                   .where((self._dataset.producer_id == producer_id)))

        pipe = redis.pipeline()
        consumer_feed = self.create_cache_name(consumer_id)
        for chunk in chunked(content, 400):
            pipe.zadd(consumer_feed, dict((c.item_id, c.timestamp) for c in chunk))

        pipe.eval(self.clean_excess_from_cache(consumer_id), 0)
        pipe.execute()
        return True

    def _publish_fan_out_from_producer(self, producer_id, item_id):
        """
        for when a consumer publishes new content
        :return: True on success
        """
        # get producer's followers
        followers = (self._relations
                     .select(self._relations.consumer_id)
                     .where(self._relations.producer_id == producer_id)
                     .namedtuples())

        content = self._dataset.get(self._dataset.item_id == item_id)
        content_info = {content.item_id: content.timestamp}

        # inject content id to their list
        pipe = redis.pipeline()
        for follower in followers:
            pipe.zadd(self.create_cache_name(follower.consumer_id), content_info)
            pipe.eval(self.clean_excess_from_cache(follower.consumer_id), 0)

        if self._include_actor:
            pipe.zadd(self.create_cache_name(producer_id), content_info)
            pipe.eval(self.clean_excess_from_cache(producer_id), 0)

        pipe.execute()
        return True

    def _delete_fan_out_from_producer(self, producer_id, item_id):
        """
        for when a producer retracts their content
        :return: True on success
        """
        # get producer's followers
        followers = (self._relations
                     .select(self._relations.consumer_id)
                     .where(self._relations.producer_id == producer_id)
                     .namedtuples())

        # inject content id to their list
        pipe = redis.pipeline()
        for follower in followers:
            pipe.zrem(self.create_cache_name(follower.consumer_id), item_id)

        if self._include_actor:
            pipe.zrem(self.create_cache_name(producer_id), item_id)

        pipe.execute()
        return True

    def _recreate_user_timeline(self, consumer_id):
        """
        for when (server restarts, or a new user logs in)
        :return: True on success
        """

        content = (self._relations
                   .select(self._dataset.item_id, self._dataset.timestamp)
                   .join(self._dataset, on=(self._relations.producer_id == self._dataset.producer_id))
                   .where(self._relations.consumer_id == consumer_id)
                   .order_by(self._dataset.timestamp.desc()).limit(self._max_cache)
                   .namedtuples())

        consumer_feed = self.create_cache_name(consumer_id)
        for chunk in chunked(content, 400):
            redis.zadd(consumer_feed, dict((c.item_id, c.timestamp) for c in chunk))

        if not self._include_actor:
            return True

        content = (self._dataset
                   .select(self._dataset.item_id, self._dataset.timestamp)
                   .where(self._dataset.producer_id == consumer_id))

        pipe = redis.pipeline()
        for chunk in chunked(content, 400):
            pipe.zadd(consumer_id, dict((c.item_id, c.timestamp) for c in chunk))

        pipe.execute()
        return True


class Activity(BaseEvent):

    def add_event(self, payload):
        """
        add a new event
        :param payload: json payload
        :return: True on success
        """
        # 1. create a new instance and add to database
        self._dataset.create(
            producer_id=payload.get('producer_id'),
            consumer_id=payload.get('consumer_id'),
            verb=payload.get('verb'),
            timestamp=payload.get('timestamp'),
            item_id=payload.get('item_id')
        ).save()

        # 2. process fan out
        self._publish_fan_out_from_producer(
            consumer_id=payload.get('consumer_id'),
            item_id=payload.get('item_id'))
        return True

    def retract_event(self, payload):
        """
        retract a new event
        :param payload: json payload
        :return: True on success
        """

        # 1. delete fan out
        self._delete_fan_out_from_producer(
            consumer_id=payload.get('producer_id'),
            item_id=payload.get('item_id'))

        # 2. delete the corresponding instance in database
        (self._dataset
         .delete()
         .where(
            (self._dataset.producer_id == payload.get('producer_id')) &
            (self._dataset.item_id == payload.get('item_id')) &
            (self._dataset.verb == payload.get('verb')) &
            (self._dataset.consumer_id == payload.get('consumer_id')))
         .execute())
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

        # 2. broadcast update timeline
        self._add_from_producer_to_consumer(
            consumer_id=consumer_id,
            producer_id=producer_id)
        return True

    def unsubscribe(self, consumer_id, producer_id):
        """
        unsubscribe a consumer from a producer
        :param consumer_id: consumer's id
        :param producer_id: producer's id
        :return: True on success
        """
        # 1. delete timeline
        self._delete_from_producer_for_consumer(
            consumer_id=consumer_id,
            producer_id=producer_id)

        (self._relations
         .delete()
         .where(
            (self._relations.consumer_id == consumer_id) &
            (self._relations.producer_id == producer_id))
         .execute())

        return True

    def _delete_from_producer_for_consumer(self, consumer_id, producer_id):
        """
        for unsubscribe
        :param consumer_id: consumer's id
        :param producer_id: producer's id
        :return: True on success
        """
        # get items from producer for consumer
        content_ids = (self._dataset
                           .select(self._dataset.item_id)
                           .where(
                                (self._dataset.producer_id == producer_id) &
                                (self._dataset.consumer_id == consumer_id)))

        pipe = redis.pipeline()
        consumer_feed = self.create_cache_name(consumer_id)
        for content_id in content_ids:
            pipe.zrem(consumer_feed, content_id)

        pipe.execute()
        return True

    def _add_from_producer_to_consumer(self, consumer_id, producer_id):
        """
        for subscribe event
        :param consumer_id: consumer's id
        :param producer_id: producer's id
        :return: True on success
        """
        content = (self._dataset
                       .select(self._dataset.item_id, self._dataset.timestamp)
                       .where(
                            (self._dataset.producer_id == producer_id) &
                            (self._dataset.consumer_id == consumer_id)))

        consumer_feed = self.create_cache_name(consumer_id)
        pipe = redis.pipeline()
        for chunk in chunked(content, 400):
            pipe.zadd(consumer_feed, dict((c.item_id, c.timestamp) for c in chunk))

        pipe.eval(self.clean_excess_from_cache(consumer_id), 0)
        pipe.execute()
        return True

    def _publish_fan_out_from_producer(self, consumer_id, item_id):
        """
        for publishing content
        :param consumer_id: consumer's id
        :param item_id: item's id
        :return: True on success
        """

        content = self._dataset.get(self._dataset.item_id == item_id)
        redis.zadd(self.create_cache_name(consumer_id), {content.item_id: content.timestamp})
        redis.eval(self.clean_excess_from_cache(consumer_id), 0)
        return True

    def _delete_fan_out_from_producer(self, consumer_id, item_id):
        """
        for retracting content
        :param consumer_id: consumer's id
        :param item_id: item's id
        :return: True on success
        """
        redis.zrem(self.create_cache_name(consumer_id), item_id)
        return True

    def _recreate_user_timeline(self, consumer_id):
        """
        recreate users timeline
        :param consumer_id: consumer's id
        :return: True on success
        """

        # get all content with consumer_id as target
        content = (self._dataset
                   .select(self._dataset.item_id, self._dataset.timestamp)
                   .where(self._dataset.consumer_id == consumer_id)
                   .order_by(self._dataset.timestamp.desc()).limit(self._max_cache))

        pipe = redis.pipeline()
        consumer_feed = self.create_cache_name(consumer_id)
        for chunk in chunked(content, 400):
            pipe.zadd(consumer_feed, dict((c.item_id, c.timestamp) for c in chunk))

        pipe.execute()
        return True
