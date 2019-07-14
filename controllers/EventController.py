from abc import ABC, abstractmethod


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
        pass

    @abstractmethod
    def retract_event(self, payload):
        pass

    @abstractmethod
    def subscribe(self, consumer_id, producer_id):
        pass

    @abstractmethod
    def unsubscribe(self, consumer_id, producer_id):
        pass

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
