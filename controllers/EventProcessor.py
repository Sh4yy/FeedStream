from controllers.EventController import *
from models import *


class EventProcessor:

    events = []
    event_by_verb = {}
    event_by_name = {}
    task_queue = None

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
    def register_task_queue(cls, task_queue):
        """
        register a new task queue
        :param task_queue: task queue instance
        :return: True on success
        """

        if cls.task_queue:
            return False

        cls.task_queue = task_queue
        return True

    @classmethod
    def preload_data(cls):
        """
        preload data into redis
        :return: True on success
        """

        models = []
        models += FlatEvent.__subclasses__()
        models += ActivityEvent.__subclasses__()

        for model in models:
            for event in model:
                cls.add_event(event.make_json(), save=False)

        return True

    @classmethod
    def consume(cls, event_name, consumer_id, limit=20, after=None, before=None):
        """
        consume for consumer
        :param event_name: event name
        :param consumer_id: consumer's id
        :param limit: number of returned data
        :param after: after specific item
        :param before: before specific item
        :return: [{ item_id, verb }]
        """

        if event_name not in cls.event_by_name:
            raise Exception('event does not exist')

        return (cls.event_by_name[event_name]
                   .consume(consumer_id=consumer_id,
                            limit=limit,
                            after=after,
                            before=before))

    @classmethod
    def add_event(cls, payload, save=True):
        """
        register new event
        :param payload: json payload
        :param save: save event permanently
        :return: True on success
        """
        if 'verb' not in payload:
            raise Exception('invalid payload; missing verb')

        for event_handler in cls.event_by_verb[payload['verb']]:
            job = event_handler.add_event
            cls.task_queue.add_task(job, payload=payload, save=save)

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
            job = event_handler.retract_event
            cls.task_queue.add_task(job, payload=payload)

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

        job = cls.event_by_name[event_name].subscribe
        cls.task_queue.add_task(job, consumer_id=consumer_id, producer_id=producer_id)

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

        job = cls.event_by_name[event_name].unsubscribe
        cls.task_queue.add_task(job, consumer_id=consumer_id, producer_id=producer_id)

        return True
