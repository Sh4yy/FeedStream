

class BaseEvent:

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
        self._name = name
        self._dataset = dataset
        self._relations = relations
        self._verbs = verbs
        self._include_actor = include_actor
        self._max_cache = max_cache

    @property
    def name(self):
        return self._name

    @property
    def dataset(self):
        return self._dataset

    @property
    def relations(self):
        return self._relations

    @property
    def verbs(self):
        return self._verbs

    @property
    def include_actor(self):
        return self._include_actor

    @property
    def max_cache(self):
        return self._max_cache

    def register(self):
        pass


class Flat(BaseEvent):
    pass


class Activity(BaseEvent):
    pass

