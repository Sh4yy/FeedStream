import requests


class BaseEventStream:

    def __init__(self, host, port, version: str = 'v1'):
        """
        initialize a new FeedStreamClient
        :param host: client's host
        :param port: client's port
        :param version: version of the api
        """
        self._host = host
        self._port = port
        self._version = version

    def _post_request(self, method, payload: dict):
        """
        make a new post request
        :param method: request method
        :param payload: json payload
        :return: response json
        """

        url = f"http://{self._host}:{self._port}/{self._version}/{method}"
        return requests.post(url, json=payload).json()

    def _get_request(self, method, args):
        """
        make a new get request
        :param method: request methods
        :param args: request arguments
        :return: response json
        """
        url = f"http://{self._host}:{self._port}/{self._version}/{method}"
        return requests.get(url, params=args).json()

    def _publish(self, producer_id: str, item_id: str, verb: str,
                 timestamp: int, consumer_id: str = None):
        """
        publish a new item
        :param producer_id: producer's unique id
        :param item_id: published item id
        :param verb: published item's verb
        :param timestamp: published timestamp (int)
        :param consumer_id: consumer's id (for activity events)
        :return: True on success
        """

        payload = {
            "producer_id": producer_id,
            "item_id": item_id,
            "timestamp": timestamp,
            "verb": verb,
        }

        if consumer_id is not None:
            payload['consumer_id'] = consumer_id

        response = self._post_request('publish', payload=payload)
        return response['published']

    def _retract(self, producer_id: str, item_id: str, verb: str,
                 consumer_id: str = None):
        """
        retract a published item
        :param producer_id: producer's unique id
        :param item_id: published item's id
        :param verb: published item's verb
        :param consumer_id: consumer's id (for activity events)
        :return: True on success
        """

        payload = {
            "producer_id": producer_id,
            "item_id": item_id,
            "verb": verb
        }

        if consumer_id is not None:
            payload['consumer_id'] = consumer_id

        response = self._post_request('retract', payload=payload)
        return response['retracted']

    def _subscribe(self, event_name: str, producer_id: str, consumer_id: str):
        """
        subscribe a consumer to a producer
        :param event_name: feed's name
        :param producer_id: producer's unique id
        :param consumer_id: consumer's unique id
        :return: True on success
        """

        payload = {
            "event_name": event_name,
            "producer_id": producer_id,
            "consumer_id": consumer_id
        }

        response = self._post_request('subscribe', payload=payload)
        return response['subscribed']

    def _unsubscribe(self, event_name: str, producer_id: str, consumer_id: str):
        """
        unsubscribe a consumer from a producer
        :param event_name: feed's name
        :param producer_id: producer's unique id
        :param consumer_id: consumer's unique id
        :return: True on scucess
        """

        payload = {
            "event_name": event_name,
            "producer_id": producer_id,
            "consumer_id": consumer_id
        }

        response = self._post_request('unsubscribe', payload=payload)
        return response['unsubscribe']

    def _consume(self, event_name: str, consumer_id: str, limit: int = 20,
                 after: str = None, before: str = None):
        """
        consume a feed for a user
        :param event_name: feed's name
        :param consumer_id: consumer's id
        :param limit: limit on result
        :param after: result after specific id
        :param before: result before specific id
        :return: list of {'item_id', 'verb' }
        """

        args = {
            "event_name": event_name,
            "consumer_id": consumer_id,
            "limit": limit
        }

        if after is not None:
            args['after'] = after
        if before is not None:
            args['before'] = before

        response = self._get_request('consume', args=args)
        return response['data']


class FlatEventStream(BaseEventStream):

    def __init__(self, event_name, host, port):
        """
        initialize a custom flat event stream
        :param event_name: event's name
        :param host: service host
        :param port: service port
        """
        super(BaseEventStream).__init__(self, host, port)
        self._event_name = event_name

    def publish(self, producer_id: str, item_id: str, verb: str, timestamp: int):
        """
        publish a new item
        :param producer_id: producer's unique id
        :param item_id: published item id
        :param verb: published item's verb
        :param timestamp: published timestamp (int)
        :return: True on success
        """

        return self._publish(producer_id, item_id, verb, timestamp)

    def retract(self, producer_id: str, item_id: str, verb: str):
        """
        retract a published item
        :param producer_id: producer's unique id
        :param item_id: published item's id
        :param verb: published item's verb
        :return: True on success
        """

        return self._retract(producer_id, item_id, verb)

    def subscribe(self, producer_id: str, consumer_id: str):
        """
        subscribe a consumer to a producer
        :param producer_id: producer's unique id
        :param consumer_id: consumer's unique id
        :return: True on success
        """

        return self._subscribe(self._event_name, producer_id, consumer_id)

    def unsubscribe(self, producer_id: str, consumer_id: str):
        """
        unsubscribe a consumer from a producer
        :param producer_id: producer's unique id
        :param consumer_id: consumer's unique id
        :return: True on scucess
        """

        return self._unsubscribe(self._event_name, producer_id, consumer_id)

    def consume(self, consumer_id: str, limit: int = 20,
                 after: str = None, before: str = None):
        """
        consume a feed for a user
        :param consumer_id: consumer's id
        :param limit: limit on result
        :param after: result after specific id
        :param before: result before specific id
        :return: list of {'item_id', 'verb' }
        """

        return self._consume(self._event_name, consumer_id, limit, after, before)


class ActivityEventStream(BaseEventStream):

    def __init__(self, event_name, host, port):
        """
        initialize a custom flat event stream
        :param event_name: event's name
        :param host: service host
        :param port: service port
        """
        super(BaseEventStream).__init__(self, host, port)
        self._event_name = event_name

    def publish(self, producer_id: str, item_id: str, verb: str, timestamp: int, consumer_id: str):
        """
        publish a new item
        :param producer_id: producer's unique id
        :param item_id: published item id
        :param verb: published item's verb
        :param timestamp: published timestamp (int)
        :param consumer_id: consumer's id (for activity events)
        :return: True on success
        """

        return self._publish(producer_id, item_id, verb, timestamp, consumer_id)

    def retract(self, producer_id: str, item_id: str, verb: str, consumer_id: str):
        """
        retract a published item
        :param producer_id: producer's unique id
        :param item_id: published item's id
        :param verb: published item's verb
        :param consumer_id: consumer's id (for activity events)
        :return: True on success
        """

        return self._retract(producer_id, item_id, verb, consumer_id)

    def subscribe(self, producer_id: str, consumer_id: str):
        """
        subscribe a consumer to a producer
        :param producer_id: producer's unique id
        :param consumer_id: consumer's unique id
        :return: True on success
        """

        return self._subscribe(self._event_name, producer_id, consumer_id)

    def unsubscribe(self, producer_id: str, consumer_id: str):
        """
        unsubscribe a consumer from a producer
        :param producer_id: producer's unique id
        :param consumer_id: consumer's unique id
        :return: True on scucess
        """

        return self._unsubscribe(self._event_name, producer_id, consumer_id)

    def consume(self, consumer_id: str, limit: int = 20,
                after: str = None, before: str = None):
        """
        consume a feed for a user
        :param consumer_id: consumer's id
        :param limit: limit on result
        :param after: result after specific id
        :param before: result before specific id
        :return: list of {'item_id', 'verb' }
        """

        return self._consume(self._event_name, consumer_id, limit, after, before)
