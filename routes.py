from sanic import Blueprint
from schema import Schema, Optional

mod = Blueprint('routes', version=1)


publish_schema = Schema({
    'verb': str, 'producer_id': str, 'item_id': str, 'timestamp': float, Optional('consumer_id'): str
})

retract_schema = Schema({
    'verb': str, 'producer_id': str, 'event_id': str, Optional('consumer_id'): str
})

consume_schema = Schema({
    'consumer_id': str, Optional('before'): str, Optional('after'): str, Optional('limit'): int
})

subscribe_schema = Schema({
    'consumer_id': str, 'producer_id': str, 'event_name': str
})

unsubscribe_schema = Schema({
    'consumer_id': str, 'producer_id': str, 'event_name': str
})


@mod.post('/publish')
def publish():
    """ publish an event """
    # { actor, event_id, timestamp, verb }
    # { actor, event_id, timestamp, verb, target_id }
    pass


@mod.post('/retract')
def retract():
    """ retract an event """
    # { actor, verb, event_id }
    pass


@mod.get('/consume')
def consume():
    # { consumer_id, feed_name, before, after, limit }
    """ consume a feed by user """
    pass


@mod.post('/subscribe')
def subscribe():
    """ subscribe to a publisher """
    # { consumer_id, producer_id, feed_name }
    pass


@mod.post('unsubscribe')
def unsubscribe():
    """ unsubscribe from a publisher """
    # { consumer_id, producer_id, feed_name }
    pass
