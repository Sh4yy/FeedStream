from sanic import Blueprint, response
from sanic.exceptions import abort
from schema import Schema, Optional
from controllers import EventProcessor


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
def publish(request):
    """ publish an event """

    if not publish_schema.is_valid(request.json):
        return abort(400, message='invalid request body')

    status = EventProcessor.add_event(request.json)
    return response.json({'ok': True, 'published': status})


@mod.post('/retract')
def retract(request):
    """ retract an event """

    if not retract_schema.is_valid(request.json):
        abort(400, message='invalid request body')

    status = EventProcessor.retract_event(request.json)
    return response.json({'ok': True, 'retracted': status})


@mod.post('/subscribe')
def subscribe(request):
    """ subscribe to a publisher """

    if not subscribe_schema.is_valid(request.json):
        abort(400, message='invalid request body')

    status = EventProcessor.subscribe(
        event_name=request.json['event_name'],
        consumer_id=request.json['consumer_id'],
        producer_id=request.json['producer_id']
    )

    return response.json({'ok': True, 'subscribed': status})


@mod.post('unsubscribe')
def unsubscribe(request):
    """ unsubscribe from a publisher """

    if not unsubscribe_schema.is_valid(request.json):
        abort(400, message='invalid request body')

    status = EventProcessor.unsubscribe(
        event_name=request.json['event_name'],
        consumer_id=request.json['consumer_id'],
        producer_id=request.json['producer_id']
    )

    return response.json({'ok': True, 'unsubscribed': status})


@mod.get('/consume')
def consume(request):
    """ consume a feed by user """

    if not consume_schema.is_valid(request.json):
        abort(400, message='invalid request body')

    # todo implement consume
