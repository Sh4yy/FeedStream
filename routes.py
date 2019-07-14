

def publish():
    """ publish an event """
    # { actor, event_id, timestamp, verb }
    # { actor, event_id, timestamp, verb, target_id }
    pass


def retract():
    """ retract an event """
    # { actor, verb, event_id }
    pass


def consume():
    # { consumer_id, feed_name, before, after, limit }
    """ consume a feed by user """
    pass


def subscribe():
    """ subscribe to a publisher """
    # { consumer_id, producer_id, feed_name }
    pass


def unsubscribe():
    """ unsubscribe from a publisher """
    # { consumer_id, producer_id, feed_name }
    pass
