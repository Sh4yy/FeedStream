from .OrangeDB import Orange
from peewee import PostgresqlDatabase
from redis import StrictRedis


config = Orange('config.json', auto_dump=True, load=True)

redis = StrictRedis(
    db=2,
    host=config['redis']['host'],
    port=config['redis']['port'],
    password=config['redis'].get('password'))

db = PostgresqlDatabase(
    config['database']['name'],
    host=config['database']['host'],
    port=config['database']['port'],
<<<<<<< HEAD
    user=config['database']['user'],
    passowrd=config['database'].get('password'))
=======
    user=config['database']['user'])


def clear_cache_ns(ns):
    """
    Clears a namespace in redis cache.
    This may be very time consuming.
    :param ns: str, namespace i.e your:prefix*
    :return: int, num cleared keys
    """
    count = 0
    pipe = redis.pipeline()
    for key in redis.scan_iter(ns):
        pipe.delete(key)
        count += 1
    pipe.execute()
    return count
>>>>>>> 6c5da3ddf434825d1f91e3e8f2a443db92043a02
