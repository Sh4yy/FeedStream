from .OrangeDB import Orange
from peewee import PostgresqlDatabase
from redis import StrictRedis


config = Orange('config.json', auto_dump=True, load=True)

redis = StrictRedis(
    host=config['redis']['host'],
    port=config['redis']['port'],
    password=config['redis'].get('password'))

db = PostgresqlDatabase(
    config['database']['name'],
    host=config['database']['host'],
    port=config['database']['port'],
    user=config['database']['user'],
    passowrd=config['database'].get('password'))
