from .OrangeDB import Orange
from peewee import PostgresqlDatabase
from redis import Redis


config = Orange('config.json', auto_dump=True, load=True)

redis = Redis(
    host=config['redis']['host'],
    port=config['redis']['port'])

db = PostgresqlDatabase(
    config['database']['name'],
    host=config['database']['host'],
    port=config['database']['port'],
    user=config['database']['user'])
