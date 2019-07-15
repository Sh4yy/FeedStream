from peewee import *
from utils import db


class BaseModel(Model):

    class Meta:
        db = db


class Relation(Model):

    consumer_id = TextField()
    producer_id = TextField()

    class Meta:
        indexes = (
            (('consumer_id', 'producer_id'), True),
        )


class FlatEvent(Model):

    id = AutoField(primary_key=True)
    item_id = TextField()
    producer_id = TextField()
    verb = TextField()
    timestamp = TimestampField()

    class Meta:
        indexes = (
            (('producer_id', 'item_id', 'verb'), True)
        )


class ActivityEvent(Model):

    id = AutoField(primary_key=True)
    item_id = TextField()
    consumer_id = TextField()
    producer_id = TextField()
    verb = TextField()
    timestamp = TimestampField()

    class Meta:
        indexes = (
            (('producer_id', 'item_id', 'verb', 'consumer_id'), True)
        )

