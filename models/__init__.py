from peewee import *
from utils import db


class BaseModel(Model):

    class Meta:
        database = db


class Relation(BaseModel):

    consumer_id = TextField()
    producer_id = TextField()

    class Meta:
        indexes = (
            (('consumer_id', 'producer_id'), True),
        )


class FlatEvent(BaseModel):

    id = AutoField(primary_key=True)
    item_id = TextField(index=True)
    producer_id = TextField()
    verb = TextField()
    timestamp = IntegerField()

    class Meta:
        indexes = (
            (('producer_id', 'item_id', 'verb'), True),
        )

    def make_json(self):
        return {
            "producer_id": self.producer_id,
            "item_id": self.item_id,
            "timestamp": self.timestamp,
            "verb": self.verb
        }


class ActivityEvent(BaseModel):

    id = AutoField(primary_key=True)
    item_id = TextField(index=True)
    consumer_id = TextField()
    producer_id = TextField()
    verb = TextField()
    timestamp = IntegerField()

    class Meta:
        indexes = (
            (('producer_id', 'item_id', 'verb', 'consumer_id'), True),
        )

    def make_json(self):
        return {
            "producer_id": self.producer_id,
            "consumer_id": self.consumer_id,
            "verb": self.verb,
            "timestamp": self.timestamp,
            "item_id": self.item_id
        }

