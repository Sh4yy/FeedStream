from peewee import *


class BaseModel(Model):

    class Meta:
        db = None


class Relation(Model):

    subscriber = TextField()
    publisher = TextField()

    class Meta:
        indexes = (
            (('subscriber', 'publisher'), True),
        )


class FlatEvent(Model):

    item_id = TextField(primary_key=True)
    actor = TextField()
    verb = TextField()
    date = DateTimeField()

    class Meta:
        indexes = (
            (('actor', 'item_id', 'verb'), True)
        )


class ActivityEvent(Model):

    item_id = TextField(primary_key=True)
    actor = TextField()
    object = TextField()
    verb = TextField()
    date = DateTimeField()

    class Meta:
        indexes = (
            (('actor', 'item_id', 'verb', 'object'), True)
        )

