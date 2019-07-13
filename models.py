from peewee import *


class Relation:

    subscriber = TextField()
    publisher = TextField()


class FlatEvent:

    item_id = TextField(primary_key=True)
    actor = TextField()
    verb = TextField()
    date = DateTimeField()


class ActivityEvent:

    item_id = TextField(primary_key=True)
    actor = TextField()
    object = TextField()
    verb = TextField()
    date = DateTimeField()


