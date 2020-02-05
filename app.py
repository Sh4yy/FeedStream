from controllers import Activity, Flat, EventProcessor, TaskQueue
from models import ActivityEvent, FlatEvent, Relation, BaseModel
from sanic import Sanic
from routes import mod
from utils import db, clear_cache_ns


# classes that are required
class UserRelations(Relation):
    """ normal user relations """


class FeedPosts(FlatEvent):
    """ feed posts dataset """


class NotificationPosts(ActivityEvent):
    """ notification items data set """


def setup_system():
    """ Setup the aggregation system here """

    # register feeds
    (EventProcessor.register_event_handler(
        Flat(name='feed', dataset=FeedPosts,
             relations=UserRelations, verbs=['podcast'],
             include_actor=True, max_cache=500)
    ))

    (EventProcessor.register_event_handler(
        Activity(name='notification', dataset=NotificationPosts,
                 relations=UserRelations, verbs=['like', 'follow', 'comment', 'mention'],
                 include_actor=False, max_cache=200)
    ))


def setup_workers(workers=1):
    """ Setup task queue and workers """

    task_queue = TaskQueue(workers=workers)
    EventProcessor.register_task_queue(task_queue)
    task_queue.start_workers()


def preload_data():
    """ preloads redis with server data """

    clear_cache_ns('fs')
    EventProcessor.preload_data()


def setup_database(drop=False):
    """ setup cache and database """

    try:
        db.connect()
    except Exception as e:
        print(e)

    if drop:
        db.drop_tables(Relation.__subclasses__())
        db.drop_tables(FlatEvent.__subclasses__())
        db.drop_tables(ActivityEvent.__subclasses__())

    db.create_tables(Relation.__subclasses__())
    db.create_tables(FlatEvent.__subclasses__())
    db.create_tables(ActivityEvent.__subclasses__())


def setup_web_server(workers=1):
    """ setup the web server """

    setup_system()
    setup_workers(workers)
    setup_database(drop=False)
    preload_data()

    app = Sanic(__name__)
    app.blueprint(mod)
    return app
