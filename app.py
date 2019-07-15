from controllers import Activity, Flat, EventProcessor, TaskQueue
from models import ActivityEvent, FlatEvent, Relation, BaseModel
from sanic import Sanic
from routes import mod
from utils import db


def setup_system():
    """ Setup the aggregation system here """

    # classes that are required
    class UserRelations(Relation):
        """ normal user relations """

    class FeedPosts(FlatEvent):
        """ feed posts dataset """

    class NotificationPosts(ActivityEvent):
        """ notification items data set """

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


def setup_workers():
    """ Setup task queue and workers """

    task_queue = TaskQueue(workers=1)
    EventProcessor.register_task_queue(task_queue)
    task_queue.start_workers()


def setup_database():
    """ setup cache and database """

    db.create_tables(BaseModel.__subclasses__())


def setup_web_server():
    """ setup the web server """

    setup_system()
    setup_workers()
    setup_database()

    app = Sanic(__name__)
    app.blueprint(mod)
    return app
