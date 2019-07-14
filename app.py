from controllers import Activity, Flat, EventProcessor, TaskQueue
from models import ActivityEvent, FlatEvent, Relation


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


def setup_web_server():
    pass


