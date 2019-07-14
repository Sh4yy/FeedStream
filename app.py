from controllers import Activity, Flat
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
    Flat(name='feed', dataset=FeedPosts,
         relations=UserRelations, verbs=['podcast'],
         include_actor=True, max_cache=500).register()

    Activity(name='notification', dataset=NotificationPosts,
             relations=UserRelations, verbs=['like', 'follow', 'comment', 'mention'],
             include_actor=False, max_cache=200).register()


def setup_database():
    pass


def setup_web_server():
    pass


