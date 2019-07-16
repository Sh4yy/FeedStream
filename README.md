# FeedStream

This is my implementation of a chronological based timeline/feed stream. This service allows you to design and customize multiple different stream feeds based on your app or website's requirements.

### Customization
The current customization process takes place in the `app.py` file located at the main directory of this project. You will have to modify `setup_system` method for customizing your feed and `setup_workers` method to modify the number of background workers.

### Available Event Types
There are two available types of supported events at the moment, `Flat` and `Activity` events. Both of these two types can include and aggregate as many possible events of different types depending on their use case and each server can process as many different feeds as one requires at once.

#### Flat Events
Flat events are the ones that require a direct relationship between the producers and consumers. The consumer gets to subscribe to as many producers as they want and the content that is published by the producers will directly be available in the consumers' timeline in cronological order for that specific event. A familiar example for `Flat` events can be Twitter's timeline or Instagram's user feed where the users get to follow another set of users and get their content in their timeline (This excludes Twitter's and Instagram's new feed algorithm).

To setup a new Flat event, you may create a new instance of `Flat` in the setup_system method mentioned earlier. Currently, Flat accepts, `name`, `dataset`, `relations`, `verbs` `include_actor`, and `max_cache` parameters. Each event requires a Relation and a FlatEvent database table to store relations and the produced data, both of which can be subclassed from the provided base classes `Relation`, `FlatEvent`.

``` python
class UserRelations(Relation):
    """ user relations database table """

class FeedPosts(FlatEvent):
    """ feed posts dataset """

feed = Flat(name='feed', dataset=FeedPosts,
            relations=UserRelations, verbs=['tweet'],
            include_actor=True, max_cache=500)
```


#### Activity Events
Activity events are events that are produced by a producer and are targeted directly towards a consumer. As an example, Instagram's notification system where a user can see activities that are related to them, such as mentions, comments, likes, and follows by other producers can be implemented using Activity Events. Just like Flat events, Activity events would require a dataset and a relation database, however, if the relations are corresponding to another Flat event, the same relation table could be used. 

``` python
class NotificationPosts(ActivityEvent):
    """ notifications dataset """

notification = Activity(name='notification', dataset=NotificationPosts,
                        relations=UserRelations, include_actor=False,
                        verbs=['like', 'follow', 'comment', 'mention'],
                        max_cache=200)
```

#### Verbs
Since there could be many different types of produced content from the producers, each Event stream requires to know which ones it needs to process. A verb defines the type of activity that is done by the producer and it is used to differentiate the `item_id` from one another in an aggregated event stream. As you can see, in the provided code, our feed is an aggregation of tweets, thus, it would process any event posted that has the verb `tweet`. Our notification, however, is an aggregation of `follow`, `comment`, `like`, and `mentions` events and will process any event including one of those verbs.

#### Event Registration
Finally, we will have to register each of these events to our `EventProcessor` in order to let the service take care of the rest of the setup process.

``` python
# register our feed handler
EventProcessor.register_event_handler(feed)

# register our notification handler
EvetProcessor.register_event_handler(notification)
```

###### And that's it! You can add or change the current event streams based on your own requirements.
Note that this service does not take care of the `Justin Bieber` problem, mainly because I don't yet have a user base that large for it to be a concern of mine, however, once I get there, I will make sure to take care of it.

### API Docs
This service is meant to be ran as a separate service and therefore provides a restful api for communication.

#### Subscribe
Subscribe a consumer to a producer.\
**Route**: `/v1/subscribe`\
**Method** : `POST`\
**Body**:
```json
{
    "event_name": "feed",
    "consumer_id": "shayan",
    "producer_id": "joerogan"
}
```
**Response**:
```json
{
    "ok": true,
    "subscribed": true
}
```
#### Unsubscribe
Unsubscribe a consumer from a producer.\
**Route**: `/v1/unsubscribe`\
**Method** : `POST`\
**Body**:
```json
{
    "event_name": "feed",
    "consumer_id": "shayan",
    "producer_id": "joerogan"
}
```
**Response**:
```json
{
    "ok": true,
    "unsubscribed": true
}
```
#### Publish
Publish an event by a producer.\
**Route**: `/v1/publish`\
**Method** : `POST`\
**Body**:
```json
{
    "verb": "tweet",
    "timestamp": 1563221022,
    "producer_id": "joerogan",
    "item_id": "tweet_123",
}
```
Note that, if you are publishing an Activity event, you are required to add an extra `consumer_id` field to the body of this request\
**Response**:
```json
{
    "ok": true,
    "published": true
}
```
#### Retract
Retract an event previously published by a producer.\
**Route**: `/v1/retract`\
**Method** : `POST`\
**Body**:
```json
{
    "verb": "tweet",
    "producer_id": "joerogan",
    "item_id": "tweet_123",
}
```
Note that just like publish, if this is an Activity event, you must provide a `consumer_id` field inside the body\
**Response**:
```json
{
    "ok": true,
    "retracted": true
}
```
#### Consume
Consume a custom event for a consumer.\
**Route**: `/v1/consume`\
**Method** : `GET`\
**request arguments**:
```json
{
    "event_name": "feed",
    "consumer_id": "shayan",
    "limit": 5
}
```
You can also use the `before` and `after` arguments with a specific item_id to get the events occurring after or before the provided item. Can be used for scrolling or updating the feed.\
**Response**:
```json
{
    "ok": true,
    "data": [
        {"item_id": "tweet_124", "verb": "tweet"},
        {"item_id": "tweet_123", "verb": "tweet"},
        {"item_id": "tweet_122", "verb": "tweet"},
        {"item_id": "tweet_121", "verb": "tweet"},
        {"item_id": "tweet_120", "verb": "tweet"}
    ]
}
```












