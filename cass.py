import uuid
import time
import threading

import pycassa
from pycassa.cassandra.ttypes import NotFoundException
import cql

_local = threading.local()

try:
    cursor = _local.conn().cursor()
except AttributeError:
    conn = _local.conn = cql.connect('localhost')
    cursor = conn.cursor();
    cursor.execute("USE twissandra")
    
def _dictify_one_row(row, description):
    result = {};
    for desc,row in zip(description, row):
        result[desc[0]]=row;
    print "result:", result;
    return result;

# TODO use uuid bytes in row key rather than string


__all__ = ['get_user_by_username', 'get_friend_usernames',
    'get_follower_usernames',
    'get_timeline', 'get_userline', 'get_tweet', 'save_user',
    'save_tweet', 'add_friends', 'remove_friend', 'DatabaseError',
    'NotFound', 'InvalidDictionary', 'PUBLIC_USERLINE_KEY']

# NOTE: Having a single userline key to store all of the public tweets is not
#       scalable.  Currently, Cassandra requires that an entire row (meaning
#       every column under a given key) to be able to fit in memory.  You can
#       imagine that after a while, the entire public timeline would exceed
#       available memory.
#
#       The fix for this is to partition the timeline by time, so we could use
#       a key like !PUBLIC!2010-04-01 to partition it per day.  We could drill
#       down even further into hourly keys, etc.  Since this is a demonstration
#       and that would add quite a bit of extra code, this excercise is left to
#       the reader.
PUBLIC_USERLINE_KEY = '!PUBLIC!'


class DatabaseError(Exception):
    """
    The base error that functions in this module will raise when things go
    wrong.
    """
    pass


class NotFound(DatabaseError):
    pass


class InvalidDictionary(DatabaseError):
    pass

def _get_line(cf, username, start, limit):
    """
    Gets a timeline or a userline given a username, a start, and a limit.
    """
    # First we need to get the raw timeline (in the form of tweet ids)

    # We get one more tweet than asked for, and if we exceed the limit by doing
    # so, that tweet's key (timestamp) is returned as the 'next' key for
    # pagination.
    next = None
    if start == None:
        start = uuid.UUID('{00000000-0000-1000-8000-000000000000}');
    query = """
    SELECT body, tweetid, author
    FROM {0}
    WHERE userid = :userid
    AND tweetid < :start
    ORDER BY tweetid DESC
    limit :count;
    """.format(cf);

    cursor.execute(query, {"count" : limit+1, "start" : start , "userid" : username})
    rows = cursor.fetchall();

    if rows == []:
        return [], next

    tweets = []
    # Now we do a manual join to get the tweets themselves
    for row in rows:
        tweet_id = row[0]
        tweets.append({'id': row[1], 'body': row[0], 'username': row[2]})

    return (tweets, next)


# QUERYING APIs

def get_user_by_username(username):
    """
    Given a username, this gets the user record.
    """
    cursor.execute("SELECT password FROM users WHERE userid = :username", {"username" : username})
    columns = cursor.fetchone();
    if columns == []:
        raise NotFound('User %s not found' % (username,))
    return  _dictify_one_row(columns, cursor.description)

def get_friend_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people that the user is
    following.
    """
    cursor.execute("SELECT followed FROM following WHERE followed_by = :username", {"username" : username})
    return [row[0] for row in cursor.fetchall()]

def get_follower_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people following that user.
    """
    cursor.execute("SELECT followed_by FROM following WHERE followed = :username", {"username" : username})
    return [row[0] for row in cursor.fetchall()]

def get_timeline(username, start=None, limit=40):
    """
    Given a username, get their tweet timeline (tweets from people they follow).
    """
    return _get_line('timeline', username, start, limit)

def get_userline(username, start=None, limit=40):
    """
    Given a username, get their userline (their tweets).
    """
    return _get_line('userline', username, start, limit)

def get_tweet(tweet_id):
    """
    Given a tweet id, this gets the entire tweet record.
    """
    cursor.execute("SELECT username, body FROM tweets WHERE tweetid = :tweet_id", {"tweet_id" : tweet_id})
    row = cursor.fetchone()
    if row == []:
        raise NotFound('Tweet %s not found' % (tweet_id,))
    d = _dictify_one_row(row, cursor.description)
    return {'username': d['username'], 'body': d['body'].decode('utf-8')}


# INSERTING APIs

def save_user(username, password):
    """
    Saves the user record.
    """
    cursor.execute("UPDATE users SET password = :password WHERE userid = :username", {"password" : password, "username" : username})

def save_tweet(username, body):
    """
    Saves the tweet record.
    """
    tweet_id = str(uuid.uuid1())

    # Make sure the tweet body is utf-8 encoded
    body = body.encode('utf-8')

    # Insert the tweet, then into the user's timeline, and userline, then into the public one
    cursor.execute("UPDATE tweets SET userid = :username, body = :body WHERE tweetid = :tweet_id",{"username" : username,"body" : body, "tweet_id" : tweet_id})
    cursor.execute("""
    INSERT INTO userline
    (userid, tweetid, body, author)
    VALUES (:username, :tweet_id, :body, :author)
    """, {"tweet_id":tweet_id, "username":username, "body":body, "author":username})

    cursor.execute("""
    INSERT INTO timeline
    (userid, tweetid, body, author)
    VALUES (:username, :tweet_id, :body, :author)
    """, {"tweet_id":tweet_id, "username":username, "body":body, "author":username})

    cursor.execute("""
    INSERT INTO userline
    (userid, tweetid, body, author)
    VALUES (:username, :tweet_id, :body, :author)
    """, {"tweet_id":tweet_id, "username":PUBLIC_USERLINE_KEY, "body":body, "author":username})
    
    # Get the user's followers, and insert the tweet into all of their streams
    follower_usernames = [username] + get_follower_usernames(username)
    for follower_username in follower_usernames:
            cursor.execute("""
            INSERT INTO timeline
            (userid, tweetid, body, author)
            VALUES (:username, :tweet_id, :body, :author)
            """, {"tweet_id":tweet_id, "username":follower_username, "body":body, "author":username})


def add_friends(from_username, to_usernames):
    """
    Adds a friendship relationship from one user to some others.
    """
    for to_username in to_usernames:
        row_id = str(uuid.uuid1())
        cursor.execute("UPDATE following SET followed = :to_username, followed_by = :from_username WHERE rowId = :row_id", {"to_username":to_username, "from_username":from_username, "row_id":row_id})

def remove_friend(from_username, to_username):
    """
    Removes a friendship relationship from one user to some others.
    """
    cursor.execute("SELECT * FROM following WHERE followed = :followed AND followed_by = :followed_by", {"followed":to_username, "followed_by":from_username})
    row = cursor.fetchone()
    assert row != []
    cursor.execute("DELETE FROM following WHERE rowId = :row", {"row":rows[0]})
