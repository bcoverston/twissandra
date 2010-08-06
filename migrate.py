#!/usr/bin/env python

#
# A quick migration from id-based users to uname-based users.
# Please flush and compact when you take down the server,
#  then after bringing down the server to update the code,
#  run this migration before bringing the server back up.
#

import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'deps'))

from odict import OrderedDict
import pycassa
from cass import USER, FRIENDS, FOLLOWERS, TWEET, TIMELINE, USERLINE

def migrate_users(ks):
    print "  * Migrating Users"
    uid_to_uname = {}
    for key, row in USER.get_range():
        if 'id' in row:
            # Map the id to the uname for later use
            uname = row['username'][1:-1]
            id_ = row['id'][1:-1]
            uid_to_uname[id_] = uname
            # Get rid of id and username
            del row['id']
            del row['username']
            # reset password
            row['password'] = 'x'
            # Delete old, insert new with uname key and all the remaining args
            USER.remove(key)
            USER.insert(uname, row)
            # This print will probably spam you to death. Please silence it.
            print '    * Migrated user %s / %s' % (uname, id_)

    # Now that we have a uid_to_uname mapping, it's time to
    #   change every other place where uid shows up.
    print "  * Migrating uid to uname globally"
    print "    * Migrating Friends"
    for key, row in FRIENDS.get_range():
        if key in uid_to_uname:
            for friend in row.keys():
                row[uid_to_uname[friend]] = row[friend]
                del row[friend]
            FRIENDS.remove(key)
            FRIENDS.insert(uid_to_uname[key], row)
            # This print will probably spam you to death. Please silence it.
            print '      * Friends of ' + uid_to_uname[key] + ' have been migrated.'

    print "    * Migrating Followers"
    for key, row in FOLLOWERS.get_range():
        if key in uid_to_uname:
            for follower in row.keys():
                row[uid_to_uname[follower]] = row[follower]
                del row[follower]
            FOLLOWERS.remove(key)
            FOLLOWERS.insert(uid_to_uname[key], row)
            # This print will probably spam you to death. Please silence it.
            print '      * Followers of ' + uid_to_uname[key] + ' have been migrated.'


    print "    * Migrating Tweets"
    for key, row in TWEET.get_range():
        if 'id' in row:
          del row['id']
          del row['_ts']
          row['uname'] = uid_to_uname[row['user_id'][1:-1]]
          del row['user_id']
          TWEET.remove(key)
          TWEET.insert(key, row)
          # This print will probably spam you to death. Please silence it.
          print '      * Tweet ' + key + ' has been migrated.'

    print "    * Migrating Timeline"
    for key, row in TIMELINE.get_range():
        if key in uid_to_uname:
            TIMELINE.remove(key)
            TIMELINE.insert(uid_to_uname[key], row)

    print "    * Migrating Userline"
    for key, row in USERLINE.get_range():
        if key in uid_to_uname:
            USERLINE.remove(key)
            USERLINE.insert(uid_to_uname[key], row)
   

if __name__ == '__main__':
    print "  * Connecting to the database in keyspace 'Twissandra'"
    migrate_users("Twissandra")
