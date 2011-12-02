import pycassa
from pycassa.system_manager import *

import cql

from django.core.management.base import NoArgsCommand

class Command(NoArgsCommand):

    def handle_noargs(self, **options):
        sys = SystemManager()
        cursor = cql.connect('localhost').cursor()

        # If there is already a Twissandra keyspace, we have to ask the user
        # what they want to do with it.
        if 'Twissandra' in sys.list_keyspaces():
            msg = 'Looks like you already have a Twissandra keyspace.\nDo you '
            msg += 'want to delete it and recreate it? All current data will '
            msg += 'be deleted! (y/n): '
            resp = raw_input(msg)
            if not resp or resp[0] != 'y':
                print "Ok, then we're done here."
                return
            sys.drop_keyspace('Twissandra')

        cursor = cql.connect("localhost").cursor()
        cursor.execute("DROP KEYSPACE twissandra")
        cursor.execute("CREATE KEYSPACE twissandra WITH strategy_class='SimpleStrategy' and strategy_options:replication_factor=1") #V1 strategy become optional
        cursor.execute("USE twissandra")
        cursor.execute("CREATE COLUMNFAMILY users (KEY text PRIMARY KEY, password text)")
        cursor.execute("CREATE COLUMNFAMILY following (KEY text PRIMARY KEY, followed text, followed_by text)")
        cursor.execute("CREATE INDEX following_followed ON following(followed)")
        cursor.execute("CREATE INDEX following_followed_by ON following(followed_by)")
        cursor.execute("CREATE COLUMNFAMILY tweets (KEY uuid PRIMARY KEY, user_id text, body text)")
        cursor.execute("CREATE COLUMNFAMILY timeline (KEY text PRIMARY KEY) WITH comparator=TimeUUIDType")
        cursor.execute("CREATE COLUMNFAMILY userline (KEY text PRIMARY KEY) WITH comparator=TimeUUIDType")

        print 'All done!'
