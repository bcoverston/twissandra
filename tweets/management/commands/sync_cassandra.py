import cql

from django.core.management.base import NoArgsCommand

class Command(NoArgsCommand):

    def handle_noargs(self, **options):
        connection  = cql.connect('localhost', cql_version='3.0.0');
        cursor = connection.cursor()

        # If there is already a Twissandra keyspace, we have to ask the user
        # what they want to do with it.
        if 'twissandra' in [x.name for x in connection.client.describe_keyspaces()]:
            msg = 'Looks like you already have a Twissandra keyspace.\nDo you '
            msg += 'want to delete it and recreate it? All current data will '
            msg += 'be deleted! (y/n): '
            resp = raw_input(msg)
            if not resp or resp[0] != 'y':
                print "Ok, then we're done here."
                return
            cursor.execute("DROP KEYSPACE twissandra")

        cursor = cql.connect("localhost", cql_version='3.0.0').cursor()
        
        cursor.execute("CREATE KEYSPACE twissandra WITH strategy_class='SimpleStrategy' and strategy_options:replication_factor=1") #V1 strategy become optional
        cursor.execute("USE twissandra")
        cursor.execute("CREATE COLUMNFAMILY users (userId text PRIMARY KEY, password text, email text)")
        cursor.execute("CREATE COLUMNFAMILY following (rowId text PRIMARY KEY, followed text, followed_by text)")
        cursor.execute("CREATE INDEX following_followed ON following(followed)")
        cursor.execute("CREATE INDEX following_followed_by ON following(followed_by)")
        cursor.execute("CREATE COLUMNFAMILY tweets (tweetId uuid PRIMARY KEY, userid text, body text)")
        cursor.execute("""
        CREATE COLUMNFAMILY userline (
        userId text,
        tweetId uuid,
        body text,
        author text,
        PRIMARY KEY (userId, tweetId)
        )
        with comparator = 'CompositeType(org.apache.cassandra.db.marshal.UTF8Type, org.apache.cassandra.db.marshal.TimeUUIDType)'
        """)

        cursor.execute("""
        CREATE COLUMNFAMILY timeline (
        userId text,
        tweetId uuid,
        body text,
        author text,
        PRIMARY KEY (userId, tweetId)
        )
        with comparator = 'CompositeType(org.apache.cassandra.db.marshal.UTF8Type, org.apache.cassandra.db.marshal.TimeUUIDType)'
        """)
       
        #cursor.execute("CREATE COLUMNFAMILY timeline (KEY text PRIMARY KEY) WITH comparator=TimeUUIDType")
        #cursor.execute("CREATE COLUMNFAMILY userline (KEY text PRIMARY KEY) WITH comparator=TimeUUIDType")

        print 'All done!'
