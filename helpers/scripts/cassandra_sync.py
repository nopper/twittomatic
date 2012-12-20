from twitter import settings

import pycassa
from pycassa.system_manager import *

sys = SystemManager()
keyspace = settings.CASSANDRA_KEYSPACE

if keyspace in sys.list_keyspaces():
    msg = 'Looks like you already have a %s keyspace.\nDo you ' % keyspace
    msg += 'want to delete it and recreate it? All current data will '
    msg += 'be deleted! (y/n): '
    resp = raw_input(msg)
    if not resp or resp[0] != 'y':
        print "Ok, then we're done here."
        import sys
        sys.exit(0)
    sys.drop_keyspace(keyspace)

sys.create_keyspace(keyspace, SIMPLE_STRATEGY, {'replication_factor': '1'})
sys.create_column_family(keyspace, 'Followers',
    comparator_type=LONG_TYPE,
    default_validation_class=LONG_TYPE,
    key_validation_class=LONG_TYPE,
)
sys.create_column_family(keyspace, 'UserTimeline',
    comparator_type=LONG_TYPE,
    default_validation_class=LONG_TYPE,
    key_validation_class=LONG_TYPE,
)
sys.create_column_family(keyspace, 'Timeline',
    comparator_type=UTF8_TYPE,
    default_validation_class=UTF8_TYPE,
    key_validation_class=UTF8_TYPE,
    compression_options={
        'sstable_compression' : 'SnappyCompressor',
        'chunk_length_kb' : '64'
    }
)

print 'All done!'