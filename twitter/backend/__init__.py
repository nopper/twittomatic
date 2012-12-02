from twitter import settings

if settings.STORAGE_CLASS == 'file':
    from twitter.backend.filestorage import TimelineFile, FollowerFile
elif settings.STORAGE_CLASS == 'cassandra':
    from twitter.backend.cassandrastorage import TimelineFile, FollowerFile