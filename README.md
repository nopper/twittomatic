# Twittomatic! What is it?

Twittomatic is a simple framework for Twitter social analysis. It consists of
several different components that work together to offer a wide range of
possibilities. The central component is the crawler which is designed to be
fault tolerant and fully distributed.

## Requirements

The software heavily relies on `redis`. There are also additional python
dependencies that need to be installed. We suggest to use `virtualenv` or
`pythonbrew` to avoid installing these dependencies system wide:

    $ pip install -r requirements.txt

Additionally you may need to install `pydoop` or `pycassa` if you need to use
HDFS or Cassandra storage.


## Testing the software and implementing your own module

We provide a simple twitter emulator in the tests directory that is capable of
loading a set of fixtures






## Configuration

All the configurations options are actually exposed in the `twitter/settings.py`
file.

## The crawler

This component is a simple crawler used to effectively traverse the twitter
social graph starting from a given seed set. For each user in the seed set.
For each user discovered the user can choose to:

 - Download the entire timeline
 - Download a list of followers of the targetuser
 - Analyze the list of followers of the target user, thus discovering new
   users to be added to the processing queue.

The system is built to easilly scale on large number of machines in order to
achieve high throughput. A central filesystem is required to be accessible for
each of the worker. A single master is in charge of keeping the state of the
traversal. Persistency is implemented through Redis key/value storage.

### Twitter modules

For the moment the focus of the application consists in analyzing followers
and the timelines of the user, thereby discovering new interesting
connections. To achieve so four different modules have been implemented:

 - `timeline` module
 - `follower` module
 - `analyzer` module
 - `update` module

### Fault tolerance

At the current stage a crash in the client is handled and the job, if it was
assigned, is automatically recovered by the master. The master is acually the
SPoF although the system can be easilly customized to handle automatic failover
in case of failure of every single core component.

### Automatic Failover

Automatic failover of the redis instance can be configured with the use of
`redis-sentinel` software. Although at the moment, for security issues a crash
in the master produces a stop of the entire system (clients are configured to
commit suicide upon a master failure is detected), the system can be easilly
tuned to handle failures in a simple way. Master can be easily wrapped in a
simple bash script for automatic respawning in case of crash, thus providing a
simple yet fault tolerant distributed architecture. The same applies for the
client too.

An experimental high-availability mode is present and can be enabled by
specifying the `--ha` flag while spawning the master. For example indicating
something like `--ha 1` will spawn a new master with id set to 1. The new
master will listen on port JT_PORT + ID. The new clients will have to use the
new port for connecting to the new master.

### Storage setup

The crawler at the moment is implemented to support three different storage
options:

 - File storage (with optional gzip compression)
 - Hadoop file storage (with optional gzip compression - experimental)
 - Cassandra key-value storage (compression must be enabled in cassandra -
   experimental)

#### Simple file storage

The crawler actually uses a simple schema for storing timeline and follower
files. The `OUTPUT_DIRECTORY` is structured and organized with the following
assumption:

 - The first two digits of the user ID number are used to form the final
   directory where the files relative to that user will be stored.

To clarify the assumption, let's try to give an example assuming we just
downloaded the timeline and the followers of the user with `user_id`
`22013880`:

  - `$OUTPUT_DIRECTORY/22/22013880.fws` : will contain the followers of the user.
     Each line is a numeric string. The file is compressed using gzip.
  - `$OUTPUT_DIRECTORY/22/22013880.twt` : cotains the timelien of the user.
     Each line is a json string. The file is compressed using gzip.

In addition to `OUTPUT_DIRECTORY`, other variables need to be properly
configured:

 - `LOG_DIRECTORY`: In this directory all the log files relative to workers
   and the master will be stored. This must not be a HDFS directory but a real
   one.

 - `TEMPORARY_DIRECTORY`: Used to store temporary files. Be sure to have this
   directory on the same Volume where the `OUTPUT_DIRECTORY` resides. This must
   not be a HDFS directory but a real one.

 - `USE_COMPRESSION`: set to True if you want your file to be gzip compressed

#### Cassandra storage

This backend is experimental and needs testing. Use it at your own risk.

To configure it just change set the variable `STORAGE_CLASS` to `cassandra` and
also set your `CASSANDRA_KEYSPACE` accordingly. Remeber to actually create the
keyspace before starting the crawling process. This can be done with the command:

    $ python helpers/scripts/cassandra_sync.py

### Monitoring

A simple web application is present to monitor the crawler activities in real
time. To launch it just execute:

	$ PYTHONPATH=. python twitter/web/monitor.py
	 * Running on http://127.0.0.1:9898/

A simple inspector is also present that heavily relies on elasticsearch for
visualization. You can access it by pointing your browser to
`http://127.0.0.1:9898/inspect`.

## Streaming modules

A set of streaming modules are present for augmenting and improving the
crawler functionalities without impacting in any way on the complexity of the
code. This is made possible by exploiting the PUB/SUB architecture of redis,
and by implementing an event driven architecture.

For the moment being there are three different streaming modules:

 - A `stats` module responsible for collecting live statistics of the crawling
   process. The module allows to store the statistics in a compressed file and
   to replay them in a Graphite server for rendering and analysis purposes.

 - A `lookup` module web application that actually handles `user_id` to
   `screen_name` look up operations through a BDB storage-like system.

 - An `indexer` module that automatically sends out new tweets to an
   ElasticSearch instance, thus realizing an on-the-fly near-real-time indexing
   of data being collected.

# Multi-IP setup

In order to exploit all the potential of the crawler you need at least a
sufficiently large pool of public IPv4 addresses. Once you get the pool you
can simply use a single machine to exploit all the pool at once.

For reference take a look at `helpers/scripts/setup-pool.sh` and
`helpers/scripts/start-workers.sh`.

Rembember of course to spawn a master before starting the workers:

    $ redis-server # in a terminal
    $ PYTHONPATH=. python twitter/master.py -T --stream=target-list

# Examples

Download only the timelines of a list of users:

    $ PYTHONPATH=. python twitter/master.py -T --stream=target-list

Download only the followers of a list of users:

    $ PYTHONPATH=. python twitter/master.py -F --stream=target-list

Download only the followers and do an analysis on them:

    $ PYTHONPATH=. python twitter/master.py -F -A --stream=target-list

Full analysis (timeline, follower and analysis):

    $ PYTHONPATH=. python twitter/master.py -T -F -A --stream=target-list

Update the timelines of a list of users:

    $ PYTHONPATH=. python twitter/master.py -U --stream=target-list

# A full working setup

A complete setup will require you:

  - A redis instance on the default port (essential)
  - A pool of IP (>=1 essential)

  - An elasticsearch instance up and running (set `ELASTICSEARCH_URL`).
    Used by the inspector page of the monitor application (optional)
  - A graphite server up and running (set `GRAPHITE_URL`). It is used
    by the monitor interface (optional).
  - A carbon server up and running (set `CARBON_SERVER` and `CARBON_PORT`).

The elasticsearch instance is used for indexing tweets on the fly and
providing a nice looking inspector interface available throught the monitor
app. The graphite and carbon server are used to collect statistics about the
workers. The graphite server is used for rendering graphs which are showed in
the main page of the monitor interface.

## Setting up elasticsearch

    $ wget https://github.com/downloads/elasticsearch/elasticsearch/elasticsearch-0.19.11.zip
    $ unzip elasticsearch-0.19.11.zip
    $ cd elasticsearch-0.19.11
    $ bin/plugin -install mobz/elasticsearch-head
    $ bin/elasticsearch -f

You can then monitor elasticsearch through `http://localhost:9200/_plugin/head/`

## Setting up redis

Assuming you have `redis-server` installed:

    $ redis-server

## Setting up graphite

This requires to have `python-cairo` package installed by your package manager.

    $ mkdir graphite
    $ cd graphite
    $ git clone https://github.com/graphite-project/graphite-web.git
    $ git clone https://github.com/graphite-project/whisper.git
    $ git clone https://github.com/graphite-project/carbon.git
    $ git clone https://github.com/graphite-project/ceres.git

    $ cd whisper
    $ sudo python setup.py install
    $ cd ceres
    $ sudo python setup.py install

    $ sudo pip install django
    $ sudo pip install django-tagging

    $ cd carbon
    $ sudo python setup.py install
    $ cd graphite-web
    $ sudo python setup.py install

Now you should have everything installed. Let's configure:

    # cd /opt/graphite
    # cat > conf/storage-schemas.conf
    [carbon]
    pattern = ^carbon\.
    retentions = 60s:1d

    [default]
    pattern = .*
    retentions = 5s:1d,1m:7d,10m:1y
    ^D
    # cp conf/carbon.conf.example conf/carbon.conf
    # cp webapp/graphite/local_settings.py.example webapp/graphite/local_settings.py

Now edit the `local_settings.py` to enable the sqlite3 database. These lines
must be commented out:

    DATABASES = {
        'default': {
            'NAME': '/opt/graphite/storage/graphite.db',
            'ENGINE': 'django.db.backends.sqlite3',
            'USER': '',
            'PASSWORD': '',
            'HOST': '',
            'PORT': ''
        }
    }

We have to create the database:

    # python webapp/graphite/manage.py syncdb
    Creating tables ...
    Creating table account_profile
    Creating table account_variable
    ...

Then you are done. We can start the carbon server and the graphite web interface:

    # python2 bin/carbon-cache.py --debug start
    Starting carbon-cache (instance a)
    04/12/2012 14:28:56 :: [console] Log opened.
    04/12/2012 14:28:56 :: [console] twistd 12.2.0 (/usr/bin/python2 2.7.3) starting up.
    04/12/2012 14:28:56 :: [console] reactor class: twisted.internet.epollreactor.EPollReactor.

    # python bin/run-graphite-devel-server.py /opt/graphite
    Running Graphite from /opt/graphite under django development server
    ...

You should be able to use graphite-web by pointing your browser to `http://0.0.0.0:8080/`

## Starting streaming services

The lookup service:

    $ PYTHONPATH=. python twitter/web/lookup.py
    Starting Lookup server on port http://localhost:9797
    ...

The indexer service. This will delete and recreate your index. All the data
will be lost:

    $ PYTHONPATH=. python twitter/web/indexer.py
    Index successfully created
    ...

The monitor:

    $ PYTHONPATH=. python twitter/web/monitor.py
     * Running on http://127.0.0.1:9898/
     * Restarting with reloader

The stats collector:

    $ PYTHONPATH=. python twitter/web/stats.py -f /tmp/stats.gz

## Starting the master

    $ cat > target
    27476557
    ^D
    $ PYTHONPATH=. python twitter/master.py -T -F -A --stream=target
    2012-12-04 14:46:47+0100 [-] Log opened.
    2012-12-04 14:46:47+0100 [-] Log opened.
    2012-12-04 14:46:47+0100 [-] New JobTracker server started
    2012-12-04 14:46:47+0100 [-] Using TwitterJob as Job class
    2012-12-04 14:46:47+0100 [-] Successfully loaded 1 users into stream
    2012-12-04 14:46:47+0100 [-] TwitterJobTrackerFactory starting on 8000
    2012-12-04 14:46:47+0100 [-] Starting factory <__main__.TwitterJobTrackerFactory instance at 0x841fa0c>

Now you can open the monitor interface (`http://127.0.0.1:9898/`) to check
that we actually have a timeline job pending in the stream.

## Starting the worker

    $ PYTHONPATH=. python twitter/worker.py worker00

## Live monitoring

You can connect with your browser to the monitor service and checkout what is going on under the hood:

### Monitor page

![Monitor page](http://i.imgur.com/KYGOg.png "Monitor page")

### Inspector page

![Inspector page](http://i.imgur.com/Ng0W3.png "Inspector page")

## Getting results

At the end in the master terminal window you should see a message like:

    2012-12-04 14:50:29+0100 [-] Frontier contents saved into /tmp/frontier-vSAhrH.gz

That gzip file will contain the targets that are needed to do the second step
of the BFS traversal.

All the data downloaded will be available in the output directory. In case of
standard settings:

    $ zcat ~/twitter-ds/27/27476557.twt | wc -l
    485
    $ PYTHONPATH=. python helpers/scripts/ff-util.py --cat ~/twitter-ds/27/27476557.fws | wc -l
    80

In case you are wondering to which user a given ID maps to you can use the
lookup service:

    $ http http://localhost:9797/lookup/id/15590090
    HTTP/1.1 200 OK
    Connection: close
    Content-Length: 2554
    Content-Type: application/json
    Date: Tue, 04 Dec 2012 14:04:05 GMT
    Server: gevent/0.13 Python/2.7

    {
        "contributors_enabled": false,
        "created_at": "Thu Jul 24 22:31:12 +0000 2008",
        "default_profile": false,
        "default_profile_image": false,
        "description": "CSO of Rapid7 and Chief Architect of Metasploit",
    ...
