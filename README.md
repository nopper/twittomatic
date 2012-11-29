# Twittomatic! What is it?

Twittomatic is a simple framework for Twitter social analysis. It consists of
several different components that work together to offer a wide range of
possibilities. The central component is the crawler which is designed to be
fault tolerant and fully distributed.

## Configuration

All the configurations options are actually exposed in the `twitter/settings.py`
file.

## The crawler

This component is a simple crawler used to effectively traverse the twitter
social graph starting from a given seed set. For each user in the seed set.
For each user discovered the user can choose to:

 - Download the entire timeline Download a list of followers of the target
 - user Analyze the list of followers of the target user, thus discovering new
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

### Storage setup

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
   and the master will be stored

 - `TEMPORARY_DIRECTORY`: Used to store temporary files. Be sure to have this
   directory on the same Volume where the `OUTPUT_DIRECTORY` resides.

### Monitoring

A simple web application is present to monitor the crawler activities in real
time. To launch it just execute:

	$ PYTHONPATH=. python twitter/web/monitor.py
	 * Running on http://127.0.0.1:9898/

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