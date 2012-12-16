# Timeline sorter with hadoop streaming

First of all, Java sucks. I have tried to implement this functionality
directly in Java using the Hadoop APIs but apparently the TotalOrderPartitioner
seems to not work in our case. I have tried to investigate the problem but I
didn't find anything. Also I've tried asking in the IRC support channel
(#hadoop on FreeNode) but I got no response. Therefore I have decided to
emulate the TotalOrderPartitioner and InputSampler directly in Python through
the hadoop streaming API.


# How to use it

We have used hadoop-1.1.1. Just unzip your binary package somewhere and copy
inside this directory the following files:

    - `hadoop-core-1.1.1.jar`
    - `hadoop-streaming-1.1.1.jar`

To build the partitioner class just execute:

    $ ./build.sh

To run the set of scripts you have first to sample some random tweets from the
dataset. In this case we will sample all the .twt.gz files from the 14/ directory assuming you want to have 12 partitions file at the end:

    $ ./sample.sh 14

At this point we can run the real sorter:

    $ ./run.sh