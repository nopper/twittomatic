#!/bin/sh

hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.1.1.jar \
    -D mapred.output.compress=true \
    -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -file mapper.py \
    -mapper mapper.py \
    -file reducer.py \
    -reducer reducer.py \
    -input /wikipedia/wikipedia-??.json.gz \
    -output /anchors/
