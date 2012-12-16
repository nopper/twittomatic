#!/bin/sh

hadoop jar hadoop-streaming-1.1.1.jar \
    -libjars partitioner.jar \
    -D map.output.key.field.separator=. \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.text.key.comparator.options=-k1,1nr \
    -D mapred.reduce.tasks=12 \
    -D mapred.output.compress=true \
    -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -file mapper.py \
    -mapper mapper.py \
    -partitioner org.twittomatic.hadoop.IntervalPartitioner \
    -file reducer.py \
    -reducer reducer.py \
    -input /twitter/*/*.twt.gz \
    -output /sorted-timeline
