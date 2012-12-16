#!/bin/sh

hadoop jar hadoop-streaming-1.1.1.jar \
    -D map.output.key.field.separator=. \
    -D stream.num.map.output.key.fields=2 \
    -D mapred.text.key.comparator.options=-k1,1nr \
    -D mapred.reduce.tasks=1 \
    -D mapred.output.compress=true \
    -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -file mapper.py \
    -mapper mapper.py \
    -file reducer.py \
    -reducer reducer.py \
    -input '/twitter/$1/*.twt.gz' \
    -output /minisample
hadoop fs -cat /minisample/part-00000.gz | gzip -d - | python sample.py > partitions.lst
hadoop fs -rm /partitions.lst
hadoop fs -put partitions.lst /