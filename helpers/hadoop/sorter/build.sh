#!/bin/sh
mkdir -p classes
javac -cp hadoop-core-1.1.1.jar -d classes IntervalPartitioner.java
jar -cvf partitioner.jar -C classes/ .
