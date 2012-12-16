package org.twittomatic.hadoop;

import java.io.*;
import java.util.*;
import java.math.BigInteger;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class IntervalPartitioner implements Partitioner<Text, Text> {
    private static final ArrayList<BigInteger> intervals = new ArrayList<BigInteger>();

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        String repr = key.toString();
        String[] args = repr.split("\\.", 2);
        BigInteger tweet_id = new BigInteger(args[0]);

        int bucket = 0;
        while ((bucket < intervals.size()) && (tweet_id.compareTo(intervals.get(bucket)) > 0))
            bucket += 1;

        return bucket;
    }

    @Override
    public void configure(JobConf job) {
        System.out.println("Loading partitions");

        try {
            Path path = new Path("/partitions.lst");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            line = br.readLine();

            while (line != null) {
                intervals.add(new BigInteger(line));
                line = br.readLine();
            }

            System.out.println("Loaded " + intervals.size() + " intervals from /partitions.lst");
        } catch (Exception e) {
            System.out.println("Unable to load /partitions.lst file");
        }
    }
}
