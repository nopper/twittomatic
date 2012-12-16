# Build

Maven is required to build the software. All the dependencies will be fetched
and installed automatically.

        $ mvn install

# How to use the merger

The merger actually takes in input all the dataset and produce a single huge
time-sorted timeline file containing all the tweets extracted from all the
single twt files created by the crawler.

For a better performance we suggest to specify the same number of threads as
per processor your machine has.

Moreover we suggest to fine tune the `/etc/security/limits.conf` file in order
to be able to effectively merge the dataset in a short time. Assuming you are
using the user `crawler` to merge the dataset, you should add the following
line in the file:

        crawler          -       nofile          8192

Then logout and login for the system change take effect:

        $ ulimit -Hn
        8192
        $ ulimit -Sn
        8192
        $ java -cp target/multiway-mergesort-1.0-SNAPSHOT-jar-with-dependencies.jar: Merger /home/crawler/twitter-ds /home/crawler/twitter-merge/ 500 16

Please note that we have specified 16 threads assigning 500 files each. For
the sake of clarity 500 * 16 gives 8000 which should be less than the hard
limit you have specified in the `limits.conf` file.

Take in consideration that this will take a lot for the final phase since only
one thread is in charge of reducing all the files. We measured a maximum of
2MB/s write performance in output.

You may consider rewriting the code to create total ordered and disjoint
partitions in parallel in order to avoid the final bottleneck.

Another option is to just leave the code as is and avoid the final round. The
caveat here is to specify the right parameters so at the end we will get
sorted N sorted runs, where N = number of processors. Additionally you will
print out in a stats file the date-range information for every sorted run. At
this point you can partition the data range and run N sub-range extractor.

Or you can simply use hadoop and a TotalOrderPartitioner.
