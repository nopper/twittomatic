import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 * Here we have several .twt file to merge. The scheme is simple:
 *  - We open all the .fws file we encounter and save them in a list of sets
 *  - Get all the twt file and decide how many steps for merging
 */
class Merger {

    private LinkedList<Long> inputFiles;
    private ConcurrentLinkedQueue<File> mergeFiles;

    private String inputPath;

    public static void main(String [] args) throws Exception {
        if (args.length != 4) {
            System.out.println("Usage: Merger <inputpath> <outputpath> <maxfiles> <maxthreads>");
            System.out.println("       It will create files inside <outputpath>/ directory");
        }
        else
            new Merger(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
    }

    public Merger(String inputPath, String outputPath, int maxFiles, int maxThreads) throws Exception {
        this.inputPath = inputPath;

        mergeFiles = new ConcurrentLinkedQueue<File>();

        inputFiles = getUsers();

        int maxLevels = (int) Math.round(Math.log(inputFiles.size()) / Math.log(maxFiles));
        int curLevel = 1;
        int counter = maxFiles;
        int roundId = 0;
        Round round;

        System.out.println("Reducing " + inputFiles.size() + " files. Levels: " + maxLevels);
        ExecutorService executorService = Executors.newFixedThreadPool(maxThreads);

        // Last level merge
        while (inputFiles.size() > 0) {
            counter = maxFiles;
            round = new Round(inputPath, outputPath, roundId++);

            while (counter > 0 && !inputFiles.isEmpty()) {
                if (round.addUser(inputFiles.pop()))
                    counter -= 1;
            }

            boolean canDestroy = false;
            boolean isLast = (inputFiles.isEmpty() && mergeFiles.isEmpty());

            RoundConsumer consumer = new RoundConsumer(round, canDestroy, isLast, mergeFiles);
            executorService.execute(consumer);
        }

        executorService.shutdown();

        while (!executorService.isTerminated())
        {
            System.out.println("Waiting for threads termination. Files " + mergeFiles.size());
            Thread.sleep(1000);
        }

        curLevel = 1;
        ConcurrentLinkedQueue<File> tmpMergeFiles = new ConcurrentLinkedQueue<File>();

        while (mergeFiles.size() > 1) {
            System.out.println("Level: " + curLevel + " maxLevels: " + maxLevels);
            executorService = Executors.newFixedThreadPool(maxThreads);

            while (mergeFiles.size() > 0) {
                counter = maxFiles;
                round = new Round(inputPath, outputPath, roundId++);

                while (counter > 0 && !mergeFiles.isEmpty()) {
                    if (round.addFile(mergeFiles.poll()))
                        counter -= 1;
                }

                boolean canDestroy = true;
                boolean isLast = (mergeFiles.isEmpty() && tmpMergeFiles.isEmpty());

                RoundConsumer consumer = new RoundConsumer(round, canDestroy, isLast, tmpMergeFiles);
                executorService.execute(consumer);
            }

            executorService.shutdown();

            while (!executorService.isTerminated())
            {
                System.out.println("Waiting for threads termination. Files " + mergeFiles.size());
                Thread.sleep(1000);
            }

            // At this point we are single threaded
            mergeFiles = tmpMergeFiles;
            tmpMergeFiles = new ConcurrentLinkedQueue<File>();

            curLevel += 1;
        }

        for (File file: mergeFiles) {
            System.out.println("Output file is " + file.getAbsolutePath());
        }
    }

    private LinkedList<Long> getUsers() throws Exception {
        Find finder = new Find("*.twt");
        Files.walkFileTree(Paths.get(this.inputPath), finder);
        return finder.getUsers();
    }
}
