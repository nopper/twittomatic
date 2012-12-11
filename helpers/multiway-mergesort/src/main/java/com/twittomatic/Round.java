import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class Round {
    private PriorityQueue<TweetReader> queue;
    private int roundId;
    private String inputPath;
    private String outputPath;
    private ArrayList<Long> inputUsers;

    public Round(String inputPath, String outputPath, int id) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.roundId = id;
        this.queue = new PriorityQueue<TweetReader>();
        this.inputUsers = new ArrayList<Long>();
    }

    private void readUsers() {
        for (Long user: inputUsers) {
            String strUser = Long.toString(user);
            addStream(new File(this.inputPath + "/" + strUser.substring(0, 2), strUser + ".twt"));
        }

        inputUsers.clear();
    }

    private File simpleMerge(boolean canDestroy) throws IOException {
        File file = File.createTempFile("round-", "-" + roundId, new File(outputPath));
        BufferedOutputStream out = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(file)));

        System.out.println("Merging round: " + roundId + " Files: " + queue.size() + " Ouput: " + file.getName());

        while (!queue.isEmpty()) {
            TweetReader reader = queue.poll();
            out.write((reader.getCurrentTweet().getLine() + "\n").getBytes());

            if (reader.advance(canDestroy))
                queue.add(reader);
        }

        out.close();
        return file;
    }

    public File merge(boolean canDestroy, boolean isLast) throws IOException {
        readUsers();

        if (!isLast)
            return simpleMerge(canDestroy);

        File jsonFile = File.createTempFile("timeline-full-json-", "-" + roundId, new File(outputPath));
        BufferedOutputStream jsonOut = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(jsonFile)));

        File txtFile = File.createTempFile("timeline-full-txt-", "-" + roundId, new File(outputPath));
        BufferedOutputStream txtOut = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(txtFile)));

        System.out.println("Final round: " + roundId + " Files: " + queue.size());

        String line;
        TweetReader reader;
        Tweet tweet;
        String txtLine;

        long lastTime = Long.MAX_VALUE;
        long lastTweetId = Long.MAX_VALUE;
        long counter = 0;

        while (!queue.isEmpty()) {
            reader = queue.poll();
            tweet = reader.getCurrentTweet();

            // This is needed to skip bogus files with duplicate entries
            if (tweet.getTid() >= lastTweetId || tweet.getTime() > lastTime) {
                if (reader.advance(canDestroy))
                    queue.add(reader);

                continue;
            }

            line = tweet.getLine();
            txtLine = tweet.output();
            lastTime = tweet.getTime();
            lastTweetId = tweet.getTid();

            jsonOut.write((line + "\n").getBytes());
            txtOut.write(txtLine.getBytes());
            counter += 1;

            if (reader.advance(canDestroy))
                queue.add(reader);
        }

        System.out.println("Tweets collected: " + counter);

        jsonOut.close();
        txtOut.close();

        return jsonFile;
    }

    private boolean addStream(File inputFile) {
        FileInputStream fstream;
        GZIPInputStream gzstream;

        try {
            //System.out.println(" -> " + inputFile.getName());
            fstream = new FileInputStream(inputFile.getPath());
            gzstream = new GZIPInputStream(fstream);
            DataInputStream in = new DataInputStream(gzstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            queue.add(new TweetReader(inputFile, br, in, fstream));
            return true;

        } catch (Exception exc) {
            System.out.println("Unable to open " + inputFile.getPath() + " for reading");
        }

        return false;
    }

    public boolean addFile(File file) {
        return addStream(file);
    }

    public boolean addUser(long user) {
        inputUsers.add(user);
        return true;
    }
}
