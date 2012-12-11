import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class TweetReader implements Comparable<TweetReader> {

    private Tweet currentTweet;
    private File file;
    private BufferedReader reader;
    private DataInputStream dis;
    private FileInputStream fis;
    private JSONParser jsonReader;

    public Tweet getCurrentTweet() {
        return currentTweet;
    }

    public TweetReader(File file, BufferedReader br, DataInputStream dis, FileInputStream fis) throws Exception {
        this.file = file;
        this.reader = br;
        this.dis = dis;
        this.fis = fis;
        this.jsonReader = new JSONParser();

        if (!advance(false))
            throw new Exception();
    }

    @Override
    public int compareTo(TweetReader o) {
        return currentTweet.compareTo(o.getCurrentTweet());
    }

    private void close(boolean canDestroy) {
        try {
            this.reader.close();
        } catch (Exception e) {
        }
        try {
            this.dis.close();
        } catch (Exception e) {
        }
        try {
            this.fis.close();
        } catch (Exception e) {
        }

        if (canDestroy) {
          // System.out.println("Deleting " + file + ".. what a joke");
          file.delete();
        }
    }

    @SuppressWarnings("rawtypes")
    public boolean advance(boolean canDestroy) {
        try {
            String strLine = reader.readLine();

            if (strLine == null || strLine == "")
            {
                close(canDestroy);
                return false;
            }

            JSONObject json = (JSONObject)jsonReader.parse(strLine);

            // Sun Apr 22 16:11:51 +0000 2012
            //("E M dd HH:mm:ss +0000 yyyy")
            //DateFormat format = new SimpleDateFormat("%a %b %d %H:%M:%S +0000 %Y");
            DateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss +0000 yyyy");
            Date parsed = format.parse((String)json.get("created_at"));

            JSONObject geo = ((JSONObject)json.get("geo"));

            this.currentTweet = new Tweet(
                    strLine,
                    parsed.getTime(),
                    Long.parseLong((String)json.get("id_str")),
                    Long.parseLong((String)((HashMap)json.get("user")).get("id_str")),
                    geo == null ? "null" : geo.toJSONString(),
                    (String)json.get("text")
            );
            return true;
        } catch (Exception exc) {
            //exc.printStackTrace();
            System.out.println("Error while reading " + file.getAbsolutePath());
            close(canDestroy);
            return false;
        }
    }

}
