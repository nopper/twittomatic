import java.math.BigInteger;

public class Tweet implements Comparable<Tweet> {
    private long time;
    private BigInteger tid;
    private BigInteger userId;
    private String line;
    private String geo;
    private String text;

//  public Tweet(String line, long time, long tid, long userId) {
//      this.setLine(line);
//      this.setTime(time);
//      this.setTid(tid);
//  }

    public Tweet(String line, long time, BigInteger tid, BigInteger userId, String geo, String text) {
        this.userId = userId;
        this.setLine(line);
        this.setTime(time);
        this.setTid(tid);
        this.setGeo(geo);
        this.setText(text.replaceAll("(\\n|\\t)", ""));
    }

    public String output() {
        // [Tweetid] [TAB] [TimeEpoch] [TAB] [UserId(numerico)] [TAB] [Geoloc] [TAB] [TweetOrRetweet]
        StringBuilder builder = new StringBuilder();
        builder.append(tid);
        builder.append("\t");
        builder.append(time);
        builder.append("\t");
        builder.append(userId);
        builder.append("\t");
        builder.append(geo);
        builder.append("\t");
        builder.append(text);
        builder.append("\n");
        return builder.toString();
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public long getTime() {
        return time;
    }

    public BigInteger getUserId() {
        return userId;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public int compareTo(Tweet o) {
        int ret = -tid.compareTo(o.getTid());

        if (ret == 0)
            return -Long.compare(time, o.getTime());

        return ret;
    }

    public BigInteger getTid() {
        return tid;
    }

    public void setTid(BigInteger tid) {
        this.tid = tid;
    }

    public String getGeo() {
        return geo;
    }

    public void setGeo(String geo) {
        this.geo = geo;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
