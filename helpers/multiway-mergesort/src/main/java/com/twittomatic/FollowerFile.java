import java.io.BufferedWriter;
import java.util.HashSet;


public class FollowerFile {
    private long userId;
    private BufferedWriter writer;
    private HashSet<Long> followers;

    public FollowerFile(long userId) {
        this.userId = userId;
        this.followers = new HashSet<Long>();
    }

    public HashSet<Long> getFollowers() {
        return followers;
    }

    public BufferedWriter getWriter() {
        return writer;
    }

    public void setWriter(BufferedWriter writer) {
        this.writer = writer;
    }

    public long getUserId() {
        return userId;
    }
}
