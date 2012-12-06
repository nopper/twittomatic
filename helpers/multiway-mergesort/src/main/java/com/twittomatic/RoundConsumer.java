import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;


public class RoundConsumer implements Runnable {

    private ConcurrentLinkedQueue<File> mergeFiles;
    private boolean isLast;
    private boolean canDestroy;
    private Round round;

    public RoundConsumer(Round round, boolean canDestroy, boolean isLast, ConcurrentLinkedQueue<File> mergeFiles) {
        this.round = round;
        this.canDestroy = canDestroy;
        this.isLast = isLast;
        this.mergeFiles = mergeFiles;
    }

    @Override
    public void run() {
        try {
            mergeFiles.add(round.merge(canDestroy, isLast));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
