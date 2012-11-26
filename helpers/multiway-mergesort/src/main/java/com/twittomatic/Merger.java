import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/*
 * Here we have several .twt file to merge. The scheme is simple:
 *  - We open all the .fws file we encounter and save them in a list of sets
 *  - Get all the twt file and decide how many steps for merging
 */
class Merger {
	
	private LinkedList<Long> inputFiles;
	private LinkedList<File> mergeFiles;

  private String inputPath;
  private int maxFiles;
	
	public static void main(String [] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: Merger <inputpath> <maxfiles>");
      System.out.println("       It will create files inside merge/ directory");
    }
    else
		  new Merger(args[0], Integer.parseInt(args[1]));
	}
	
	public Merger(String inputPath, int maxFiles) throws Exception {
    this.inputPath = inputPath;
    this.maxFiles = maxFiles;

		mergeFiles = new LinkedList<File>();
		
		inputFiles = getUsers();
		
		int maxLevels = (int) Math.round(Math.log(inputFiles.size()) / Math.log(maxFiles));
		int curLevel = 1;
		int counter = maxFiles;
		int roundId = 0;
		Round round;
		
		System.out.println("Reducing " + inputFiles.size() + " files. Levels: " + maxLevels);

		// Last level merge
		while (inputFiles.size() > 0) {
			counter = maxFiles;
			round = new Round(inputPath, roundId++);
			
			while (counter > 0 && !inputFiles.isEmpty()) {
				if (round.addUser(inputFiles.pop()))
					counter -= 1;
			}
			
			if (inputFiles.isEmpty() && mergeFiles.isEmpty())
				mergeFiles.add(round.merge(false, true));
			else
				mergeFiles.add(round.merge(false, false));
		}
		
		curLevel = 1;
		LinkedList<File> tmpMergeFiles = new LinkedList<File>();
		
		while (mergeFiles.size() > 1) {
			System.out.println("Level: " + curLevel + " maxLevels: " + maxLevels);
			
			while (mergeFiles.size() > 0) {
				counter = maxFiles;
				round = new Round(inputPath, roundId++);
				
				while (counter > 0 && !mergeFiles.isEmpty()) {
					if (round.addFile(mergeFiles.pop()))
						counter -= 1;
				}
				
				if (mergeFiles.isEmpty() && tmpMergeFiles.isEmpty())
					tmpMergeFiles.add(round.merge(true, true));
				else
					tmpMergeFiles.add(round.merge(true, false));
			}
			
			mergeFiles = tmpMergeFiles;
			tmpMergeFiles = new LinkedList<File>();
			
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
