import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import static java.nio.file.FileVisitResult.*;
import java.util.*;


public class Find extends SimpleFileVisitor<Path> {

  private final PathMatcher matcher;
  private final LinkedList<Long> users;

	public Find(String pattern) {
  	users = new LinkedList<Long>();
    matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
  }

  void find(Path file) {
    Path name = file.getFileName();
    if (name != null && matcher.matches(name)) {
    	String fname = name.toString();
    	getUsers().add(Long.parseLong(fname.substring(0, fname.length() - 4)));
    }
  }

  @Override
  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
    find(file);
    return CONTINUE;
  }

  @Override
  public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
    find(dir);
    return CONTINUE;
  }

  @Override
  public FileVisitResult visitFileFailed(Path file, IOException exc) {
    System.err.println(exc);
    return CONTINUE;
  }

	public LinkedList<Long> getUsers() {
		return users;
	}
}
