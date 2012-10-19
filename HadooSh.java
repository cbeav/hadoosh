import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.SimpleCompletor;

public class HadooSh {
	static Configuration config;
	static FileSystem fs;
	static Path p;
	static Path home;

	public static void main(String[] args) throws Exception {
		config = new Configuration();
		config.set("fs.default.name", "hdfs://localhost:9000");
		fs = FileSystem.get(config);
		
		p = fs.getWorkingDirectory();
		home = new Path(p.toString());
		
		ConsoleReader reader = new ConsoleReader();
		reader.setBellEnabled(false);
		List completors = new LinkedList();
		String[] commandsList = new String[] {"cd", "ls", "pwd", "exit"};
		Completor fileCompletor = new HDFSCompletor();
		completors.add(new SimpleCompletor(commandsList));
		completors.add(fileCompletor);
		reader.addCompletor(new ArgumentCompletor(completors));
	
		
		String line;
		PrintWriter out = new PrintWriter(System.out);
		
		while ((line = reader.readLine("> ").trim()) != null)
		{
			if (line.equals("exit"))
				break;
			else if(line.startsWith("ls"))
				ls(line);
			else if(line.startsWith("cd"))
				cd(line);
			else if(line.startsWith("pwd"))
				System.out.println(p.toString());
			out.flush();
		}
	}
	
	private static class HDFSCompletor implements Completor {
		private SimpleCompletorWithoutSpace completor; 
		
		public HDFSCompletor()
		{
			completor = new SimpleCompletorWithoutSpace(new String[] {});
		}
	
		public int complete(final String buffer, final int cursor, final List clist) {
			String myBuffer = buffer == null ? "" : buffer;
			
			
			int lastSlash = myBuffer.lastIndexOf('/');
			String pathDir, pathCont;
			try {
				pathDir = myBuffer.substring(0, lastSlash);
				pathCont = myBuffer.substring(lastSlash+1, myBuffer.length());
			} catch (Exception e) {
				pathDir = "";
				pathCont = myBuffer;
			}

			String dir = p.toString();
			if (!pathDir.equals("")) {
				dir += "/" + pathDir;
			}
			String prefix = dir + "/" + pathCont;
				
			PathFilter pf = new MatchingPrefixPathFilter(prefix);
			
			FileStatus[] completions;
			try {
				completions = fs.listStatus(new Path(dir), pf);
			} catch (IOException e) {
				completions = null;
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			String[] candidates = new String[completions.length];

			for (int i = 0; i < completions.length; i++) {
				candidates[i] = trimToLeaf(completions[i].getPath().toString());
				if (!pathDir.equals("")) {
				  candidates[i] = pathDir + "/" + candidates[i];
				}
				if(completions[i].isDir())
					candidates[i] += "/";
				else
					candidates[i] += " ";
			}

			completor.setCandidateStrings(candidates);
			
			return completor.complete(myBuffer, cursor, clist);
		}
	}
	
	public static void ls(String fullCommand) throws IOException
	{
		FileStatus[] stati = fs.listStatus(p);
		for(FileStatus f : stati)
		{
			System.out.println(trimToLeaf(f.getPath().toString()));
		}
	}
	
	public static void cd(String fullCommand) throws IOException
	{
		String[] parts = fullCommand.split(" ");
		if(parts.length > 2)
		{
			System.out.println("You can't change to multiple directories, dumbass");
		}
		else if(parts.length == 1)
		{
			p = new Path(home.toString());
		}
		else if(!parts[1].startsWith("/"))
		{
			Path targetPath = new Path(p, parts[1]);
			if(fs.exists(targetPath))
			{
				p = targetPath;
			}
			else
			{
				System.out.println("Path " + targetPath.toString() + " does not exist");
			}
		}
	}
	
	public static String trimToLeaf(String path)
	{
		String[] parts = path.split("/");
		return parts[parts.length - 1];
	}
	
	private static class MatchingPrefixPathFilter implements PathFilter {
		String _prefix;

		public MatchingPrefixPathFilter(String prefix) {
			_prefix = prefix;
		}

		@Override
		public boolean accept(Path p) {
			return p.toString().startsWith(_prefix);
		}
	}

}
