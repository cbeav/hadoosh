import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
	static String rootStr;
	static Path root;

	public static void main(String[] args) throws Exception {
		config = new Configuration();
		//config.set("fs.default.name", "hdfs://localhost:9000");
		fs = FileSystem.get(config);
		
		p = fs.getWorkingDirectory();
		home = new Path(p.toString());

		String homeStr = home.toString();
		rootStr = homeStr.substring(0, homeStr.indexOf("/user/", 0));
		root = new Path(rootStr);
		
		ConsoleReader reader = new ConsoleReader();
		reader.setBellEnabled(false);
		List completors = new LinkedList();
		String[] commandsList = new String[] {"cd", "ls", "pwd", "exit", "cat", "head", "local"};
		Completor fileCompletor = new HDFSCompletor();
		completors.add(new SimpleCompletor(commandsList));
		completors.add(fileCompletor);
		reader.addCompletor(new ArgumentCompletor(completors));
	
		
		String line;
		PrintWriter out = new PrintWriter(System.out);
		
		while ((line = reader.readLine(trimToLeaf(p.toString()) + " > ").trim()) != null)
		{
			try
			{
				if (line.equals("exit"))
					break;
				else if(line.startsWith("ls"))
					ls(line);
				else if(line.startsWith("cd"))
					cd(line);
				else if(line.startsWith("cat"))
					cat(line);
				else if(line.startsWith("head"))
					head(line);
				else if(line.startsWith("pwd"))
					System.out.println(p.toString().substring(p.toString().indexOf(rootStr) + rootStr.length()));
				else if(line.startsWith("local"))
					sysExec(line.substring(line.indexOf("local") + "local".length()));
				else
					sysExec(line);
				out.flush();
			}
			catch (Exception e)
			{
				e.printStackTrace();
				// Do nothing!
			}
			catch (Throwable t)
			{
				t.printStackTrace();
				// Do nothing!
			}
		}
	}
	
	private static void sysExec(String line) throws IOException, InterruptedException {
        Runtime rt = java.lang.Runtime.getRuntime();
        Process p = rt.exec(line);
        p.waitFor();
        InputStream is = p.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        // And print each line
        String s = null;
        while ((s = reader.readLine()) != null) {
            System.out.println(s);
        }
        is.close();
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
			
			String dir;
			if(myBuffer.startsWith("/"))
			{
				dir = rootStr + "/";
			} else {
				dir = p.toString() + "/";
			}
 
			if (!pathDir.equals("") || myBuffer.equals("/")) {
				String extra = pathDir;
				if (myBuffer.startsWith("/"))
						extra = extra.substring(1);
				dir += extra + "/";
			}
			
			String prefix = dir;
			if (!pathCont.equals("")) {
			   prefix += pathCont;
			}
			
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
				else if (myBuffer.startsWith("/")) {
					candidates[i] = "/" + candidates[i];
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
		String[] parts = fullCommand.split(" ");
		Path targetDir = null;
		if(parts.length > 2)
		{
			System.out.println("You can't change to multiple directories");
			return;
		}
		else if(parts.length == 1)
			targetDir = p;
		else
		{
			targetDir = new Path(parts[1]);
		}
		
		FileStatus[] stati = fs.listStatus(targetDir);
		for(FileStatus f : stati)
		{
			String s = trimToLeaf(f.getPath().toString());
			System.out.println(f.isDir() ? s + "/" : s);
		}
	}
	
	public static void cat(String fullCommand) throws IOException
	{
		String[] parts = fullCommand.split(" ");
		if(parts.length == 1)
		{
			System.out.println("Error: not a file");
			return;
		}
		else
		{
			for(int i=1; i<parts.length; i++)
			{
				Path f = new Path(p.toString(), parts[i]);
				if(fs.exists(f))
				{
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(f)));
					String s;
				    while((s=br.readLine()) != null)
					{
					    System.out.println(s);
					}
				    br.close();
				}
				else
				{
					System.out.println("Error: not a file");
				}
			}
		}
	}
	
	public static void head(String fullCommand) throws IOException
	{
		String[] parts = fullCommand.split(" ");
		if(parts.length > 3)
		{
			System.out.println("Usage error: head [numLines] file");
			return;
		}
		else if(parts.length == 1)
		{
			System.out.println("Error: not a file");
			return;
		}
		else
		{
			try
			{
				int numLines = 1;
				Path f;
				if(parts.length == 3)
				{
					numLines = Integer.parseInt(parts[1]);
					f = new Path(p.toString(), parts[2]);
				}
				else
					f = new Path(p.toString(), parts[1]);
				if(fs.exists(f))
				{
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(f)));
					String line = null;
					int count = 0;
					while((line = br.readLine()) != null && count < numLines)
					{
						System.out.println(line);
						count++;
					}
					br.close();
				}
				else
				{
					System.out.println("Error: no such file");
				}
			} catch (Exception e)
			{
				e.printStackTrace();
				System.out.println("Usage error: head [numLines] file");
			}
		}
	}
	
	public static void cd(String fullCommand) throws IOException
	{
		String[] parts = fullCommand.split(" ");
		if(parts.length > 2)
			System.out.println("You can't change to multiple directories");
		else if(parts.length == 1)
			p = new Path(home.toString());
		else if(parts[1].startsWith("/"))
		{

			
			Path targetPath = new Path(root, parts[1]);
			if(fs.exists(targetPath))
				p = targetPath;
			else
				System.out.println("Path " + targetPath.toString() + " does not exist");
		}
		else
		{
			Path targetPath = new Path(p, parts[1]);
			if(fs.exists(targetPath))
				p = targetPath;
			else
				System.out.println("Path " + targetPath.toString() + " does not exist");
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
