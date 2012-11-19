import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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

	private final static boolean DEBUG = false;

	public static void main(String[] args) throws Exception {
		config = new Configuration();
		// config.set("fs.default.name", "hdfs://localhost:9000");
		fs = FileSystem.get(config);

		p = fs.getWorkingDirectory();
		home = new Path(p.toString());

		String homeStr = home.toString();
		rootStr = homeStr.substring(0, homeStr.indexOf("/user/", 0));
		root = new Path(rootStr);

		ConsoleReader reader = new ConsoleReader();
		reader.setBellEnabled(false);
		List completors = new LinkedList();
		String[] commandsList = new String[] { "cd", "ls", "pwd", "exit",
				"cat", "head", "local", "rm", "mv" };
		Completor fileCompletor = new HDFSCompletor();
		completors.add(new SimpleCompletor(commandsList));
		completors.add(fileCompletor);
		reader.addCompletor(new ArgumentCompletor(completors));

		PrintWriter out = new PrintWriter(System.out);
		String line;

		while ((line = reader.readLine(trimToLeaf(p.toString()) + " > ").trim()) != null) {
			try {
				PipedInputStream is;
				if (line.equals("exit"))
					break;
				else {
					int localOut = line.indexOf(">l");
					int remoteOut = line.indexOf(">");
					String outLoc = "";
					if(localOut > 0)
					{
						outLoc = line.split(">l")[1];
						line = line.substring(0, localOut);
					}
					else if (remoteOut > 0)
					{
						outLoc = line.split(">")[1];
						line = line.substring(0, remoteOut);
					}
					outLoc = outLoc.trim();
					
					String[] pipeBreaks = line.split("\\|");
					OutputStream[] oss = new OutputStream[pipeBreaks.length];
					InputStream[] iss = new InputStream[pipeBreaks.length];
					
					if (pipeBreaks.length > 1) {
						
						// First, set up all input and output streams
						Runtime rt = java.lang.Runtime.getRuntime();
						for(int i=1; i < pipeBreaks.length; i++)
						{
							Process pr = rt.exec(pipeBreaks[i]);
							iss[i] = pr.getInputStream();
							oss[i] = pr.getOutputStream();
						}
						// Now connect our first process to the first outputstream
						getCmdOutput(pipeBreaks[0], oss[1]);
						
						// Then run it through the chain...
						for(int i=2; i < pipeBreaks.length; i++)
						{
							dumpToOS(iss[i-1], oss[i]);
						}

						// Close up our streams
						for(int i=1; i < pipeBreaks.length; i++)
						{
							oss[i].close();
							if(i < pipeBreaks.length - 1)
								iss[i].close();
						}
						
						// Now take our final output, and write it where appropriate
						InputStream finalIn = iss[pipeBreaks.length - 1];
						
						if(localOut > 0)
							dumpToFile(finalIn, outLoc);
						else if(remoteOut > 0)
						{
							dumpToHDFS(finalIn, outLoc);
						}
						else
							dumpToOS(finalIn, System.out);
						finalIn.close();
					} else
					{
						if(localOut > 0)
						{
							FileOutputStream os = new FileOutputStream(getLocalPath(outLoc));
							getCmdOutput(pipeBreaks[0], os);
							os.close();
						}
						else if(remoteOut > 0)
						{
							FileSystem fs = FileSystem.get(new Configuration());
							Path outPath = getPath(outLoc);
							FSDataOutputStream os = fs.create(outPath, true);
							getCmdOutput(pipeBreaks[0], os);
							os.close();
						}
						else
							getCmdOutput(pipeBreaks[0], System.out);
					}

				}
				out.flush();
			} catch (Exception e) {
				if (DEBUG)
					e.printStackTrace();
				System.out.println("command not found");
			} catch (Throwable e) {
				if (DEBUG)
					e.printStackTrace();
				System.out.println();
			}
		}
	}

	private static void dumpToOS(InputStream is, OutputStream os)
			throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String line;
		while ((line = br.readLine()) != null)
		{
			os.write(line.getBytes());
			os.write('\n');
		}

		br.close();
	}
	
	private static void dumpToFile(InputStream is, String loc)
			throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		// Warning, blindly over-writing files?
		String fullLoc = getLocalPath(loc);
		BufferedWriter os = new BufferedWriter(new FileWriter(fullLoc));
		
		String line;
		while ((line = br.readLine()) != null)
		{
			os.write(line + '\n');
		}

		os.close();
		br.close();
		is.close();
	}
	
	private static void dumpToHDFS(InputStream is, String loc)
			throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		FileSystem fs = FileSystem.get(new Configuration());
		Path outPath = getPath(loc);
		FSDataOutputStream os = fs.create(outPath, true);
		
		String line;
		while ((line = br.readLine()) != null)
		{
			os.write(line.getBytes());
			os.write('\n');
		}

		os.close();
		br.close();
		is.close();
	}
	
	private static void dump(InputStream is, PrintStream os)
			throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		
		String line;
		while ((line = br.readLine()) != null)
		{
			os.write(line.getBytes());
			os.write('\n');
		}

		br.close();
		is.close();
	}
	
	private static Path getPath(String input)
	{
		return input.startsWith("/") ? new Path(input) : new Path(p, input);
	}
	
	private static String getLocalPath(String input)
	{
		String localPath;
		return input.startsWith("/") ? input : System.getProperty("user.dir") + "/" + input;
	}

	private static void getCmdOutput(String cmd, OutputStream os)
			throws IOException, InterruptedException {

		String line = cmd;

		if (line.startsWith("ls"))
			ls(line, os);
		else if (line.startsWith("cd"))
			cd(line, os);
		else if (line.startsWith("cat"))
			cat(line, os);
		else if (line.startsWith("head"))
			head(line, os);
		else if (line.startsWith("pwd"))
			pwd(line, os);
		else if(line.startsWith("rm"))
			rm(line, os);
		else if(line.startsWith("mv"))
			mv(line, os);
		else if (line.startsWith("local"))
			sysExec(line.substring(line.indexOf("local") + "local".length()),
					os);
		else
			sysExec(line, os);

		//os.close();
	}

	private static void println(OutputStream os, String s) throws IOException {
		os.write(s.getBytes());
		os.write('\n');
	}

	private static void sysExec(String line, OutputStream os)
			throws IOException, InterruptedException {
		Runtime rt = java.lang.Runtime.getRuntime();
		Process p = rt.exec(line);
		p.waitFor();
		InputStream is = p.getInputStream();
		InputStream es = p.getErrorStream();
		BufferedReader br = new BufferedReader(new InputStreamReader(is));

		String s = null;
		while ((s = br.readLine()) != null) {
			println(os, s);
		}
		br.close();

		br = new BufferedReader(new InputStreamReader(es));
		s = null;
		while ((s = br.readLine()) != null) {
			println(os, s);
		}
		br.close();
	}

	private static void pwd(String line, OutputStream os)
			throws UnsupportedEncodingException, IOException {
		println(os,
				p.toString().substring(
						p.toString().indexOf(rootStr) + rootStr.length()));
	}
	
	public static void rm(String fullCommand, OutputStream os) throws IOException
	{
		String[] parts = fullCommand.split(" ");
		for(int i=1; i < parts.length; i++)
		{
			Path targ = getPath(parts[i]);
			if(fs.exists(targ))
				fs.delete(targ);
			else
				println(os, "can't delete + " + targ + ". no such file");
		}
	}
	
	public static void mv(String fullCommand, OutputStream os) throws IOException
	{
		String[] parts = fullCommand.split(" ");
		if(parts.length != 3)
			println(os, "error: not given input and output path for move");
		else
		{
			Path src = getPath(parts[1]);
			Path dst = getPath(parts[2]);
			if(fs.exists(src))
				fs.rename(src, dst);
			else
				println(os, "can't move + " + src + ". no such file");
		}
	}

	public static void ls(String fullCommand, OutputStream os)
			throws IOException {
		String[] parts = fullCommand.split(" ");
		Path targetDir = null;
		if (parts.length > 2) {
			println(os, "You can't change to multiple directories");
			return;
		} else if (parts.length == 1)
			targetDir = p;
		else {
			targetDir = new Path(parts[1]);
		}

		FileStatus[] stati = fs.listStatus(targetDir);
		for (FileStatus f : stati) {
			String s = trimToLeaf(f.getPath().toString());
			println(os, f.isDir() ? s + "/" : s);
		}
	}

	public static void cat(String fullCommand, OutputStream os)
			throws IOException {
		String[] parts = fullCommand.split(" ");
		if (parts.length == 1) {
			println(os, "Error: not a file");
			return;
		} else {
			for (int i = 1; i < parts.length; i++) {
				Path f = new Path(p.toString(), parts[i]);
				if (fs.exists(f)) {
					BufferedReader br = new BufferedReader(
							new InputStreamReader(fs.open(f)));
					String s;
					while ((s = br.readLine()) != null) {
						println(os, s);
					}
					br.close();
				} else {
					println(os, "Error: not a file");
				}
			}
		}
	}

	public static void head(String fullCommand, OutputStream os)
			throws IOException {
		String[] parts = fullCommand.split(" ");
		if (parts.length > 3) {
			println(os, "Usage error: head [numLines] file");
			return;
		} else if (parts.length == 1) {
			println(os, "Error: not a file");
			return;
		} else {
			try {
				int numLines = 1;
				Path f;
				if (parts.length == 3) {
					numLines = Integer.parseInt(parts[1]);
					f = new Path(p.toString(), parts[2]);
				} else
					f = new Path(p.toString(), parts[1]);
				if (fs.exists(f)) {
					BufferedReader br = new BufferedReader(
							new InputStreamReader(fs.open(f)));
					String line = null;
					int count = 0;
					while ((line = br.readLine()) != null && count < numLines) {
						println(os, line);
						count++;
					}
					br.close();
				} else {
					println(os, "Error: no such file");
				}
			} catch (Exception e) {
				e.printStackTrace();
				println(os, "Usage error: head [numLines] file");
			}
		}
	}

	public static void cd(String fullCommand, OutputStream os)
			throws IOException {
		String[] parts = fullCommand.split(" ");
		if (parts.length > 2)
			println(os, "You can't change to multiple directories");
		else if (parts.length == 1)
			p = new Path(home.toString());
		else if (parts[1].startsWith("/")) {
			Path targetPath = new Path(root, parts[1]);
			if (fs.exists(targetPath))
				p = targetPath;
			else
				println(os, "Path " + targetPath.toString() + " does not exist");
		} else {
			Path targetPath = new Path(p, parts[1]);
			if (fs.exists(targetPath))
				p = targetPath;
			else
				println(os, "Path " + targetPath.toString() + " does not exist");
		}
	}

	public static String trimToLeaf(String path) {
		String[] parts = path.split("/");
		return parts[parts.length - 1];
	}

	private static class HDFSCompletor implements Completor {
		private SimpleCompletorWithoutSpace completor;

		public HDFSCompletor() {
			completor = new SimpleCompletorWithoutSpace(new String[] {});
		}

		public int complete(final String buffer, final int cursor,
				final List clist) {
			String myBuffer = buffer == null ? "" : buffer;
			int lastSlash = myBuffer.lastIndexOf('/');
			String pathDir, pathCont;
			try {
				pathDir = myBuffer.substring(0, lastSlash);
				pathCont = myBuffer.substring(lastSlash + 1, myBuffer.length());
			} catch (Exception e) {
				pathDir = "";
				pathCont = myBuffer;
			}

			String dir;
			if (myBuffer.startsWith("/")) {
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
				} else if (myBuffer.startsWith("/")) {
					candidates[i] = "/" + candidates[i];
				}

				if (completions[i].isDir())
					candidates[i] += "/";
				else
					candidates[i] += " ";
			}

			completor.setCandidateStrings(candidates);
			return completor.complete(myBuffer, cursor, clist);
		}
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
