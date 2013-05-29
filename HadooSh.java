import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.JobStatus;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.ConsoleReader;
import jline.FileNameCompletor;
import jline.MultiCompletor;
import jline.NullCompletor;
import jline.SimpleCompletor;

public class HadooSh {
	private static final JobConf config = new JobConf();
  private static JobClient jobClient;
	private static FileSystem fs;
	private static Path p;
	private static Path home;
	private static Path root;

	private final static boolean DEBUG = false;

	public static void main(String[] args) throws Exception {
		// config.set("fs.default.name", "hdfs://localhost:9000");
		fs = FileSystem.get(config);
    jobClient = new JobClient(config);

		p = fs.getWorkingDirectory();
		home = new Path(p.toString());
    root = new Path("/");
		ConsoleReader reader = new ConsoleReader();
		reader.setBellEnabled(false);
		List completors = new LinkedList();
		String[] commandsList = new String[] { "cd", "ls", "pwd", "exit",
				"cat", "head", "rm", "mv", "avrocat" };
		Completor hdfsCompletor = new HDFSCompletor();
		completors.add(new SimpleCompletor(commandsList));
		completors.add(hdfsCompletor);

		Completor remoteFsCompletor = new ArgumentCompletor(completors);

    Completor localFsCompletor =
      new ArgumentCompletor(
        new Completor[] {
          new SimpleCompletor(new String[] {"local"}),
          new SimpleCompletor(commandsList),
          new FileNameCompletor()
        }
      );

    final SimpleCompletor jobSimpleComletor =
      new SimpleCompletor(new String[] {"job"});

    Completor jobSKCompletor =
      new ArgumentCompletor(
        new Completor[] {
          jobSimpleComletor,
          new SimpleCompletor(
            new String[] {
              "-status",
              "-kill"
            }
          ),
          new JobCompletor(),
          new NullCompletor()
        }
      );
    Completor jobLsTTCompletor =
      new ArgumentCompletor(
        new Completor[] {
          jobSimpleComletor,
          new SimpleCompletor(
            new String[] {
              "-list-active-trackers",
              "-list-blacklisted-trackers"
            }
          ),
          new NullCompletor()
        }
      );
    Completor jobLsCompletor =
      new ArgumentCompletor(
        new Completor[] {
          jobSimpleComletor,
          new SimpleCompletor(
            new String[] {
              "-list",
            }
          ),
          new SimpleCompletor(
            new String[] {
              "all",
            }
          ),
          new NullCompletor()
        }
      );

    final JobPriority[] jpen = JobPriority.values();
    final String[] jpstr = new String[jpen.length];
    for (int i = 0; i < jpen.length; i++) {
      jpstr[i] = jpen[i].name();
    }
    Completor jobPriorityCompletor =
      new ArgumentCompletor(
        new Completor[] {
          jobSimpleComletor,
          new SimpleCompletor(
            new String[] {
              "-set-priority",
            }
          ),
          new JobCompletor(),
          new SimpleCompletor(jpstr),
          new NullCompletor()
        }
      );
    Completor jobHistoryCompletor =
      new ArgumentCompletor(
        new Completor[] {
          jobSimpleComletor,
          new SimpleCompletor(
            new String[] {
              "-history",
            }
          ),
          hdfsCompletor,
          new NullCompletor()
        }
      );
 
    final MultiCompletor topComletor = new MultiCompletor(
      new Completor[] {
        remoteFsCompletor, 
        localFsCompletor,
        jobSKCompletor,
        jobLsTTCompletor,
        jobLsCompletor,
        jobPriorityCompletor,
        jobHistoryCompletor});
    
    reader.addCompletor(topComletor);

		PrintWriter out = new PrintWriter(System.out);
		String line;

		while ((line = reader.readLine(p.getName().toString() + " > ").trim()) != null) {
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
						oss[1].close();
						
						// Then run it through the chain...
						for(int i=2; i < pipeBreaks.length; i++)
						{
							dumpToOS(iss[i-1], oss[i]);
							iss[i-1].close();
							oss[i].close();
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
							FileSystem fs = FileSystem.get(config);
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
		FileSystem fs = FileSystem.get(config);
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
		else if(line.startsWith("avrocat"))
			avrocat(line, os);
    else if (line.startsWith("job"))
      job(line, os);
		else if (line.startsWith("local"))
			sysExec(line.substring(line.indexOf("local") + "local".length()),
					os);
		else
			sysExec(line, os);
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
		println(os, p.toString());
	}

	public static void avrocat(String fullCommand, OutputStream os) throws IOException
	{
		String[] parts = fullCommand.split(" ");
		if(parts.length < 2)
		{
			println(os, "error: no avrocat files specified");
		}
		for(int i=1; i < parts.length; i++)
		{
			Path loc = getPath(parts[1]);
			if(fs.exists(loc))
			{
				println(os, "First ten records of " + parts[1]);
				displayFile(fs, loc, os, 0, 10);
				os.write('\n');
			}
			else
				println(os, "error, no such file " + parts[1]);
		}
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
			println(os, "error, too many directories");
			return;
		} else if (parts.length == 1)
			targetDir = p;
		else {
			targetDir = new Path(parts[1]);
		}

		FileStatus[] stati = fs.listStatus(targetDir);
		for (FileStatus f : stati) {
			String s = f.getPath().getName().toString();
			println(os, f.isDir() ? s + "/" : s);
		}
	}

	public static void cat(String fullCommand, OutputStream os)
			throws IOException {
		String[] parts = fullCommand.split(" ");
		if (parts.length == 1) {
			println(os, "error, not a file");
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
					println(os, "error, not a file");
				}
			}
		}
	}

	public static void head(String fullCommand, OutputStream os)
			throws IOException {
		String[] parts = fullCommand.split(" ");
		if (parts.length > 3) {
			println(os, "error, usage: head [numLines] file");
			return;
		} else if (parts.length == 1) {
			println(os, "error, not a file");
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
					println(os, "error, no such file");
				}
			} catch (Exception e) {
				e.printStackTrace();
				println(os, "error, usage: head [numLines] file");
			}
		}
	}

	public static void cd(String fullCommand, OutputStream os)
			throws IOException {
		String[] parts = fullCommand.split(" ");
		if (parts.length > 2)
			println(os, "error, too many directories");
		else if (parts.length == 1)
			p = new Path(home.toString());
		else if (parts[1].startsWith("/")) {
			Path targetPath = new Path(root, parts[1]);
			if (fs.exists(targetPath))
				p = targetPath;
			else
				println(os, "path " + targetPath.toString() + " does not exist");
		} else {
			Path targetPath = new Path(p, parts[1]);
			if (fs.exists(targetPath))
				p = targetPath;
			else
				println(os, "path " + targetPath.toString() + " does not exist");
		}
	}

  public static void job(String fullCommand, OutputStream os)
    throws IOException
  {
    final String[] origargs = fullCommand.split(" ");
    final String[] args = new String[origargs.length - 1];
    System.arraycopy(origargs, 1, args, 0, args.length);
    try {
      jobClient.run(args);
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException)e;
      }
      throw new IOException(e);
    }
  }

	private static class HDFSCompletor implements Completor {
		public int complete(final String buffer, final int cursor,
				final List clist) {
			String myBuffer = (buffer == null ? "" : buffer)
                      + "*"; // avoid empty path
      final Path glob = new Path(myBuffer);
      final Path parent = glob.getParent();
			FileStatus[] completions;
			try {
				completions = glob.isAbsolute()
          ? fs.globStatus(glob)
          : fs.globStatus(new Path(p, glob));
			} catch (IOException e) {
				completions = new FileStatus[0];
				e.printStackTrace();
			}

			for (int i = 0; i < completions.length; i++) {
        clist.add(
            new Path(parent, completions[i].getPath().getName()).toString()
          + (completions[i].isDir() ? "/" : " "));
			}
      return clist.size() == 0 ? -1 : 0;
		}
	}

  private static final class JobCompletor implements Completor {
		public int complete(
      final String buffer,
      final int cursor,
			final List clist)
    {
      try {
        JobStatus[] jobs = jobClient.jobsToComplete();
        String b = (buffer == null ? "" : buffer);
        for (JobStatus j : jobs) {
          final String jstr = j.getJobID().toString();
          if (jstr.startsWith(b)) {
            clist.add(jstr.length() == b.length()
              ? jstr + " "
              : jstr);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      return clist.size() == 0 ? -1 : 0;
    }
  }

	// Borrowed from Azkaban source
	// https://github.com/azkaban/azkaban/blob/master/azkaban-common/src/java/azkaban/common/web/HdfsAvroFileViewer.java
	private static DataFileStream<Object> getAvroDataStream(FileSystem fs, Path path) throws IOException {
        GenericDatumReader<Object> avroReader = new GenericDatumReader<Object>();
        InputStream hdfsInputStream = fs.open(path);
        return new DataFileStream<Object>(hdfsInputStream, avroReader);

    }

    public static void displayFile(FileSystem fs,
                            Path path,
                            OutputStream outputStream,
                            int startLine,
                            int endLine) throws IOException {

        DataFileStream<Object> avroDatastream = null;

        try {
            avroDatastream = getAvroDataStream(fs, path);
            Schema schema = avroDatastream.getSchema();
            DatumWriter<Object> avroWriter = new GenericDatumWriter<Object>(schema);

            JsonGenerator g = new JsonFactory().createJsonGenerator(outputStream, JsonEncoding.UTF8);
            g.useDefaultPrettyPrinter();
            Encoder encoder = new JsonEncoder(schema, g);

            int lineno = 1; // line number starts from 1
            while(avroDatastream.hasNext() && lineno <= endLine) {
                Object datum = avroDatastream.next();
                if(lineno >= startLine) {
                    String record = "\n\n Record " + lineno + ":\n";
                    outputStream.write(record.getBytes("UTF-8"));
                    avroWriter.write(datum, encoder);
                    encoder.flush();
                }
                lineno++;
            }
        } catch(IOException e) {
            outputStream.write(("Error in display avro file: " + e.getLocalizedMessage()).getBytes("UTF-8"));
            throw e;
        } finally {
            avroDatastream.close();
        }
    }
}
