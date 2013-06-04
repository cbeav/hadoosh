import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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
import java.net.URI;
import java.security.Permission;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.util.RunJar;

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
	private static Path home;
  private static FsShell dfsShell;
  private static URI defaultUri;
  private static URI localURI;
	private final static boolean DEBUG = false;

  // Disallow RunJar call System.exit [http://stackoverflow.com/questions/5401281/preventing-system-exit-from-api]
  private static class ExitTrappedException extends SecurityException { }

  private static void forbidSystemExitCall() {
    final SecurityManager securityManager = new SecurityManager() {
      public void checkPermission(Permission permission ) {
        if(permission.getName().startsWith("exitVM")) {
          throw new ExitTrappedException();
        }
      }
    };
    System.setSecurityManager( securityManager ) ;
  }

  private static void enableSystemExitCall() {
    System.setSecurityManager(null);
  }

  private static final String[] commonFsCmdList = new String[] {
    "ls",                                                      // <path>]
    "lsr",                                                     // <path>]
    "du",                                                      // <path>]
    "dus",                                                     // <path>]
    "count",                                              // [-q] <path>]
    "mv",                                                 // <src> <dst>]
    "cp",                                                 // <src> <dst>]
    "rm",                                        //  [-skipTrash] <path>]
    "rmr",                                        // [-skipTrash] <path>]
    "cat",                                                      // <src>]
    "text",                                                     // <src>]
    "mkdir",                                                   // <path>]
    "touchz",                                                  // <path>]
    "test",                                             // -[ezd] <path>]
    "stat",                                           // [format] <path>]
    "tail",                                               // [-f] <file>]
    "chmod",                // [-R] <MODE[,MODE]... | OCTALMODE> PATH...]
    "chown",                           // [-R] [OWNER][:[GROUP]] PATH...]
    "chgrp"                                        // [-R] GROUP PATH...]
  };

  private static final String[] dfsCmdListNoArgs = new String[] {
    "expunge"
  };

  private static final String[] dfsCmdListLocalRemote = new String[] {
    "put",                                       // <localsrc> ... <dst>]
    "copyFromLocal",                             // <localsrc> ... <dst>]
    "moveFromLocal"                              // <localsrc> ... <dst>]
  };

  private static final String[] dfsCmdListRemoteLocal = new String[] {
    "get",                       // [-ignoreCrc] [-crc] <src> <localdst>]
    "getmerge",                              // <src> <localdst> [addnl]]
    "copyToLocal",               // [-ignoreCrc] [-crc] <src> <localdst>]
    "moveToLocal",                            // [-crc] <src> <localdst>]
  };

  private static final String[] dfsCmdListRemote = new String[] {
    "setrep"                              // [-R] [-w] <rep> <path/file>]
  };

  private static final String[][] fsShellCmds = new String[][] {
    commonFsCmdList,
    dfsCmdListNoArgs,
    dfsCmdListLocalRemote,
    dfsCmdListRemoteLocal,
    dfsCmdListRemote
  };

	public static void main(String[] args) throws Exception {
    forbidSystemExitCall();
		// config.set("fs.default.name", "hdfs://localhost:9000");
		fs = FileSystem.get(config);
    defaultUri = FileSystem.getDefaultUri(config);
    localURI = FileSystem.getLocal(config).getUri();

    dfsShell = new FsShell(config);
    jobClient = new JobClient(config);
		home = fs.getWorkingDirectory();

		ConsoleReader reader = new ConsoleReader();
		reader.setBellEnabled(false);
		List completors = new LinkedList();
		String[] commandsList = new String[] {
      "cd",
      "head",
      "avrocat"
    };

		final Completor hdfsFileNameCompletor = new HDFSCompletor();
    final Completor localFileNameCompletor = new FileNameCompletor();
    final Completor nullCompletor = new NullCompletor();
    final Completor jobCompletor = new JobCompletor();
    final Completor localJarCompletor = new FileNameCompletor() {
      public int matchFiles(
        String buffer,
        String translated,
        File[] entries,
        List clist)
      {
        if (entries == null) return -1;

        for (int i = 0; i < entries.length; i++) {
          String name = entries[i].getName();
          if ((   entries[i].isDirectory()
               || name.endsWith(".jar"))
           && entries[i].getAbsolutePath().startsWith(translated))
          {
            name += (entries[i].isDirectory() ? File.separator : " ");
            clist.add(name);
          }
        }
        if (clist.size() > 0) {
          final int index = buffer.lastIndexOf(File.separator);
          return index + File.separator.length();
        }
        return -1;
      }
    };

		final Completor remoteFsCompletor = new ArgumentCompletor(
      new Completor[] {
        new MultiCompletor(
          new Completor[] {
            new SimpleCompletor(commonFsCmdList),
            new SimpleCompletor(dfsCmdListRemote),
            new SimpleCompletor(commandsList)
          }
        ),
        hdfsFileNameCompletor
      }
    );

    final Completor localFsCompletor = new ArgumentCompletor(
      new Completor[] {
        new SimpleCompletor(new String[] {"local"}),
        new SimpleCompletor(commonFsCmdList),
        localFileNameCompletor
      }
    );

    final Completor localRemoteCompletor = new ArgumentCompletor(
      new Completor[] {
        new SimpleCompletor(dfsCmdListLocalRemote),
        localFileNameCompletor,
        hdfsFileNameCompletor,
        nullCompletor
      }
    );

    final Completor remoteLocalCompletor = new ArgumentCompletor(
      new Completor[] {
        new SimpleCompletor(dfsCmdListRemoteLocal),
        hdfsFileNameCompletor,
        localFileNameCompletor,
        nullCompletor
      }
    );

    final SimpleCompletor jobSimpleComletor =
      new SimpleCompletor(new String[] {"job"});

    final Completor jobSKCompletor = new ArgumentCompletor(
      new Completor[] {
        jobSimpleComletor,
        new SimpleCompletor(
          new String[] {
            "-status",
            "-counter",
            "-kill",
            "-events"
          }
        ),
        jobCompletor,
        nullCompletor
      }
    );

    final JobPriority[] jpen = JobPriority.values();
    final String[] jpstr = new String[jpen.length];
    for (int i = 0; i < jpen.length; i++) {
      jpstr[i] = jpen[i].name();
    }
    final Completor jobPriorityCompletor = new ArgumentCompletor(
      new Completor[] {
        jobSimpleComletor,
        new SimpleCompletor(
          new String[] {
            "-set-priority",
          }
        ),
        jobCompletor,
        new SimpleCompletor(jpstr),
        nullCompletor
      }
    );

    final Completor jobLsTTCompletor = new ArgumentCompletor(
      new Completor[] {
        jobSimpleComletor,
        new SimpleCompletor(
          new String[] {
            "-list-active-trackers",
            "-list-blacklisted-trackers"
          }
        ),
        nullCompletor
      }
    );

    final Completor jobLsCompletor = new ArgumentCompletor(
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
        nullCompletor
      }
    );


    final Completor jobHistoryCompletor = new ArgumentCompletor(
      new Completor[] {
        jobSimpleComletor,
        new SimpleCompletor(
          new String[] {
            "-history",
          }
        ),
        hdfsFileNameCompletor,
        nullCompletor
      }
    );

    final Completor jobKillTaskCompletor = new ArgumentCompletor(
      new Completor[] {
        jobSimpleComletor,
        new SimpleCompletor(
          new String[] {
            "-kill-task",
            "-fail-task"
          }
        ),
        nullCompletor
      }
    );

    final Completor runJarCompletor = new ArgumentCompletor(
      new Completor[] {
        new SimpleCompletor(new String[] { "runjar" }),
        localJarCompletor,
        nullCompletor
      }
    );

    final MultiCompletor topComletor = new MultiCompletor(
      new Completor[] {
        new SimpleCompletor(new String[] {"pwd","exit"}),
        new SimpleCompletor(dfsCmdListNoArgs),
        remoteFsCompletor,
        localFsCompletor,
        remoteLocalCompletor,
        localRemoteCompletor,
        jobSKCompletor,
        jobLsTTCompletor,
        jobLsCompletor,
        jobPriorityCompletor,
        jobHistoryCompletor,
        jobKillTaskCompletor,
        runJarCompletor
      }
    );

    reader.addCompletor(topComletor);

		PrintWriter out = new PrintWriter(System.out);
		String line;

		while ((line = reader.readLine(fs.getWorkingDirectory().getName() + " > ").trim()) != null) {
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
		return new Path(input);
	}
	
	private static String getLocalPath(String input)
	{
		return input.startsWith("/") ? input : System.getProperty("user.dir") + "/" + input;
	}

	private static void getCmdOutput(String cmd, OutputStream os)
			throws IOException, InterruptedException {

    final boolean isLocal = cmd.startsWith("local");
    int largIndex = "local ".length();
    if (isLocal) {
      FileSystem.setDefaultUri(config, localURI);
      for (int i = largIndex; i < cmd.length(); i++) {
        if (cmd.charAt(i) != ' ') {
          largIndex = i;
          break;
        }
      }
    } else {
      FileSystem.setDefaultUri(config, defaultUri);
    }

		String line = isLocal
      ? cmd.substring(largIndex, cmd.length())
      : cmd;

    boolean isFsShellCmd = false;
    // TODO put into a tree for O(logN) lookup if big
    for (int i = 0; !isFsShellCmd && i < fsShellCmds.length; i++) {
      for (int j = 0; !isFsShellCmd && j < fsShellCmds[i].length; j++) {
        isFsShellCmd = line.startsWith(fsShellCmds[i][j]);
      }
    }

		if (isFsShellCmd)
			execFsShell(line, os);
		else if (line.startsWith("cd"))
			cd(line, os);
		else if (line.startsWith("head"))
			head(line, os);
		else if (line.startsWith("pwd"))
			pwd(line, os);
		else if(line.startsWith("avrocat"))
			avrocat(line, os);
    else if (line.startsWith("job"))
      job(line, os);
    else if (line.startsWith("runjar"))
      runjar(line, os);
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
		println(os, fs.getWorkingDirectory().toString());
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

	public static void execFsShell(String fullCommand, OutputStream os)
			throws IOException {
    final PrintStream oldout = System.out;
		String[] parts = fullCommand.split(" ");
    parts[0] = "-" + parts[0];
    for (int i = 1; i < parts.length; i++) {
      parts[i] = new Path(parts[i]).toString();
    }
    try {
      System.setOut(new PrintStream(os));
      dfsShell.run(parts);
      System.out.flush();
    } catch (Throwable e) {
      if (e instanceof IOException) {
        throw (IOException)e;
      }
      throw new IOException(e);
    } finally {
      System.setOut(oldout);
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
				Path f = new Path(parts[i]);
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
					f = new Path(parts[2]);
				} else
					f = new Path(parts[1]);
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
      fs.setWorkingDirectory(home);
    else {
      fs.setWorkingDirectory(new Path(parts[1]));
    }
	}

  public static void job(String fullCommand, OutputStream os)
    throws IOException
  {
    final PrintStream oldout = System.out;
    final int argIndex = fullCommand.indexOf("-");
    try {
      final String[] args = argIndex >= 0
        ? fullCommand.substring(argIndex, fullCommand.length()).split(" ")
        : new String[0];
      System.setOut(new PrintStream(os));
      jobClient.run(args);
      System.out.flush();
    } catch (Throwable e) {
      if (e instanceof IOException) {
        throw (IOException)e;
      }
      throw new IOException(e);
    } finally {
      System.setOut(oldout);
    }
  }

  public static void runjar(String fullCommand, OutputStream os)
    throws IOException
  {
    final PrintStream oldout = System.out;
    int argIndex = -1;
    for (int i = "runjar".length(); i < fullCommand.length(); i++) {
      if (!Character.isWhitespace(fullCommand.charAt(i))) {
        argIndex = i;
        break;
      }
    }

    try {
      final String[] args = argIndex >= 0
        ? fullCommand.substring(argIndex, fullCommand.length()).split(" ")
        : new String[0];

      System.setOut(new PrintStream(os));
      RunJar.main(args);
      System.out.flush();
    } catch (ExitTrappedException e) {
      //
      // intentionally swallow
      //
    } catch (Throwable e) {
      if (e instanceof IOException) {
        throw (IOException)e;
      }
      throw new IOException(e);
    } finally {
      System.setOut(oldout);
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
				completions = fs.globStatus(glob);
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
        if (jobs != null) {
          String b = (buffer == null ? "" : buffer);
          for (JobStatus j : jobs) {
            final String jstr = j.getJobID().toString();
            if (jstr.startsWith(b)) {
              clist.add(jstr.length() == b.length()
                ? jstr + " "
                : jstr);
            }
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
