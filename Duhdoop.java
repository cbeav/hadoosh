import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Duhdoop {

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		config.set("fs.default.name", "hdfs://localhost:9000");
		FileSystem fs = FileSystem.get(config);
		
		Path p = fs.getWorkingDirectory();
		Path home = new Path(p.toString());
		
		while (true) {
			System.out.print("> ");
			String line = readLine();
			if (line.equals("exit"))
				break;
			else if(line.startsWith("ls"))
			{
				FileStatus[] stati = fs.listStatus(p);
				System.out.println("Found " + stati.length + " items");
				for(FileStatus f : stati)
				{
					System.out.println(f.getPath().toString());
				}
			}
			else if(line.startsWith("cd"))
			{
				String[] parts = line.split(" ");
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
		}
	}

	public static String readLine() throws IOException {
		StringBuffer s = new StringBuffer();
		char c;
		while ((c = (char) System.in.read()) != '\n') {
			if (c == '\b' && s.length() > 0)
				s.setLength(s.length() - 1);
			else
				s.append(c);
		}
		return s.toString();
	}

}
