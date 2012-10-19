import org.apache.hadoop.*;

public class Duhdoop {

  public static void main(String[] args) throws Exception
  {
    while(true)
    {
      System.out.print("> ");
      if(readLine().equals("exit"))
    	  break;
    }
  }
  public static String readLine() 
        throws java.io.IOException
  {
    StringBuffer s = new StringBuffer();
    char c;
    while ((c = (char)System.in.read())!= '\n')
    {
        if (c == '\b' && s.length() > 0)
            s.setLength(s.length() - 1);
        else
            s.append(c);
    }
    System.out.println(s.toString());
    return s.toString();
  }

}
