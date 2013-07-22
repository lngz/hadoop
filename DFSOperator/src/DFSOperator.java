import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
public class DFSOperator {

 /**
  * @param args
  */
 public static void main(String[] args) {
  // TODO Auto-generated method stub
  Configuration conf = new Configuration();
    try {
	    FileSystem fs = FileSystem.get(conf);
	    Path f = new Path("hdfs:///dfs_operator.txt");
	    FSDataOutputStream s = fs.create(f, true);
	    int i=0;
	    for (i=0; i<100000; ++i)
	    	s.writeChars("test");
	    s.close();
    } catch (IOException e) {
    e.printStackTrace();
    }
 }
}