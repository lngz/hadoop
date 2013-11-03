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
    	java.util.Random random =new java.util.Random();
	    FileSystem fs = FileSystem.get(conf);
	    Path f = new Path("hdfs:///dfs_operator.txt");
	    FSDataOutputStream s = fs.create(f, true);
	    int i=0;
	    for (i=0; i<100000000; ++i)
	    	
	    	s.writeBytes(Integer.toString(random.nextInt(100000000))+"\n");
	    s.close();
    } catch (IOException e) {
    e.printStackTrace();
    }
 }
}