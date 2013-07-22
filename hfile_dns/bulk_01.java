import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

 
public class bulk_01 {
 
        public static class HBaseHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
                private ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
 
                protected void map(LongWritable key, Text value, Context context) {
                        try {
                                String[] strs = value.toString().split("\t");
                                immutableBytesWritable.set(Bytes.toBytes(strs[0]));
                                KeyValue kv = createKeyValue(value.toString());
                                context.write(immutableBytesWritable, kv);
                        } catch (IOException e) {
                                e.printStackTrace();
                        } catch (InterruptedException e) {
                                e.printStackTrace();
                        }
                }
                private KeyValue createKeyValue(String str)
                {
                        String[] strs = str.split("\t");
                        if(strs.length<3)
                                return null;
                        String row=strs[0];
                        String family="f1";
                        String qualifier=strs[1];
                        String value=strs[2];
                        return new KeyValue(Bytes.toBytes(row),Bytes.toBytes(family),Bytes.toBytes(qualifier),
                                        System.currentTimeMillis(), Bytes.toBytes(value));
                }
        }
 
        public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: bulk_01 <in> <hfile dir>");
      System.exit(2);
    } 
        Job job = new Job(conf, "bulk load dns result file");
 
        job.setJarByClass(bulk_01.class);
        job.setMapperClass(HBaseHFileMapper.class);
        job.setReducerClass(KeyValueSortReducer.class);  
 
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        HFileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
 
        }
 
}
