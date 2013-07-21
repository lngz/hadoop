import java.io.IOException;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.DoubleWritable;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  
import org.apache.hadoop.util.Tool;  
import org.apache.hadoop.util.ToolRunner;  
  
public class AveragingWithCombiner extends Configured implements Tool {  
  
    public static class MapClass extends Mapper<LongWritable,Text,Text,Text> {  
          
        static enum ClaimsCounters { MISSING, QUOTED };  
        // Map Method  
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
            String fields[] = value.toString().split(",", -20);  
            String country = fields[4];  
            String numClaims = fields[8];  
              
            if (numClaims.length() > 0 && !numClaims.startsWith("\"")) {  
                context.write(new Text(country), new Text(numClaims + ",1"));  
            }  
        }  
    }  
      
    public static class Reduce extends Reducer<Text,Text,Text,DoubleWritable> {  
          
        // Reduce Method  
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
            double sum = 0;  
            int count = 0;  
            for (Text value : values) {  
                String fields[] = value.toString().split(",");  
                sum += Double.parseDouble(fields[0]);  
                count += Integer.parseInt(fields[1]);  
            }  
            context.write(key, new DoubleWritable(sum/count));  
        }  
    }  
      
    public static class Combine extends Reducer<Text,Text,Text,Text> {  
          
        // Reduce Method  
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
            double sum = 0;  
            int count = 0;  
            for (Text value : values) {  
                String fields[] = value.toString().split(",");  
                sum += Double.parseDouble(fields[0]);  
                count += Integer.parseInt(fields[1]);  
            }  
            context.write(key, new Text(sum+","+count));  
        }  
    }  
      
    // run Method  
    public int run(String[] args) throws Exception {  
        // Create and Run the Job  
        Job job = new Job();  
        job.setJarByClass(AveragingWithCombiner.class);  
          
        FileInputFormat.addInputPath(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
          
        job.setJobName("AveragingWithCombiner");  
        job.setMapperClass(MapClass.class);  
        job.setCombinerClass(Combine.class);  
        job.setReducerClass(Reduce.class);  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
          
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
          
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
        return 0;  
    }  
      
    public static void main(String[] args) throws Exception {  
        int res = ToolRunner.run(new Configuration(), new AveragingWithCombiner(), args);  
        System.exit(res);  
    }  
  
}  
