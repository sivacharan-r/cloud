import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class adjList {

  public static class adjMapper
       extends Mapper<Object, Text, Text, Text>{

      //private final static IntWritable one = new IntWritable(1);
    private Text outVal = new Text();
      private Text outKey = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
	String inline = value.toString();
	if (!inline.startsWith("#"))
	{
	    String [] inVals = inline.split("\t");
	    outKey.set(inVals[0]);
	    outVal.set(inVals[1]);
	    context.write(outKey, outVal);
	}
    }
  }

  public static class adjReducer
      extends Reducer<Text,Text,Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int cntr = 0;
      String adjlst = "";
      for (Text val : values)
      {
	  adjlst = adjlst+","+val;
	  cntr++;
      }
      adjlst = adjlst.substring(1);
      adjlst = adjlst+"#"+cntr;
      result.set(adjlst);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Cloud Computing Adj List");
    job.setJarByClass(adjList.class);
    job.setMapperClass(adjMapper.class);
    job.setCombinerClass(adjReducer.class);
    job.setReducerClass(adjReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
