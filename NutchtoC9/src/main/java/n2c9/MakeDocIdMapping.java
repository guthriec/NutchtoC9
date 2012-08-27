package n2c9;

import java.io.*;
import java.util.*;

import n2c9.WholeFileInputFormat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.*;


public class MakeDocIdMapping {
  
  static class MakeDocIdMappingMapper extends Mapper<NullWritable, Text, Text, IntWritable> {
    
    static void writeOneUrl(String url, IntWritable val, Context context) 
        throws IOException, InterruptedException {
      context.write(new Text(url), val);
    }
    
    static void writeAllUrls(BufferedReader in, Context context) 
        throws IOException, InterruptedException {
      IntWritable val = new IntWritable(1);
      for (String currLine = in.readLine(); currLine != null; currLine = in.readLine()) {
        Scanner s = new Scanner(currLine);
        writeOneUrl(s.next(), val, context);
        s.close();
        
        currLine = in.readLine();
        for (currLine = in.readLine(); currLine.length() != 0; currLine = in.readLine()) {
          s = new Scanner(currLine);
          s.next();             //skip fromUrl:
          writeOneUrl(s.next(), val, context);
          s.close();     
        }
      }
    }
    
    public void map(NullWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String val = value.toString();
      BufferedReader in = new BufferedReader(new StringReader(val));
      writeAllUrls(in, context);
    }
  }
	
  static class MakeDocIdMappingReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
    IntWritable cnt = new IntWritable(1);
			
    public void reduce (Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      context.write(cnt, key);
      cnt.set(cnt.get() + 1);
		}
  }
	
  public static void main(String[] args) throws Exception {
    Job job = new Job();
    job.setJarByClass(MakeDocIdMapping.class);

    WholeFileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setInputFormatClass(WholeFileInputFormat.class);
    job.setNumReduceTasks(1);
	
    job.setMapperClass(MakeDocIdMappingMapper.class);
    job.setReducerClass(MakeDocIdMappingReducer.class);
		
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
		
	
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}