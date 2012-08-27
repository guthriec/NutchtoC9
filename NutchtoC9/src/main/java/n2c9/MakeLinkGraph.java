package n2c9;

import java.io.*;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;
import java.util.HashMap;
import java.util.HashSet;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.*;

public class MakeLinkGraph {
  
  static class MakeLinkGraphMapper extends Mapper<NullWritable, Text, IntWritable, IntWritable> {
    HashMap<String, Integer> pageTable = new HashMap<String, Integer>();
    
    protected void setup(Context context) throws IOException, FileNotFoundException {
      Path mapping = DistributedCache.getLocalCacheFiles(context.getConfiguration())[0];
      BufferedReader in = new BufferedReader(new FileReader(new File(mapping.toString())));
      loadTable(in);
    }
    
    private void loadTable(BufferedReader in) throws IOException {
      for (String currLine = in.readLine(); currLine != null; currLine = in.readLine()) {
        Scanner s = new Scanner(currLine);
        Integer id = Integer.parseInt(s.next());
        String url = s.next();
        pageTable.put(url, id);
        s.close();
      }
    }
    
    private IntWritable getId(String url) 
        throws IOException, InterruptedException {
      Integer id;
      if ((id = pageTable.get(url)) != null) return new IntWritable(id);
      else {
        throw new RuntimeException("Could not find url in docidmapping");
      }
    }
    
    private void AddToGraph(BufferedReader in, Context context) 
        throws IOException, InterruptedException {
      for (String currLine = in.readLine(); currLine != null; currLine = in.readLine()) {
        Scanner s = new Scanner(currLine);
        IntWritable outUrl = getId(s.next());
        s.close();
        
        currLine = in.readLine();
        context.write(outUrl, new IntWritable(0));
        for (currLine = in.readLine(); currLine.length() != 0; currLine = in.readLine()) {
          s = new Scanner(currLine);
          s.next();             //skip fromUrl:
          IntWritable inUrl = getId(s.next());
          s.close();
          
          if (!inUrl.equals(outUrl)) context.write(inUrl, outUrl);
        }
      }
    }
    
    public void map(NullWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String val = value.toString();
      BufferedReader in = new BufferedReader(new StringReader(val));
      AddToGraph(in, context);
    }
  }
  
  static class MakeLinkGraphReducer extends Reducer<IntWritable, IntWritable, NullWritable, Text> {
    
    public void reduce (IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      HashSet<IntWritable> inNodes = new HashSet<IntWritable>();
      String outList = Integer.toString(key.get());
      for (IntWritable val : values) {
        if (val.get() == 0) continue;
        if (inNodes.add(val)) outList += "\t" + Integer.toString(val.get());
      }
      context.write(NullWritable.get(), new Text(outList));
    }
  }
  
  public static void main(String[] args) throws Exception {
    
    Configuration conf = new Configuration();
    DistributedCache.addCacheFile(new URI(args[2]), conf);
    Job job = new Job(conf);
    
    job.setJarByClass(MakeDocIdMapping.class);

    WholeFileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
    job.setInputFormatClass(WholeFileInputFormat.class);
    job.setNumReduceTasks(1);
  
    job.setMapperClass(MakeLinkGraphMapper.class);
    job.setReducerClass(MakeLinkGraphReducer.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
  
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}