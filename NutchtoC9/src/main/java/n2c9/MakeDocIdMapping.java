package n2c9;

import java.io.*;
import java.util.*;

import n2c9.WholeFileInputFormat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.fs.*;


public class MakeDocIdMapping {
	
	static class MakeDocIdMappingMapper extends Mapper<NullWritable, Text, NullWritable, Text> {
		public void map(NullWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	static class MakeDocIdMappingReducer extends Reducer<NullWritable, Text, IntWritable, Text> {
		private HashMap<String, Integer> pageTable = new HashMap<String, Integer>();
		private HashMap<Integer, List<Integer>> linkGraph = new HashMap<Integer, List<Integer>>();
		private int nextId = 1;
		private MultipleOutputs<IntWritable, Text> mos;
		
		public void setup(Context context) {
			mos = new MultipleOutputs<IntWritable, Text>(context);
		}
			
		public void reduce (NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			for (Text val : values) {
				String value = val.toString();
				BufferedReader in = new BufferedReader(new StringReader(value));
				AddToGraph(in);
				WriteGraph();
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
		
		private Integer getId(String url) 
				throws IOException, InterruptedException {
			Integer id;
			if ((id = pageTable.get(url)) != null) return id;
			else {
				id = new Integer(nextId);
				pageTable.put(url, id);
				mos.write("map",  new IntWritable(id.intValue()), new Text(url));
				nextId++;
				return id;
			}
		}
		
		private void AddToGraph(BufferedReader in) 
				throws IOException, InterruptedException {
			for (String currLine = in.readLine(); currLine != null; currLine = in.readLine()) {
				Scanner s = new Scanner(currLine);
				Integer outUrl = getId(s.next());
				s.close();
				
				currLine = in.readLine();
				while (currLine.length() != 0) {
					s = new Scanner(currLine);
					s.next(); 						//skip fromUrl:
					Integer inUrl = getId(s.next());
					s.close();
					
					List<Integer> list = linkGraph.get(inUrl);
					if (list == null) list = new Vector<Integer>();
					list.add(outUrl);
					linkGraph.put(inUrl, list);	
					
					currLine = in.readLine();
				}
			}
		}
		
		private void WriteGraph() throws InterruptedException, IOException {
			for (int i=1; i < nextId; i++) {
				Integer inNode = new Integer(i);
				List<Integer> value = linkGraph.get(inNode);
				String outLine = inNode.toString();
				if (value != null) 
					for (Integer outNode : value) outLine += "\t" + outNode.toString();
				mos.write("graph", NullWritable.get(), new Text(outLine));
			}
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
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		MultipleOutputs.addNamedOutput(job, "map", TextOutputFormat.class, IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "graph", TextOutputFormat.class, NullWritable.class, Text.class);
			
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}