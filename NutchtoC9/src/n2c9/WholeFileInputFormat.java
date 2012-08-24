package n2c9;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeFileInputFormat extends FileInputFormat<NullWritable, Text> {
	
	@Override
	public boolean isSplitable(JobContext context, Path file) {
		return false;
	}
	
	@Override
	public RecordReader<NullWritable, Text> createRecordReader (
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new WholeFileRecordReader();
	}
}
