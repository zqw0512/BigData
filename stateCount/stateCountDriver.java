package stateCount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class stateCountDriver {

	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration(), "state count");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setJarByClass(stateCountDriver.class);
		job.setMapperClass(stateCountMapper.class);
		job.setReducerClass(stateCountReducer.class);
		// job.setCombinerClass(stateCountReducer.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}