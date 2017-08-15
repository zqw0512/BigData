package LcaData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LcaDataDriver {
	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration(), "lca processor");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LcaEmployerDigest.class);

		job.setJarByClass(LcaDataDriver.class);
		job.setMapperClass(LcaEmployerMapper.class);
		job.setReducerClass(LcaDataReducer.class);
		job.setCombinerClass(LcaDataReducer.class);

		job.setNumReduceTasks(10);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}