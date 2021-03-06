package TopTenEmployer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopTenEmployerDriver {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration(), "Top Ten");

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(TopTenEmployerDriver.class);
		job.setMapperClass(TopTenEmployerMapper.class);
		job.setReducerClass(TopTenEmployerReducer.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
