package stateCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class stateCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private Text state = new Text();
	private IntWritable number = new IntWritable();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().split(";;");

		state.set(line[3].trim());

		number.set(Integer.valueOf(line[4].trim()));

		context.write(state, number);
	}
}
