package TopPosition;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopPositionReducer extends
		Reducer<NullWritable, Text, NullWritable, Text> {

	private TreeMap<Integer, String> titleMap = new TreeMap<Integer, String>();

	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			String[] line = value.toString().split("\t");

			String number = line[1];

			titleMap.put(Integer.parseInt(number) , value.toString());
			if (titleMap.size() > 10) {
				titleMap.remove(titleMap.firstKey());
			}
		}

		for (String s : titleMap.values()) {
			context.write(NullWritable.get(), new Text(s));

		}
	}
}