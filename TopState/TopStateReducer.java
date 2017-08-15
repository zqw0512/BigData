package TopState;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopStateReducer extends
		Reducer<NullWritable, Text, NullWritable, Text> {

	private TreeMap<Integer, String> stateMap = new TreeMap<Integer, String>();

	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			String[] line = value.toString().split("\t");

			String number = line[1];
			stateMap.put(Integer.parseInt(number), value.toString());
			if (stateMap.size() > 10) {
				stateMap.remove(stateMap.firstKey());
			}
		}

		for (String s : stateMap.values()) {
			context.write(NullWritable.get(), new Text(s));
		}
	}
}
