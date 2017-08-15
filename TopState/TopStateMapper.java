package TopState;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TopStateMapper extends Mapper<Object, Text, NullWritable, Text> {
	private TreeMap<Integer, String> stateMap = new TreeMap<Integer, String>();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split("\t");

		String number = line[1];

		stateMap.put(Integer.parseInt(number), value.toString());

		if (stateMap.size() > 10) {
			stateMap.remove(stateMap.firstKey());
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (String s : stateMap.values()) {
			context.write(NullWritable.get(), new Text(s));
		}
	}
}