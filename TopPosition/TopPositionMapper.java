package TopPosition;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TopPositionMapper  extends Mapper<Object, Text, NullWritable, Text> {
	private TreeMap<Integer, String> titleMap = new TreeMap<Integer, String>();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split("\t");

		String number = line[1];
		

		titleMap.put(Integer.parseInt(number), value.toString());

		if (titleMap.size() > 10) {
			titleMap.remove(titleMap.firstKey());
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (String s : titleMap.values()) {
			context.write(NullWritable.get(), new Text(s));
		}
	}
}