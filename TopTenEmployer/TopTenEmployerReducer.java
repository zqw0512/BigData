package TopTenEmployer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTenEmployerReducer extends
		Reducer<NullWritable, Text, NullWritable, Text> {

	private TreeMap<Integer, String> TopEmployerMap = new TreeMap<Integer, String>();

	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			String[] line = value.toString().split("\t");
			String number = line[1];

			TopEmployerMap.put(Integer.parseInt(number), value.toString());
			if (TopEmployerMap.size() > 10) {
				TopEmployerMap.remove(TopEmployerMap.firstKey());
			}
		}
		for (String s : TopEmployerMap.values()) {
			context.write(NullWritable.get(), new Text(s));
		}

	}
}
