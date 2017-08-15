package TopTenEmployer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenEmployerMapper extends
		Mapper<Object, Text, NullWritable, Text> {
	private TreeMap<Integer, String> employerMap = new TreeMap<Integer, String>();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split("\t");

		String number = line[1];

		employerMap.put(Integer.parseInt(number), value.toString());

		if (employerMap.size() > 10) {
			employerMap.remove(employerMap.firstKey());
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (String s : employerMap.values()) {
			context.write(NullWritable.get(), new Text(s));
		}
	}
}
