package LcaData;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LcaDataReducer extends
		Reducer<Text, LcaEmployerDigest, Text, LcaEmployerDigest> {

	public void reduce(Text key, Iterable<LcaEmployerDigest> values,
			Context context) throws IOException, InterruptedException {
		int from = -1;
		int to = -1;
		int totalSalary = 0;
		int totalNum = 0;
		String state = new String();
		String title = new String();
		for (LcaEmployerDigest val : values) {
			state = val.getState();
			title = val.getTitle();
			totalNum += val.getCount();
			totalSalary += ((val.getSalaryFrom() + val.getSalaryTo()) / 2 * val
					.getCount());
			if (from == -1 || val.getSalaryFrom() < from) {
				from = val.getSalaryFrom();
			}
			if (to == -1 || val.getSalaryTo() > to) {
				to = val.getSalaryTo();
			}
		}

		int average = totalSalary / totalNum;
		LcaEmployerDigest result = new LcaEmployerDigest();
		result.setCompanyName(key.toString());
		result.setTitle(title);
		result.setState(state);
		result.setSalaryFrom(from);
		result.setSalaryTo(to);
		result.setSalaryAverage(average);
		result.setCount(totalNum);

		context.write(key, result);

	}
}
